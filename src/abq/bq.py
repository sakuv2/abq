import asyncio
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from logging import getLogger
from typing import AsyncIterator, Dict, List, Optional, Union

import httplib2
import httpx
from google.cloud import bigquery
from google.cloud.bigquery._helpers import _record_field_to_json
from google.cloud.bigquery.schema import _parse_schema_resource
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import BaseSettings

from .async_retry import retry
from .rows_parser import RowsParser

logger = getLogger(__name__)

RETRYIES = 3


class EndPoint:
    endpoint = "https://bigquery.googleapis.com/bigquery/v2/projects/{projectId}"
    query = "/queries"
    job = "/jobs/"
    table = "/datasets/{datasetId}/tables/{tableId}"
    insert_all = "/datasets/{datasetId}/tables/{tableId}/insertAll"

    def __getattribute__(self, attr):
        endpoint = super().__getattribute__("endpoint")
        return endpoint + super().__getattribute__(attr)


class Env(BaseSettings):
    google_application_credentials: str

    class Config:
        env_file = ".env"


class Credential:
    _credentials = None

    @classmethod
    def _update_credential(cls):
        if cls._credentials is None:
            scopes = [
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/bigquery.readonly",
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/cloud-platform.read-only",
            ]

            cls._credentials = ServiceAccountCredentials.from_json_keyfile_name(
                Env().google_application_credentials, scopes=scopes
            )
            cls._credentials.refresh(httplib2.Http())
        else:
            if cls._credentials.access_token_expired:
                cls._credentials.refresh(httplib2.Http())

    @classmethod
    def get_headers(cls, headers=None):
        if headers is None:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
        cls._update_credential()
        cls._credentials.apply(headers)
        return headers


class QueryException(Exception):
    ...


class Client:
    def __init__(self):
        limits = httpx.PoolLimits(max_keepalive=10, max_connections=100)
        self._client = httpx.AsyncClient(pool_limits=limits)

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self._client.aclose()

    @retry(RETRYIES, httpx.HTTPError)
    async def post(self, endpoint, json, timeout=5000, call_back=None):
        r = await self._client.post(
            url=endpoint, headers=Credential.get_headers(), json=json, timeout=timeout,
        )
        if call_back is not None:
            call_back(r)
        r.raise_for_status()
        return r

    @retry(RETRYIES, httpx.HTTPError)
    async def get(self, endpoint, params=None, timeout=5000, call_back=None):
        r = await self._client.get(
            url=endpoint,
            headers=Credential.get_headers(),
            params=params,
            timeout=timeout,
        )
        if call_back is not None:
            call_back(r)
        r.raise_for_status()
        return r


class BQ(Client):
    def __init__(self, project_id: str):
        self._project_id = project_id
        super().__init__()

    # https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    async def _base_query(
        self,
        sql: str,
        parameters: Optional[
            List[
                Union[
                    bigquery.ScalarQueryParameter,
                    bigquery.ArrayQueryParameter,
                    bigquery.StructQueryParameter,
                ]
            ]
        ] = None,
        projectId: Optional[str] = None,
        useLegacySql: bool = False,
        maxResults: int = 0,
        dryRun: bool = False,
        timeoutMs: int = 0,
        maximumBytesBilled: Optional[int] = None,
    ):
        if projectId is None:
            projectId = self._project_id
        endpoint = EndPoint().query.format(projectId=projectId)
        data = {
            "query": sql,
            "useLegacySql": useLegacySql,
            "maxResults": maxResults,
            "timeoutMs": timeoutMs,
            "dryRun": dryRun,
            "maximumBytesBilled": maximumBytesBilled,
        }
        if parameters is not None:
            data["parameterMode"] = "NAMED"
            data["queryParameters"] = [p.to_api_repr() for p in parameters]

        logger.debug(f"begin BQ query: {sql}")
        await self.query_wait()

        def error(r):
            if "error" in r.json().keys():
                msg = f'Query Error: {r.json()["error"]["message"]}'
                logger.error(msg)
                raise QueryException(msg)

        result = await self.post(
            endpoint=endpoint, json=data, timeout=timeoutMs + 5000, call_back=error
        )

        return result.json()

    async def dry_query(self, *args, **kwargs) -> int:
        """ドライランしてbytesを返却

        Returns:
            int: x bytes
        """
        result = await self._base_query(dryRun=True, *args, **kwargs)
        return int(result["totalBytesProcessed"])

    async def query(
        self,
        sql: str,
        parameters: Optional[
            List[
                Union[
                    bigquery.ScalarQueryParameter,
                    bigquery.ArrayQueryParameter,
                    bigquery.StructQueryParameter,
                ]
            ]
        ] = None,
        projectId=None,
        useLegacySql=False,
        maxResults=0,
        timeoutMs=0,
        maximumBytesBilled=None,
    ):
        result = await self._base_query(
            sql=sql,
            parameters=parameters,
            projectId=projectId,
            useLegacySql=useLegacySql,
            maxResults=maxResults,
            timeoutMs=timeoutMs,
            maximumBytesBilled=maximumBytesBilled,
        )
        return await JobResult.create_from_json(result)

    # クエリの同時実行数に制限をかける
    @retry(RETRYIES)
    async def query_wait(self):
        while True:
            json = await self.get_job_list(
                allUsers=True, projection="MINIMAL", stateFilter="PENDING"
            )
            pendings = json.get("jobs")
            f = pendings is None
            json = await self.get_job_list(
                allUsers=True, projection="MINIMAL", stateFilter="RUNNING"
            )
            runnings = json.get("jobs")
            if f and (runnings is None or len(runnings) <= 5):
                break
            await asyncio.sleep(1)

    # https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list
    @retry(RETRYIES)
    async def get_job_list(
        self,
        projectId=None,
        allUsers=False,
        maxResults=None,
        minCreationTime=None,
        maxCreationTime=None,
        stateFilter=None,
        projection=None,
        parentJobId=None,
    ):
        if projectId is None:
            projectId = self._project_id
        endpoint = EndPoint().job.format(projectId=projectId)
        params = {"allUsers": str(allUsers)}
        if maxResults is not None:
            params["maxResults"] = str(maxResults)
        if minCreationTime is not None:
            params["minCreationTime"] = str(minCreationTime)
        if maxCreationTime is not None:
            params["maxCreationTime"] = str(maxCreationTime)
        if stateFilter is not None:
            params["stateFilter"] = str(stateFilter)
        if projection is not None:
            params["projection"] = str(projection)
        if parentJobId is not None:
            params["parentJobId"] = str(parentJobId)
        r = await self.get(endpoint, params=params)
        return r.json()

    @retry(RETRYIES)
    async def get_job(self, job_id: str, projectId=None):
        if projectId is None:
            projectId = self._project_id
        endpoint = EndPoint().job.format(projectId=projectId) + job_id

        r = await self.get(endpoint)
        r.raise_for_status()
        return r.json()

    def parse_table_name(self, table: str) -> Dict[str, str]:
        s = table.split(".")
        epf = {}
        if len(s) == 3:
            epf["projectId"] = s[0]
            epf["datasetId"] = s[1]
            epf["tableId"] = s[2]
        elif len(s) == 2:
            epf["projectId"] = self._project_id
            epf["datasetId"] = s[0]
            epf["tableId"] = s[1]
        else:
            raise RuntimeError(
                f'tableは"projectId.datasetId.tableId"もしくは"datasetId.tableId"の形で設定してください: {table}'
            )
        return epf

    @retry(RETRYIES)
    async def get_table(self, table: str):
        ep = EndPoint().table.format(**self.parse_table_name(table))

        r = await self.get(ep)
        r.raise_for_status()
        return r.json()

    # https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
    async def insert_rows(
        self,
        table: str,
        rows,
        row_ids=None,
        skipInvalidRows=False,
        ignoreUnknownValues=False,
        templateSuffix=None,
    ):
        ep = EndPoint().insert_all.format(**self.parse_table_name(table))
        table_json = await self.get_table(table)
        schema = _parse_schema_resource(
            table_json["schema"]
        )  # 外部ライブラリーの低レベルメソッドなので危険かも
        rows_info = []
        json_rows = [_record_field_to_json(schema, row) for row in rows]

        for index, row in enumerate(json_rows):
            info = {"json": row}
            if row_ids is not None:
                info["insertId"] = row_ids[index]
            else:
                info["insertId"] = str(uuid.uuid4())
            rows_info.append(info)

        params = dict(
            rows=rows_info,
            skipInvalidRows=skipInvalidRows,
            ignoreUnknownValues=ignoreUnknownValues,
        )
        if templateSuffix is not None:
            params["templateSuffix"] = templateSuffix

        def error(r):
            errors = []
            for error in r.json().get("insertErrors", ()):
                errors.append({"index": int(error["index"]), "errors": error["errors"]})
            if len(errors) > 0:
                logger.error(errors)

        r = await self.post(endpoint=ep, json=params, call_back=error)

        return r.json()


class JobResult(Client):
    def __init__(self, projectId: str, jobId: str):
        self.project_id = projectId
        self.job_id = jobId
        self.state = None
        super().__init__()

    @classmethod
    async def create_from_json(cls, json):
        self = cls(
            projectId=json["jobReference"]["projectId"],
            jobId=json["jobReference"]["jobId"],
        )
        await self.update()
        return self

    async def result(self, startIndex=0, maxResults=None):
        result = []
        async for r in self.iter_result(startIndex, maxResults):
            result += r
        return result

    async def iter_result(self, startIndex=0, maxResults=None) -> AsyncIterator:
        await self.wait()
        json = await self.get_query_result(startIndex=startIndex, maxResults=maxResults)
        total_rows = int(json["totalRows"])
        if total_rows == 0:
            yield []
            return
        self.rows_parser = RowsParser(json["schema"])
        result = self.rows_parser.parse_rows(json["rows"])

        # 2ページ目以降の取得をproduce/consumerパターンで行う
        if "pageToken" in json.keys():
            queue: asyncio.Queue[dict] = asyncio.Queue()
            result_queue: asyncio.Queue[Future] = asyncio.Queue()
            asyncio.create_task(self._result_producer(json, queue))
            asyncio.create_task(self._result_consumer(queue, result_queue))
            yield result
            while True:
                f = await result_queue.get()
                if f is None:
                    break
                yield f.result()
        else:
            yield result

    async def _result_producer(self, json, queue: asyncio.Queue):
        while "pageToken" in json.keys():
            json = await self.get_query_result(pageToken=json["pageToken"])
            queue.put_nowait(json["rows"])
        queue.put_nowait(None)

    async def _result_consumer(self, queue: asyncio.Queue, result_queue: asyncio.Queue):
        with ThreadPoolExecutor(max_workers=2) as executor:
            while True:
                rows = await queue.get()
                if rows is None:
                    result_queue.put_nowait(None)
                    break
                f = executor.submit(self.rows_parser.parse_rows, rows)
                result_queue.put_nowait(f)

    # https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
    async def get_query_result(
        self, startIndex=0, maxResults=None, pageToken=None, timeoutMs=1000,
    ):
        if self.state is None:
            await self.update()
        endpoint = (
            EndPoint().query.format(projectId=self.project_id) + "/" + self.job_id
        )
        params = {
            "startIndex": str(startIndex),
            "timeoutMs": str(timeoutMs),
        }
        if maxResults is not None:
            params["maxResults"] = str(maxResults)
        if pageToken is not None:
            params["pageToken"] = pageToken

        r = await self.get(endpoint, params=params, timeout=timeoutMs + 5000)
        return r.json()

    async def update(self, close=True):
        endpoint = EndPoint().job.format(projectId=self.project_id) + self.job_id
        r = await self.get(endpoint)
        self.state = r.json()["status"]["state"]
        if close:
            await self.close()

    async def wait(self, state="DONE"):
        while True:
            await self.update(close=False)
            if self.state == state:
                break
            await asyncio.sleep(1)
            logger.debug(f"waiting for query state: {self}")
        await self.close()
        return True
