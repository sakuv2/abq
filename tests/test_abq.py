from abq import __version__
from abq import BQ
from syncer import sync
from pathlib import Path


def test_version():
    assert __version__ == "0.1.5"


def test_credential_file(credential_file_path):
    path = Path(credential_file_path)
    assert path.exists()


@sync
async def test_abq(project_name: str):

    bq = BQ(project_name)
    job = await bq.query("SELECT 1 AS a")
    result = await job.result()
    assert result[0]["a"] == 1

    # nullを含むクエリ
    job = await bq.query("SELECT NULL AS x")
    result = await job.result()
    assert result[0]["x"] == None

    job = await bq.query("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 200000)) AS x")
    result = await job.result()
    assert len(result) == 200000

    from google.cloud import bigquery

    parameters = [bigquery.ScalarQueryParameter("num", "INT64", 42)]
    job = await bq.query("SELECT @num AS num", parameters=parameters)
    result = await job.result()
    assert result[0]["num"] == 42

    byte = await bq.dry_query("SELECT 1")
    assert byte == 0

