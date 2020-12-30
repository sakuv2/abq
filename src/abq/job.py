from pydantic import BaseModel

from typing import Optional


class DestinationTable(BaseModel):
    projectId: Optional[str]
    datasetId: Optional[str]
    tableId: Optional[str]


class Query(BaseModel):
    query: Optional[str]
    destinationTable: Optional[DestinationTable]
    useLegacySql: Optional[str]
    writeDisposition: Optional[str]
    priority: Optional[str]


class Configuration(BaseModel):
    query: Optional[Query]
    jobType: Optional[str]


class JobReference(BaseModel):
    jobId: Optional[str]
    location: Optional[str]
    projectId: Optional[str]


class StateQuery(BaseModel):
    cacheHit: Optional[bool]
    statementType: Optional[str]
    totalBytesBilled: Optional[int]
    totalBytesProcessed: Optional[int]


class Statistics(BaseModel):
    creationTime: Optional[int]
    startTime: Optional[int]
    endTime: Optional[int]
    totalBytesProcessed: Optional[int]
    query: Optional[StateQuery]


class Status(BaseModel):
    state: Optional[str]


class Job(BaseModel):
    id: Optional[str]
    etag: Optional[str]
    selfLink: Optional[str]
    kind: Optional[str]
    user_email: Optional[str]
    jobReference: Optional[JobReference]
    configuration: Optional[Configuration]
    statistics: Optional[Statistics]
    status: Optional[Status]
