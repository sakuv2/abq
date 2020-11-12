from abq import __version__
from abq import BQ
from syncer import sync
from pathlib import Path


def test_version():
    assert __version__ == "0.1.0"


def test_credential_file(credential_file_path):
    path = Path(credential_file_path)
    assert path.exists()


@sync
async def test_abq(project_name: str):
    bq = BQ(project_name)
    job = await bq.query("SELECT 1 AS a")
    result = await job.result()
    assert result[0]["a"] == 1

    job = await bq.query("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 200000)) AS x")
    result = await job.result()
    assert len(result) == 200000
