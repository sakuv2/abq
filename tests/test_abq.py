from abq import __version__
from abq import BQ
from syncer import sync


def test_version():
    assert __version__ == "0.1.0"


@sync
async def test_abq():
    bq = BQ("agarichan")
    job = await bq.query("SELECT 1 AS a")
    result = await job.result()
    assert result[0]["a"] == 1

    job = await bq.query("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 200000)) AS x")
    result = await job.result()
    assert len(result) == 200000
