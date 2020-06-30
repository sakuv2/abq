## asyncなbigquery client

### install

```bash
pip install git+https://github.com/sakuv2/abq.git
# or
poetry add git+https://github.com/sakuv2/abq.git
```

### credential
環境変数`GOOGLE_APPLICATION_CREDENTIALS`にbigqueryのアクセス権をもったGCPクレデンシャルをセットする。

### sample

```python
import asyncio
from abq import BQ

async def main():
    bq = BQ(project_id="agarichan")
    job = await bq.query("SELECT 1 AS a")
    result = await job.result()
    assert result[0]["a"] == 1

    job = await bq.query("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 200000)) AS x")
    result = await job.result()
    assert len(result) == 200000

if __name__ == '__main__':
    asyncio.run(main())
```
