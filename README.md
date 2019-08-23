### boto3-cloudwatch-s3
```python
python3 -m venv .env
. .env/bin/activate
pip install -r requirements.txt
python cloudwatch_s3_metrics.py
```
buckets should have tag key 'coherent:project', otherwise ignored
buckets with empty metrics ignored
stdout and json/csv files as output