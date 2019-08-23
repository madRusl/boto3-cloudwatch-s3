### boto3-cloudwatch-s3
```python
python3 -m venv .env
. .env/bin/activate
pip install -r requirements.txt
python cloudwatch_s3_metrics.py
```
ignored buckets:
```
buckets w/o tag key 'coherent:project';
buckets with empty metrics
```
stdout and json/csv files as output