#!/usr/local/bin/python3

import boto3
from datetime import datetime

regions = ["eu-central-1", "eu-west-1", "eu-north-1"]

# client_s3 = boto3.client("s3")
# for reg in regions:
#     print(f'\n{reg} region:')
#     for bucket in client_s3.list_buckets()["Buckets"]:
#         if client_s3.get_bucket_location(Bucket=bucket['Name'])['LocationConstraint'] == reg:
#             print(f'\tbucket name: {bucket["Name"]}')


cloudwatch_client = boto3.client("cloudwatch", region_name='us-west-2')

def get_metric(bucket_name, storage_type, month, year):
  response = client_cloudwatch.get_metric_statistics(
    Namespace='AWS/S3',
    MetricName='BucketSizeBytes',
    Dimensions=[
      {
        'Name': 'BucketName',
        'Value': bucket_name
      },
      {
        'Name': 'StorageType',
        'Value': storage_type
      },
    ],
    StartTime=datetime(year, month, 1, 0, 0),
    EndTime=datetime(year, month, 1, 0, 1),
    Period=86400,
    Statistics=[
      'Maximum',
    ],
    Unit='Bytes'
  )
  return response

month = [1,2,3,4,5,6,7,8,9,10,11,12]
year = 2019
storage_type =['StandardStorage','StandardIAStorage', 'ReducedRedundancyStorage', 'GlacierStorage', 'GlacierS3ObjectOverhead', 'GlacierObjectOverhead']

buckets = ['eu-central-1-some-test-bucket', 'eu-central-1-some-test-bucket']
# eu_north_1_buckets = ['eu-north-1-some-test-bucket']
# eu_west_1_buckets = ['eu-west-1-some-test-bucket']

for st in storage_type:
    for m in month:
        for bk in buckets:
            res = get_metric(bk, st, m, year)
            if len(res['Datapoints']) > 0:
                print(str(year) + '-' + str(m), st, bk, res['Datapoints'][0]['Maximum'])
