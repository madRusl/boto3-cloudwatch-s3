import boto3
import json
import argparse
import os
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError
from pandas.io.json import json_normalize


os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

list_of_dict = []
storages = [
    'StandardStorage',
    'StandardIAStorage',
    'ReducedRedundancyStorage',
    'GlacierStorage',
    'GlacierS3ObjectOverhead',
    'GlacierObjectOverhead'
    ]

sm, sy = [int(x) for x in input('Enter start date (MM-YYYY) (month including):\n').split('-')]
em, ey = [int(x) for x in input('Enter end date (MM-YYYY) (month including):\n').split('-')]

parser = argparse.ArgumentParser(
    usage='python test.py [-h] [-r REGION]',
    description='Get s3 metric statistics by region',
    formatter_class=argparse.RawTextHelpFormatter
    )

parser.add_argument(
    '-r',
    '--region',
    action = 'store',
    dest = 'region',
    nargs = '*',
    help = 'Specify one or multiple regions (default: all): \
        \nus-east-1, us-east-2 \
        \nus-west-1, us-west-2 \
        \nap-east-1 \
        \nap-south-1 \
        \nap-northeast-2 \
        \nap-southeast-1, ap-southeast-2 \
        \nap-northeast-1 \
        \nca-central-1 \
        \ncn-north-1 \
        \ncn-northwest-1 \
        \neu-central-1 \
        \neu-west-1, eu-west-2, eu-west-3 \
        \neu-north-1 \
        \nme-south-1 \
        \nsa-east-1 \
        \nus-gov-east-1 \
        \nus-gov-west-1',
    type=str,
)

args = parser.parse_args()

if args.region:
    regions = args.region
else:
    try:
        client_ec2 = boto3.client('ec2')
        regions = [region['RegionName'] for region in client_ec2.describe_regions()['Regions']]
    except Exception as e:
        print(e)


def period_iterator(start_month, start_year, end_month, end_year):
    month, year = start_month, start_year
    while True:
        yield month, year
        if (month, year) == (end_month, end_year):
            return
        month += 1
        if (month > 12):
            month = 1
            year += 1


def get_metric(bucket, storage, month, next_month, year, next_year):
    response = client_cloudwatch.get_metric_statistics(
        Namespace = 'AWS/S3',
        MetricName = 'BucketSizeBytes',
        Dimensions = [
        {
            'Name': 'BucketName',
            'Value': bucket
        },
        {
            'Name': 'StorageType',
            'Value': storage
        },
        ],
        StartTime = datetime(year, month, 1, 0, 0),
        EndTime = datetime(next_year, next_month, 1, 0, 1),
        Period = 86400,
        Statistics = [
        'Maximum',
        ],
        Unit = 'Bytes'
    )
    return response


s3_resource = boto3.resource('s3')
buckets = [bucket.name for bucket in s3_resource.buckets.all()]
print(f"\nregions:\n {regions} \n\nbuckets: {buckets}\n")

s3_client = boto3.client('s3')

for bucket in buckets:
    region_response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
    bucket_tags = None
    try:
        bucket_tags = s3_client.get_bucket_tagging(Bucket=bucket)
        for i in bucket_tags['TagSet']:
            if i['Key'] == 'coherent:project':
                bucket_metadata = {'bucket_name':bucket, 'region':region_response, 'project_tag':i['Value']}
                list_of_dict.append(bucket_metadata)
    except ClientError:
        f=open('untagged_buckets.txt', 'a+')
        f.write(bucket + '\n')
        f.close()
        pass

for region in regions:
    client_cloudwatch = boto3.client('cloudwatch', region_name=region)
    for bk_dict in list_of_dict:
        if bk_dict.get('region') == region:
            for st in storages:
                for p in period_iterator(sm, sy, em, ey):
                    bk_metric_period = {}
                    month, year = p
                    next_month, next_year = month, year
                    next_month += 1
                    if next_month == 13:
                        next_month, next_year = 1, year + 1
                    metric_response = get_metric(bucket=bk_dict.get('bucket_name'), storage=st, month=month, next_month=next_month, year=year, next_year=next_year)
                    if metric_response['Datapoints']:
                        bk_metric_period = {'storage_class':st, 'date':datetime(year, month, 1).strftime('%Y-%m'), 'size':metric_response['Datapoints'][0]['Maximum']}
                    else:
                        bk_metric_period = {'storage_class':st, 'date':datetime(year, month, 1).strftime('%Y-%m'), 'size':'0'}
                    bk_dict.setdefault('metrics', []).append(bk_metric_period)

with open('result.json', 'w') as fp:
    json.dump(list_of_dict, fp, separators=(',', ': '), indent=4, default=str)

result_output = json_normalize(list_of_dict, record_path=['metrics'], meta=['bucket_name', 'region', 'project_tag'])
print(f"{result_output}")
result_output.to_csv('result.csv', index=False, sep=',')
