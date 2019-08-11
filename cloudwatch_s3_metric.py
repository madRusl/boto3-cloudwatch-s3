#!/usr/local/bin/python3

import boto3
import argparse
import sys
import datetime as dt

parser = argparse.ArgumentParser(usage='python cloudwatch_s3_metric.py [-h] [-r REGION]', description='Get s3 metric statistics by region')

parser.add_argument(
    "-r",
    "--region",
    action='store',
    dest = 'region',
    nargs='*',
    help=
    'us-east-2, \
    us-east-1, \
    us-west-1, \
    us-west-2, \
    ap-east-1, \
    ap-south-1, \
    ap-northeast-2, \
    ap-southeast-1, \
    ap-southeast-2, \
    ap-northeast-1, \
    ca-central-1, \
    cn-north-1, \
    cn-northwest-1, \
    eu-central-1, \
    eu-west-1, \
    eu-west-2, \
    eu-west-3, \
    eu-north-1, \
    me-south-1, \
    sa-east-1, \
    us-gov-east-1, \
    us-gov-west-1',
    type=str,
)

args = parser.parse_args()

if not args.region:
    regions = []
    client = boto3.client('ec2')
    regions = [region['RegionName'] for region in client.describe_regions()['Regions']]
    # print("Please specify a region")
    # parser.print_help()
    # sys.exit(1)
else:
    regions = args.region
    
month = [1,2,3,4,5,6,7,8]
year = dt.datetime.now().year

storage_type = [ 
                'StandardStorage'
                # 'StandardIAStorage', 
                # 'ReducedRedundancyStorage', 
                # 'GlacierStorage', 
                # 'GlacierS3ObjectOverhead', 
                # 'GlacierObjectOverhead'
                ]

client_s3 = boto3.client("s3")



def get_metric(bucket_name, storage_type, month, year):
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/S3',
        MetricName='BucketSizeBytes',
        Dimensions=[
        {
            'Name': 'BucketName',
            'Value': bucket["Name"]
        },
        {
            'Name': 'StorageType',
            'Value': storage_type
        },
        ],
        StartTime=dt.datetime(year, month, 1, 0, 0),
        EndTime=dt.datetime(year, month + 1, 1, 0, 1),
        Period=86400,
        Statistics=[
        'Maximum',
        ],
        Unit='Bytes'
    )
    return response

for reg in regions:
    cloudwatch_client = boto3.client("cloudwatch", region_name=reg)
    print(f'\nregion: {reg}')
    for st in storage_type:
        for m in month:
            for bucket in client_s3.list_buckets()["Buckets"]:
                if client_s3.get_bucket_location(Bucket=bucket['Name'])['LocationConstraint'] == reg:
                    # print(f'\tbucket name: {bucket["Name"]}')
                    res = get_metric(bucket, st, m, year)
                    if len(res['Datapoints']) > 0:
                        print(str(year) + '-' + str(m), st, bucket['Name'], res['Datapoints'][0]['Maximum'])
    