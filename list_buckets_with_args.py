#!/usr/local/bin/python3

import boto3
import argparse
import sys

parser = argparse.ArgumentParser(usage='python list_bucket.py [-h] [-r REGION]', description='Get bucket name by region')

regions = ["eu-central-1", "eu-west-1", "eu-north-1"]

parser.add_argument(
    "-r",
    "--region",
    # default='eu-central-1',
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
    us-gov-west-1'
    ,
    type=str,
)

args = parser.parse_args()

if not args.region:
    print("Please specify a region")
    parser.print_help()
    sys.exit(1)
else:
    reg = args.region
    print(f'\nregion: {reg}')

client_s3 = boto3.client("s3")

for bucket in client_s3.list_buckets()["Buckets"]:
    if client_s3.get_bucket_location(Bucket=bucket['Name'])['LocationConstraint'] == reg:
        print(f'\tbucket name: {bucket["Name"]}')
