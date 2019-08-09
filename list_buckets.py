#!/usr/local/bin/python3

import boto3
import argparse
import sys


regions = ["eu-central-1", "eu-west-1", "eu-north-1"]

dct = {}
list_dct = []
region_bucket = dict()

client_s3 = boto3.client("s3")
for reg in regions:
    print(f'\nregion: {reg}')
    dct['dict_%s' % reg] = []
    for bucket in client_s3.list_buckets()["Buckets"]:
        if client_s3.get_bucket_location(Bucket=bucket['Name'])['LocationConstraint'] == reg:
            print(f'\tbucket name: {bucket["Name"]}')
            dct['dict_%s' % reg].append(bucket["Name"])
    print(dct)

print('\n')
for r, b in dct.items():
    for i in b:
        print(i, ' is in region', r)
