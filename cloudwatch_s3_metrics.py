import boto3
import argparse
import datetime as dt

month = [1,2,3,4,5,6,7,8,9,10,11]
year = dt.datetime.now().year

storage_type = [
    'StandardStorage',
    'StandardIAStorage',
    'ReducedRedundancyStorage',
    'GlacierStorage',
    'GlacierS3ObjectOverhead',
    'GlacierObjectOverhead'
    ]

parser = argparse.ArgumentParser(
    usage='python cloudwatch_s3_metric.py [-h] [-r REGION]',
    description='Get s3 metric statistics by region',
    formatter_class=argparse.RawTextHelpFormatter
    )

parser.add_argument(
    "-r",
    "--region",
    action = 'store',
    dest = 'region',
    nargs = '*',
    help = 'You can specify one or multiple regions (default: all): \
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
    except:
        print('client_ec2 connection failed')

try:
    client_s3 = boto3.client("s3")
    buckets = client_s3.list_buckets()["Buckets"]
except:
    print('client_s3 connection failed')

def get_metric(bucket_name, storage_type, month, year):
    response = client_cloudwatch.get_metric_statistics(
        Namespace = 'AWS/S3',
        MetricName = 'BucketSizeBytes',
        Dimensions = [
        {
            'Name': 'BucketName',
            'Value': bucket["Name"]
        },
        {
            'Name': 'StorageType',
            'Value': storage_type
        },
        ],
        StartTime = dt.datetime(year, month, 1, 0, 0),
        EndTime = dt.datetime(year, month + 1, 1, 0, 1),
        Period = 86400,
        Statistics = [
        'Maximum',
        ],
        Unit = 'Bytes'
    )
    return response

for reg in regions:
    client_cloudwatch = boto3.client("cloudwatch", region_name=reg)
    print(f'\nregion: {reg}')
    for st in storage_type:
        for m in month:
            for bucket in buckets:
                if client_s3.get_bucket_location(Bucket = bucket['Name'])['LocationConstraint'] == reg:
                    res = get_metric(bucket, st, m, year)
                    if len(res['Datapoints']) > 0:
                        print(str(year) + '-' + str(m), st, bucket['Name'], res['Datapoints'][0]['Maximum'])
