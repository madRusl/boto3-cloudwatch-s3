import boto3
import argparse
import datetime as dt
from botocore.exceptions import ClientError


#create 3x4 buckets, tag them like project (3-4 buckets per project, 3 projects overall) add several files to each bucket
#add options(+): -r region | -p period or period input
#add output(+): storage_type | year | month | bucket_name | bucket_size
#add underline output: sum of buckets per project for each month in a period
##and maybe: output report to csv
tag = []
sm, sy = [int(x) for x in input("Enter start date (MM-YYYY) (month including):\n").split('-')]
em, ey = [int(x) for x in input("Enter end date (MM-YYYY) (month including):\n").split('-')]

storage_type = [
    'StandardStorage',
    # 'StandardIAStorage',
    # 'ReducedRedundancyStorage',
    # 'GlacierStorage',
    # 'GlacierS3ObjectOverhead',
    # 'GlacierObjectOverhead'
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

<<<<<<< HEAD
if args.year:
    years = args.year
else:
    years.append(dt.datetime.now().year)

if args.month:
    months = args.month
else:
    months = [1,2,3,4,5,6,7,8,9,10,11]

=======
>>>>>>> 309fe82f71d4dd7c34b90970a55061acf8cdf7e3
if args.region:
    regions = args.region
else:
    try:
        client_ec2 = boto3.client('ec2')
        regions = [region['RegionName'] for region in client_ec2.describe_regions()['Regions']]
    except:
        print('client_ec2 connection failed')
        regions = None

try:
    client_s3 = boto3.client("s3")
    buckets = client_s3.list_buckets()["Buckets"]
except:
    print('client_s3 connection failed')
    buckets = None



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


def get_metric(bucket_name, storage_type, month, next_month, year, next_year):
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
        EndTime = dt.datetime(next_year, next_month, 1, 0, 1),
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
        for year in years:
            print(year)
            for month in months:
                print(month)
                for bucket in buckets:
                    if client_s3.get_bucket_location(Bucket = bucket['Name'])['LocationConstraint'] == reg:
                        res = get_metric(bucket, st, month, year)
                        if len(res['Datapoints']) > 0:
                            print(str(year) + '-' + str(month), st, bucket['Name'], res['Datapoints'][0]['Maximum'])


#create 3x4 buckets, tag them like project (3-4 buckets per project, 3 projects overall) add several files to each bucket
#add options: -r region | -p period
#add output: storage_type | year | month | bucket_name | bucket_size
#add underline output: sum by month for all buckets per project(?)
##and maybe: output report to csv
