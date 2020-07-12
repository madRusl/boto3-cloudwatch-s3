import boto3
import pandas as pd
import numpy as np
import json
import argparse
import os
from datetime import datetime
from botocore.exceptions import ClientError
from pandas.io.json import json_normalize


def write_to_json(data, filename):
    with open(f"{filename}.json", "w") as f:
        json.dump(
            data, f, separators=(",", ": "), indent=4, default=str
        )

def mkdate(datestring):
    return datetime.strptime(datestring, "%Y-%m")


def period_iterator(start_month, start_year, end_month, end_year):
    month, year = start_month, start_year
    next_month, next_year = month + 1, year
    while True:
        yield month, year, next_month, next_year
        if (next_month, next_year) == (end_month, end_year):
            return
        month = next_month
        year = next_year
        next_month += 1
        if next_month > 12:
            next_month = 1
            next_year += 1


def get_metric(bucket, storage, month, next_month, year, next_year):
    response = client_cloudwatch.get_metric_statistics(
        Namespace="AWS/S3",
        MetricName="BucketSizeBytes",
        Dimensions=[
            {"Name": "BucketName", "Value": bucket},
            {"Name": "StorageType", "Value": storage},
        ],
        StartTime=datetime(year, month, 1, 0, 0),
        EndTime=datetime(next_year, next_month, 1, 0, 1),
        Period=86400,
        Statistics=["Maximum"],
        Unit="Bytes",
    )
    return response

def main():
    parser = argparse.ArgumentParser(
        usage="python cloudwatch_s3_metrics.py [-h] [-r REGION] -[t PROJECT_TAG_KEY]",
        description="Get s3 metric statistics by region",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "-r",
        "--region",
        action="store",
        dest="region",
        nargs="*",
        help="Specify one or multiple regions (default: all): \
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
            \nus-gov-west-1",
        type=str,
    )

    parser.add_argument(
        "-s",
        "--start",
        action="store",
        dest="start_date",
        help="start date format YYYY-MM (inclusive)",
        type=mkdate,
    )

    parser.add_argument(
        "-e",
        "--end",
        action="store",
        dest="end_date",
        help="end date format YYYY-MM (inclusive)",
        type=mkdate,
    )

    parser.add_argument(
        "-t",
        "--tag-key",
        action="store",
        dest="project_key",
        help="resource project key tag",
        type=str,
    )

    args = parser.parse_args()

    if args.start_date and args.end_date:
        sy, sm = args.start_date.year, args.start_date.month
        ey, em = args.end_date.year, args.end_date.month
    else:
        print('No period selection with "-s" and "-e" arguments.')
        sy, sm = [
            int(x)
            for x in input("Enter start date in format YYYY-MM (inclusive):\n").split("-")
        ]
        ey, em = [
            int(x)
            for x in input("Enter end date in format YYYY-MM (inclusive):\n").split("-")
        ]

    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    list_of_bk_dict = list()
    list_of_bk_dict_untagged = list()
    output_rows = list()
    storages = [
        "StandardStorage",
        "StandardIAStorage",
        "ReducedRedundancyStorage",
        "GlacierStorage",
        "GlacierS3ObjectOverhead",
        "GlacierObjectOverhead",
    ]

    s3_resource = boto3.resource("s3")
    s3_client = boto3.client("s3")
    ec2_client = boto3.client("ec2")

    try:
        buckets = [bucket.name for bucket in s3_resource.buckets.all()]
        if args.region:
            regions = args.region
        else:
            regions = [region["RegionName"] for region in ec2_client.describe_regions()["Regions"]]
        print(f"\nregions:\n{regions} \n\nbuckets:\n{buckets}\n")
    except Exception as e:
        raise (e)
    
    write_to_json(buckets, "all_buckets")
    write_to_json(regions, "all_regions")

    for bucket in buckets:
        region_response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)[
            "ResponseMetadata"]["HTTPHeaders"]["x-amz-bucket-region"]
        try:
            bucket_tags = s3_client.get_bucket_tagging(Bucket=bucket)
            for i in bucket_tags["TagSet"]:
                if i["Key"] == project_key:
                    bucket_metadata = {
                        "bucket_name": bucket,
                        "region": region_response,
                        "project_tag": i["Value"],
                    }
                    list_of_bk_dict.append(bucket_metadata)
        except ClientError:
            bucket_metadata_untagged = {"bucket_name": bucket, "region": region_response}
            list_of_bk_dict_untagged.append(bucket_metadata_untagged)
            pass

    write_to_json(list_of_bk_dict_untagged, "untagged_buckets")

    for region in regions:
        client_cloudwatch = boto3.client("cloudwatch", region_name=region)
        for bk_dict in list_of_bk_dict:
            if bk_dict.get("region") == region:
                for st in storages:
                    for p in period_iterator(sm, sy, em, ey):
                        month, year, next_month, next_year = p
                        metric_response = get_metric(
                            bucket=bk_dict.get("bucket_name"),
                            storage=st,
                            month=month,
                            next_month=next_month,
                            year=year,
                            next_year=next_year,
                        )
                        if metric_response["Datapoints"]:
                            bk_metric_data = {
                                "storage_class": st,
                                "date": datetime(year, month, 1).strftime("%Y-%m"),
                                "size": metric_response["Datapoints"][0]["Maximum"],
                            }
                            bk_dict.setdefault("metrics", []).append(bk_metric_data)

    list_of_bk_dict[:] = [bk for bk in list_of_bk_dict if "metrics" in bk.keys()]

    write_to_json(list_of_bk_dict, "s3_metrics")

    result_output = json_normalize(
        list_of_bk_dict,
        record_path=["metrics"],
        meta=["bucket_name", "region", "project_tag"],
    )
    result_output.to_csv("result.csv", index=False, sep=",")
    print(f"\n{result_output}")

    for bk_dict in list_of_bk_dict:
        bk_row = bk_dict["metrics"]
        bk_project_tag = bk_dict["project_tag"]
        bk_region = bk_dict["region"]
        for row in bk_row:
            row["project_tag"] = bk_project_tag
            row["region"] = bk_region
            output_rows.append(row)

    df = pd.DataFrame(output_rows)
    df = df.pivot_table(
        index=["project_tag", "region", "storage_class"],
        columns=["date"],
        values=["size"],
        fill_value=0,
        aggfunc=np.sum,
    ).reset_index()
    df.to_csv("result_sum.csv", index=False, sep=",")


if __name__ == "__main__":
    main()