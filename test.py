import datetime as dt
from dateutil.rrule import rrule, MONTHLY

storage_type = ['StandardStorage']
buckets = ['bucket_example_1','bucket_example_2']
regions = ['eu-central-1']

sm, sy = [int(x) for x in input("Enter start date (MM-YYYY) (month including):\n").split('-')]
# start_date = dt.date(sy, sm, int(1))
em, ey = [int(x) for x in input("Enter end date (MM-YYYY) (month including):\n").split('-')]
# end_date = dt.date(ey, em+1, int(1))

def get_metric(bucket_name, storage_type, month, next_month, year):
    start_date = dt.datetime(year, month, 1, 0, 0)
    end_date = dt.datetime(year, next_month, 1, 0, 1)
    response = [bucket_name, storage_type, start_date, end_date, year]
    return response

def months(start_month, start_year, end_month, end_year):
    month, year = start_month, start_year
    while True:
        yield month, year
        if (month, year) == (end_month, end_year):
            return
        month += 1
        if (month > 12):
            month = 1
            year += 1

for reg in regions:
    # print(f'\nregion: {reg}')
    for st in storage_type:
        for i in months(start_month = sm, start_year=sy, end_month=em, end_year=ey):
            month, year = i
            next_month = month + 1
            if next_month == 13:
                month, next_month, year = 12, 1, year + 1
            else: pass
            for bucket in buckets:
                res = get_metric(bucket, st, month, next_month, year)
                if res:
                    print('region:', reg, 'bucket_name:', res[0], 'storage_type:', res[1], 'start_date:', res[2], 'end_date:', res[3] )
                    # print(str(year) + '-' + str(month), st, bucket)


