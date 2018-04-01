import json
import pymysql.cursors
import sys
import boto3
from pathlib import Path

def get_bucket_size(bucket_name):
    bucket = s3.Buckt(bucket_name)
    return bucket.size

if Path("/run/secret/config.json").is_file():
    conf = json.load(open('/run/secret/config.json'))
else:
    conf = json.load(open('./config.json'))

connection = pymysql.connect(
    host = conf['host'],
    user = conf['username'],
    password = conf['password'],
    db = conf['db'],
    charset = 'utf8mb4',
)
s3 = boto3.client(
    's3',
    region_name = conf['AWS_REGION'],
    aws_access_key_id = conf['AWS_ACCESS_KEY'],
    aws_secret_access_key = conf['AWS_SECRET_KEY']
)

bucket_sizes = []

try:
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT name FROM buckets"
    cursor.execute(sql)
    for row in cursor:
        bucket_sizes[row['name']] = get_bucket_size(row['name'])

    sql = "UPDATE buckets SET size=%s, size_updated=NOW() WHERE name=%s"
    for k in bucket_sizes:
        print(k, bucket_sizes[k])
        cursor.execute(sql, (k, bucket_sizes[k]))

finally:
    connection.close()
