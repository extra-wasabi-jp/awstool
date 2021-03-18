#!/usr/bin/python3
from pprint import pprint
import os
import sys
import yaml
import csv
import boto3
from botocore.exceptions import ClientError

ymlfile = "pydydump.yml"
if len(sys.argv) == 2:
    if not os.path.isfile(sys.argv[1]):
        print("引数に指定されたYamlファイル ({}) が見つかりません".format(sys.argv[1]))
        exit()
    ymlfile = sys.argv[1]
elif not os.path.isfile(ymlfile):
    print("Yamlファイル (pydydump.yml) が見つかりません")
    exit()
    

# プロパティ読み込み
with open(ymlfile) as yamlfile:
    prop = yaml.safe_load(yamlfile)

endpoint_url = prop["aws"]["endpoint-url"]

tableName = prop["pydydump"]["table-name"]
nullstr = prop["pydydump"]["nullstr"]
fields = prop["pydydump"]["fields"]

if endpoint_url != "":
    dynamodb = boto3.resource("dynamodb", endpoint_url=endpoint_url)
else:
    dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(tableName)

# ヘッダプリント
line = "#"
for field in fields:
    line += "\t{}".format(field)
print(line)

# 全件スキャン
try:
    done = False
    start_key = None
    scan_kwargs = {}
    rownum = 0
    while not done:
        if start_key:
            scan_kwargs["ExclusiveStartKey"] = start_key
        response = table.scan(**scan_kwargs)

        for rec in response["Items"]:
            rownum += 1
            line = "{}".format(rownum)
            for field in fields:
                try:
                    val = rec[field]
                except KeyError:
                    val = nullstr
                line += "\t{}".format(val)
            print(line)
        start_key = response.get("LastEvaluatedKey", None)
        done = start_key is None

except ClientError as e:
    print("*** ERROR ***")
    print("exception :{}".format(e))
