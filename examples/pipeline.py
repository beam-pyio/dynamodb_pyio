#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import decimal
import logging

import boto3
from boto3.dynamodb.types import TypeDeserializer

import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from dynamodb_pyio.io import WriteToDynamoDB

TABLE_NAME = "dynamodb-pyio-test"


def get_table(table_name):
    resource = boto3.resource("dynamodb")
    return resource.Table(table_name)


def create_table(table_name):
    client = boto3.client("dynamodb")
    try:
        client.describe_table(TableName=table_name)
        table_exists = True
    except Exception:
        table_exists = False
    if not table_exists:
        print(">> create table...")
        params = {
            "TableName": table_name,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        client.create_table(**params)
        get_table(table_name).wait_until_exists()


def to_int_if_decimal(v):
    try:
        if isinstance(v, decimal.Decimal):
            return int(v)
        else:
            return v
    except Exception:
        return v


def scan_table(**kwargs):
    client = boto3.client("dynamodb")
    paginator = client.get_paginator("scan")
    page_iterator = paginator.paginate(**kwargs)
    items = []
    for page in page_iterator:
        for document in page["Items"]:
            items.append(
                {
                    k: to_int_if_decimal(TypeDeserializer().deserialize(v))
                    for k, v in document.items()
                }
            )
    return sorted(items, key=lambda d: d["sk"])


def truncate_table(table_name):
    records = scan_table(TableName=TABLE_NAME)
    table = get_table(table_name)
    with table.batch_writer() as batch:
        for record in records:
            batch.delete_item(Key=record)


def mask_secrets(d: dict):
    return {k: (v if k.find("aws") < 0 else "x" * len(v)) for k, v in d.items()}


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--table_name", default=TABLE_NAME, type=str, help="DynamoDB table name"
    )
    parser.add_argument(
        "--num_records", default="500", type=int, help="Number of records"
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known_args - {known_args}")
    print(f"pipeline options - {mask_secrets(pipeline_options.display_data())}")

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "CreateElements"
            >> beam.Create(
                [
                    {
                        "pk": str(int(1 if i >= known_args.num_records / 2 else i)),
                        "sk": int(1 if i >= known_args.num_records / 2 else i),
                    }
                    for i in range(known_args.num_records)
                ]
            )
            | "BatchElements" >> BatchElements(min_batch_size=100, max_batch_size=200)
            | "WriteToDynamoDB"
            >> WriteToDynamoDB(
                table_name=known_args.table_name, dedup_pkeys=["pk", "sk"]
            )
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    create_table(TABLE_NAME)
    print(">> start pipeline...")
    run()
    print(">> check number of records...")
    print(len(scan_table(TableName=TABLE_NAME)))
    print(">> truncate table...")
    truncate_table(TABLE_NAME)
