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


import decimal
import unittest
from moto import mock_aws
import boto3
from boto3.dynamodb.types import TypeDeserializer

from dynamodb_pyio.boto3_client import DynamoDBClient, DynamoDBClientError


def set_client(service_name="dynamodb"):
    options = {
        "service_name": service_name,
        "aws_access_key_id": "testing",
        "aws_secret_access_key": "testing",
        "region_name": "us-east-1",
    }
    return boto3.session.Session().client(**options)


def describe_table(**kwargs):
    return set_client().describe_table(**kwargs)


def create_table(params):
    return set_client().create_table(**params)


def to_int_if_decimal(v):
    try:
        if isinstance(v, decimal.Decimal):
            return int(v)
        else:
            return v
    except Exception:
        return v


def scan_table(**kwargs):
    paginator = set_client().get_paginator("scan")
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
    return items


@mock_aws
class TestBoto3Client(unittest.TestCase):
    table_name = "test-table"

    def setUp(self):
        options = {
            "aws_access_key_id": "testing",
            "aws_secret_access_key": "testing",
            "region_name": "us-east-1",
        }

        self.dynamodb_client = DynamoDBClient(options)
        params = {
            "TableName": self.table_name,
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
        create_table(params)

    def test_put_items_batch(self):
        records = [{"pk": str(i), "sk": i} for i in range(3)]
        self.dynamodb_client.put_items_batch(records, self.table_name)

        self.assertListEqual(records, scan_table(TableName=self.table_name))

    def test_put_items_batch_with_unsupported_record_type(self):
        # records should be a list
        records = {}
        self.assertRaises(
            DynamoDBClientError,
            self.dynamodb_client.put_items_batch,
            records,
            self.table_name,
        )

    def test_put_items_batch_duplicate_records_without_dedup_keys(self):
        records = [{"pk": str(1), "sk": 1} for i in range(3)]
        self.assertRaises(
            DynamoDBClientError,
            self.dynamodb_client.put_items_batch,
            records,
            self.table_name,
        )

    def test_put_items_batch_duplicate_records_with_dedup_keys(self):
        records = [{"pk": str(1), "sk": 1} for i in range(3)]
        self.dynamodb_client.put_items_batch(
            records, self.table_name, dedup_pkeys=["pk", "sk"]
        )
        self.assertListEqual(records[:1], scan_table(TableName=self.table_name))

    def test_put_items_batch_with_wrong_data_types(self):
        # pk and sk should be string and number respectively
        records = [{"pk": 1, "sk": str(1)} for i in range(3)]
        self.assertRaises(
            DynamoDBClientError,
            self.dynamodb_client.put_items_batch,
            records,
            self.table_name,
        )

    def test_put_items_batch_with_large_items(self):
        records = [{"pk": str(i), "sk": i} for i in range(5000)]
        self.dynamodb_client.put_items_batch(records, self.table_name)

        self.assertEqual(
            len(records),
            len(scan_table(TableName=self.table_name)),
        )
