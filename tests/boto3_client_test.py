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


import unittest
from moto import mock_aws

from dynamodb_pyio.boto3_client import DynamoDBClient, DynamoDBClientError


def describe_table(dynamodb_client, **kwargs):
    return dynamodb_client.client.describe_table(**kwargs)


def create_table(dynamodb_client, params):
    return dynamodb_client.client.create_table(**params)


def scan_table(dynamodb_client, **kwargs):
    paginator = dynamodb_client.client.get_paginator("scan")
    page_iterator = paginator.paginate(**kwargs)
    for page in page_iterator:
        print(page)


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
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        create_table(self.dynamodb_client, params)

    def test_create_items(self):
        resp = describe_table(self.dynamodb_client, TableName=self.table_name)
        print(resp)
        # records = [{"pk": str(i), "sk": i} for i in range(3)]
        requests = []
        # requests.append({"PutRequest": {"Item": {"pk": {"S": "1"}, "sk": {"S": "1"}}}})
        requests.append({"PutRequest": {"Item": {"pk": "1", "sk": "1"}}})

        self.dynamodb_client.client.batch_write_item(
            RequestItems={"test-table": requests}
        )

        # resp = self.dynamodb_client.batch_write_item(records, self.table_name)
        # print(resp)
