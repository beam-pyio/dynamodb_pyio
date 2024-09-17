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

import apache_beam as beam
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.transforms.util import BatchElements
from apache_beam import GroupIntoBatches
from apache_beam.options import pipeline_options
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dynamodb_pyio.boto3_client import DynamoDBClient, DynamoDBClientError
from dynamodb_pyio.io import WriteToDynamoDB, _DynamoDBWriteFn


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
    return sorted(items, key=lambda d: d["sk"])


@mock_aws
class TestWriteToDynamoDB(unittest.TestCase):
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

        self.pipeline_opts = pipeline_options.PipelineOptions(
            [
                "--aws_access_key_id",
                "testing",
                "--aws_secret_access_key",
                "testing",
                "--region_name",
                "us-east-1",
            ]
        )

    def test_write_to_dynamodb(self):
        records = [{"pk": str(i), "sk": i} for i in range(20)]
        with TestPipeline(options=self.pipeline_opts) as p:
            (p | beam.Create([records]) | WriteToDynamoDB(table_name=self.table_name))
        self.assertListEqual(records, scan_table(TableName=self.table_name))

    def test_write_to_dynamodb_with_large_items(self):
        # batch writer automatically handles buffering and sending items in batches
        records = [{"pk": str(i), "sk": i} for i in range(5000)]
        with TestPipeline(options=self.pipeline_opts) as p:
            (p | beam.Create([records]) | WriteToDynamoDB(table_name=self.table_name))
        self.assertListEqual(records, scan_table(TableName=self.table_name))

    def test_write_to_dynamodb_with_non_existing_table(self):
        records = [{"pk": str(i), "sk": i} for i in range(20)]
        with self.assertRaises(DynamoDBClientError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (
                    p
                    | beam.Create([records])
                    | WriteToDynamoDB(table_name="non-existing-table")
                )

    def test_write_to_dynamodb_with_unsupported_record_type(self):
        # supported types are list or tuple where the second element is a list!
        records = [{"pk": str(i), "sk": i} for i in range(20)]
        with self.assertRaises(DynamoDBClientError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (p | beam.Create(records) | WriteToDynamoDB(table_name=self.table_name))

    def test_write_to_dynamodb_with_wrong_data_type(self):
        # pk and sk should be string and number respectively
        records = [{"pk": i, "sk": str(i)} for i in range(20)]
        with self.assertRaises(DynamoDBClientError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (
                    p
                    | beam.Create([records])
                    | WriteToDynamoDB(table_name=self.table_name)
                )

    def test_write_to_dynamodb_duplicate_records_without_dedup_keys(self):
        records = [{"pk": str(1), "sk": 1} for i in range(20)]
        with self.assertRaises(DynamoDBClientError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (
                    p
                    | beam.Create([records])
                    | WriteToDynamoDB(table_name=self.table_name)
                )

    def test_write_to_dynamodb_duplicate_records_with_dedup_keys(self):
        records = [{"pk": str(1), "sk": 1} for i in range(20)]
        with TestPipeline(options=self.pipeline_opts) as p:
            (
                p
                | beam.Create([records])
                | WriteToDynamoDB(table_name=self.table_name, dedup_pkeys=["pk", "sk"])
            )
        self.assertListEqual(records[:1], scan_table(TableName=self.table_name))

    def test_write_to_dynamodb_with_batch_elements(self):
        records = [{"pk": str(i), "sk": i} for i in range(20)]
        with TestPipeline(options=self.pipeline_opts) as p:
            (
                p
                | beam.Create(records)
                | BatchElements(min_batch_size=10, max_batch_size=10)
                | WriteToDynamoDB(table_name=self.table_name)
            )
        self.assertListEqual(records, scan_table(TableName=self.table_name))

    def test_write_to_dynamodb_with_group_into_batches(self):
        records = [(i % 2, {"pk": str(i), "sk": i}) for i in range(20)]
        with TestPipeline(options=self.pipeline_opts) as p:
            (
                p
                | beam.Create(records)
                | GroupIntoBatches(batch_size=10)
                | WriteToDynamoDB(table_name=self.table_name)
            )
        self.assertListEqual(
            [r[1] for r in records], scan_table(TableName=self.table_name)
        )

    def test_metrics(self):
        records = [{"pk": str(i), "sk": i} for i in range(20)]

        pipeline = TestPipeline()
        (
            pipeline
            | beam.Create(records)
            | BatchElements(min_batch_size=10, max_batch_size=10)
            | WriteToDynamoDB(table_name=self.table_name)
        )

        res = pipeline.run()
        res.wait_until_finish()

        ## verify total_elements_count
        metric_results = res.metrics().query(
            MetricsFilter().with_metric(_DynamoDBWriteFn.total_elements_count)
        )
        total_elements_count = metric_results["counters"][0]
        self.assertEqual(total_elements_count.key.metric.name, "total_elements_count")
        self.assertEqual(total_elements_count.committed, 20)
