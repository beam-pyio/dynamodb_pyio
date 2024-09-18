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
import pytest
import docker
import boto3
from boto3.dynamodb.types import TypeDeserializer
import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from apache_beam import GroupIntoBatches
from apache_beam.options import pipeline_options
from apache_beam.testing.test_pipeline import TestPipeline
from localstack_utils.localstack import startup_localstack, stop_localstack

from dynamodb_pyio.io import WriteToDynamoDB


def create_client(service_name="dynamodb"):
    return boto3.client(
        service_name=service_name,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        endpoint_url="http://localhost:4566",
    )


def create_table(params):
    return create_client().create_table(**params)


def to_int_if_decimal(v):
    try:
        if isinstance(v, decimal.Decimal):
            return int(v)
        else:
            return v
    except Exception:
        return v


def scan_table(**kwargs):
    paginator = create_client().get_paginator("scan")
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


@pytest.mark.integration
class TestWriteToSqs(unittest.TestCase):
    table_name = "test-table"

    def setUp(self):
        startup_localstack()

        ## create resources
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
                "--runner",
                "FlinkRunner",
                "--parallelism",
                "1",
                "--aws_access_key_id",
                "testing",
                "--aws_secret_access_key",
                "testing",
                "--aws_access_key_id",
                "testing",
                "--region_name",
                "us-east-1",
                "--endpoint_url",
                "http://localhost:4566",
            ]
        )

    def tearDown(self):
        stop_localstack()
        docker_client = docker.from_env()
        docker_client.containers.prune()
        return super().tearDown()

    def test_write_to_dynamodb(self):
        records = [{"pk": str(i), "sk": i} for i in range(500)]
        with TestPipeline(options=self.pipeline_opts) as p:
            (p | beam.Create([records]) | WriteToDynamoDB(table_name=self.table_name))
        self.assertListEqual(records, scan_table(TableName=self.table_name))

    def test_write_to_dynamodb_duplicate_records_without_dedup_keys(self):
        records = [{"pk": str(1), "sk": 1} for _ in range(500)]
        with self.assertRaises(RuntimeError):
            with TestPipeline(options=self.pipeline_opts) as p:
                (
                    p
                    | beam.Create([records])
                    | WriteToDynamoDB(table_name=self.table_name)
                )

    def test_write_to_dynamodb_duplicate_records_with_dedup_keys(self):
        records = [{"pk": str(1), "sk": 1} for _ in range(500)]
        with TestPipeline(options=self.pipeline_opts) as p:
            (
                p
                | beam.Create([records])
                | WriteToDynamoDB(table_name=self.table_name, dedup_pkeys=["pk", "sk"])
            )
        self.assertListEqual(records[:1], scan_table(TableName=self.table_name))

    def test_write_to_dynamodb_with_batch_elements(self):
        records = [{"pk": str(i), "sk": i} for i in range(500)]
        with TestPipeline(options=self.pipeline_opts) as p:
            (
                p
                | beam.Create(records)
                | BatchElements(min_batch_size=50, max_batch_size=100)
                | WriteToDynamoDB(table_name=self.table_name)
            )
        self.assertListEqual(records, scan_table(TableName=self.table_name))

    def test_write_to_dynamodb_with_group_into_batches(self):
        records = [(i % 2, {"pk": str(i), "sk": i}) for i in range(500)]
        with TestPipeline(options=self.pipeline_opts) as p:
            (
                p
                | beam.Create(records)
                | GroupIntoBatches(batch_size=100)
                | WriteToDynamoDB(table_name=self.table_name)
            )
        self.assertListEqual(
            [r[1] for r in records], scan_table(TableName=self.table_name)
        )
