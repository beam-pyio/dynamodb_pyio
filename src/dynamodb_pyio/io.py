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

import logging
import typing
import apache_beam as beam
from apache_beam import metrics
from apache_beam.pvalue import PCollection

from dynamodb_pyio.boto3_client import DynamoDBClient
from dynamodb_pyio.options import DynamoDBOptions


__all__ = ["WriteToDynamoDB"]


class _DynamoDBWriteFn(beam.DoFn):
    """Create the connector can send messages in batch to an Amazon DynamoDB table.

    Args:
        table_name (str): Amazon DynamoDB table name.
        dedup_pkeys (list): List of keys to be used for deduplicating records in buffer.
        options (Union[DynamoDBOptions, dict]): Options to create a boto3 dynamodb client.
    """

    total_elements_count = metrics.Metrics.counter(
        "_DynamoDBWriteFn", "total_elements_count"
    )

    def __init__(
        self,
        table_name: str,
        dedup_pkeys: list,
        options: typing.Union[DynamoDBOptions, dict],
    ):
        """Constructor of _DynamoDBWriteFn

        Args:
            table_name (str): Amazon DynamoDB table name.
            dedup_pkeys (list): List of keys to be used for deduplicating records in buffer.
            options (Union[DynamoDBOptions, dict]): Options to create a boto3 dynamodb client.
        """
        super().__init__()
        self.table_name = table_name
        self.dedup_pkeys = dedup_pkeys
        self.options = options

    def start_bundle(self):
        self.client = DynamoDBClient(self.options)

    def process(self, element):
        if isinstance(element, tuple):
            element = element[1]
        self.client.put_items_batch(element, self.table_name, self.dedup_pkeys)
        self.total_elements_count.inc(len(element))
        logging.info(f"total {len(element)} elements processed...")


class WriteToDynamoDB(beam.PTransform):
    """A transform that puts records to an Amazon DynamoDB table.

    Takes an input PCollection and put them in batch using the boto3 package.
    For more information, visit the `Boto3 Documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/batch_writer.html>`__.

    Note that, if the PCollection element is a tuple (i.e. keyed stream), only the value is used to put records in batch.

    Args:
        table_name (str): Amazon DynamoDB table name.
        dedup_pkeys (list, Optional): List of keys to be used for deduplicating records in buffer.
    """

    def __init__(self, table_name: str, dedup_pkeys: list = None):
        """Constructor of the transform that puts records into an Amazon DynamoDB table.

        Args:
            table_name (str): Amazon DynamoDB table name.
            dedup_pkeys (list, Optional): List of keys to be used for deduplicating items in buffer.
        """
        super().__init__()
        self.table_name = table_name
        self.dedup_pkeys = dedup_pkeys

    def expand(self, pcoll: PCollection):
        options = pcoll.pipeline.options.view_as(DynamoDBOptions)
        return pcoll | beam.ParDo(
            _DynamoDBWriteFn(self.table_name, self.dedup_pkeys, options)
        )
