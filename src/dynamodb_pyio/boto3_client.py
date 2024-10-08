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

import typing
import boto3
from apache_beam.options import pipeline_options

from dynamodb_pyio.options import DynamoDBOptions

__all__ = ["DynamoDBClient", "DynamoDBClientError"]


def get_http_error_code(exc):
    if hasattr(exc, "response"):
        return exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return None


class DynamoDBClientError(Exception):
    def __init__(self, message=None, code=None):
        self.message = message
        self.code = code


class DynamoDBClient(object):
    """
    Wrapper for boto3 library.
    """

    def __init__(self, options: typing.Union[DynamoDBOptions, dict]):
        """Constructor of the DynamoDBClient.

        Args:
            options (Union[DynamoDBOptions, dict]): Options to create a boto3 DynamoDB client.
        """
        assert boto3 is not None, "Missing boto3 requirement"
        if isinstance(options, pipeline_options.PipelineOptions):
            options = options.view_as(DynamoDBOptions)
            access_key_id = options.aws_access_key_id
            secret_access_key = options.aws_secret_access_key
            session_token = options.aws_session_token
            endpoint_url = options.endpoint_url
            use_ssl = not options.disable_ssl
            region_name = options.region_name
            api_version = options.api_version
            verify = options.verify
        else:
            access_key_id = options.get("aws_access_key_id")
            secret_access_key = options.get("aws_secret_access_key")
            session_token = options.get("aws_session_token")
            endpoint_url = options.get("endpoint_url")
            use_ssl = not options.get("disable_ssl", False)
            region_name = options.get("region_name")
            api_version = options.get("api_version")
            verify = options.get("verify")

        session = boto3.session.Session()
        self.resource = session.resource(
            service_name="dynamodb",
            region_name=region_name,
            api_version=api_version,
            use_ssl=use_ssl,
            verify=verify,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            aws_session_token=session_token,
        )

    def put_items_batch(self, records: list, table_name: str, dedup_pkeys: list = None):
        """Put records to an Amazon DynamoDB table using a batch writer object.

        Args:
            records (list): Records to send into an Amazon SQS queue.
            table_name (str): Amazon DynamoDB table name.
            dedup_pkeys (list, Optional): List of keys to be used for deduplicating items in buffer.

        Raises:
            DynamoDBClientError: DynamoDB client error.

        Returns:
            (Object): Boto3 response message.
        """

        if not isinstance(records, list):
            raise DynamoDBClientError("Records should be a list.")
        try:
            table = self.resource.Table(table_name)
            with table.batch_writer(overwrite_by_pkeys=dedup_pkeys or []) as batch:
                for record in records:
                    batch.put_item(Item=record)
        except Exception as e:
            raise DynamoDBClientError(str(e), get_http_error_code(e))
