# Changelog

<!--next-version-placeholder-->

## v0.1.0 (24/09/2024)

✨NEW

- Add a composite transform (`WriteToDynamoDB`) that writes records to a DynamoDB table with help of the [`batch_writer`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/batch_writer.html) of the boto3 package.
  - The batch writer will automatically handle buffering and sending items in batches. In addition, it will also automatically handle any unprocessed items and resend them as needed.
- Provide an option that handles duplicate records
  - _dedup_pkeys_ - List of keys to be used for deduplicating items in buffer.
- Create a dedicated pipeline option (`DynamoDBOptions`) that reads AWS related values (e.g. `aws_access_key_id`) from pipeline arguments.
- Implement a metric object that records the total counts.
- Add unit and integration testing cases. The [moto](https://github.com/getmoto/moto) and [localstack-utils](https://docs.localstack.cloud/user-guide/tools/testing-utils/) are used for unit and integration testing respectively.
- Integrate with GitHub Actions by adding workflows for testing, documentation and release management.
