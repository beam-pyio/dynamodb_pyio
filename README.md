# dynamodb_pyio

![doc](https://github.com/beam-pyio/dynamodb_pyio/workflows/doc/badge.svg)
![test](https://github.com/beam-pyio/dynamodb_pyio/workflows/test/badge.svg)
[![release](https://img.shields.io/github/release/beam-pyio/dynamodb_pyio.svg)](https://github.com/beam-pyio/dynamodb_pyio/releases)
![pypi](https://img.shields.io/pypi/v/dynamodb_pyio)
![python](https://img.shields.io/pypi/pyversions/dynamodb_pyio)

[Amazon DynamoDB](https://aws.amazon.com/dynamodb/) is a serverless, NoSQL database service that allows you to develop modern applications at any scale. The Apache Beam Python I/O connector for Amazon DynamoDB (`dynamodb_pyio`) aims to integrate with the database service by supporting source and sink connectors. Currently, the sink connector is available.

## Installation

The connector can be installed from PyPI.

```bash
$ pip install dynamodb_pyio
```

## Usage

### Sink Connector

It has the main composite transform ([`WriteToDynamoDB`](https://beam-pyio.github.io/dynamodb_pyio/autoapi/dynamodb_pyio/io/index.html#dynamodb_pyio.io.WriteToDynamoDB)), and it expects a list or tuple _PCollection_ element. If the element is a tuple, the tuple's first element is taken. If the element is not of the accepted types, you can apply the [`GroupIntoBatches`](https://beam.apache.org/documentation/transforms/python/aggregation/groupintobatches/) or [`BatchElements`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements) transform beforehand. Then, the records of the element are written to a DynamoDB table with help of the [`batch_writer`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/batch_writer.html) of the boto3 package. Note that the batch writer will automatically handle buffering and sending items in batches. In addition, it will also automatically handle any unprocessed items and resend them as needed.

The transform also has an option that handles duplicate records.

- _dedup_pkeys_ - List of keys to be used for de-duplicating items in buffer.

#### Sink Connector Example

The transform can process a large number of records, thanks to the _batch writer_.

```python
import apache_beam as beam
from dynamodb_pyio.io import WriteToDynamoDB

records = [{"pk": str(i), "sk": i} for i in range(500)]

with beam.Pipeline() as p:
    (
        p
        | beam.Create([records])
        | WriteToDynamoDB(table_name=self.table_name)
    )
```

Duplicate records can be handled using the _dedup_pkeys_ option.

```python
import apache_beam as beam
from dynamodb_pyio.io import WriteToDynamoDB

records = [{"pk": str(1), "sk": 1} for _ in range(20)]

with beam.Pipeline() as p:
    (
        p
        | beam.Create([records])
        | WriteToDynamoDB(table_name=self.table_name, dedup_pkeys=["pk", "sk"])
    )
```

Batches of elements can be controlled further with the `BatchElements` or `GroupIntoBatches` transform

```python
import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from dynamodb_pyio.io import WriteToDynamoDB

records = [{"pk": str(i), "sk": i} for i in range(100)]

with beam.Pipeline() as p:
    (
        p
        | beam.Create(records)
        | BatchElements(min_batch_size=50, max_batch_size=50)
        | WriteToDynamoDB(table_name=self.table_name)
    )
```

See [Introduction to DynamoDB PyIO Sink Connector](/blog/2024/dynamodb-pyio-intro/) for more examples.

## Contributing

Interested in contributing? Check out the contributing guidelines. Please note that this project is released with a Code of Conduct. By contributing to this project, you agree to abide by its terms.

## License

`dynamodb_pyio` was created as part of the [Apache Beam Python I/O Connectors](https://github.com/beam-pyio) project. It is licensed under the terms of the Apache License 2.0 license.

## Credits

`dynamodb_pyio` was created with [`cookiecutter`](https://cookiecutter.readthedocs.io/en/latest/) and the `pyio-cookiecutter` [template](https://github.com/beam-pyio/pyio-cookiecutter).
