# CryptoDataEngine: A robust engine designed specifically for large scale privacy computing


CryptoDataEngine is a robust engine designed specifically for handling large-scale computational problems in privacy computing scenarios.
It provides a powerful solution for distributed privacy computing,
enabling efficient and secure processing of large datasets while ensuring privacy preservation.


## Key Features

- Distributed Privacy Computing: CryptoDataEngine leverages the power of distributed computing to handle large-scale privacy computations. This allows for efficient processing of massive datasets across multiple nodes, significantly improving computational speed and scalability.

- Support for Single-Machine Multi-Process Parallelism: In addition to distributed parallelism, CryptoDataEngine also supports single-machine multiprocess parallelism. This provides flexibility for different computational needs and resource availability.

- Integration with Spark: CryptoDataEngine is built to work seamlessly with Spark, a leading platform for distributed data processing. This integration enables CryptoDataEngine to take full advantage of Spark's capabilities in handling large datasets and performing complex computations.

- Privacy Preservation: At the core of CryptoDataEngine is a strong commitment to privacy. The engine is designed to perform computations in a way that preserves the privacy of the data, making it an ideal solution for scenarios where data privacy is a critical requirement.


## Getting Started

### Requirements

| System | Toolchain                     |
|--------|-------------------------------|
| Linux  | Python (==3.9), pip(>=23.3.1) |


### How to Build

To build PETNet as a docker image, execute:

```bash
python3 setup.py sdist bdist_wheel
```

### How to Run

Here is an example to show how to use CryptoDataEngine to parallize privacy computation.

```python
from crypto_data_engine.context import Context
from crypto_data_engine import PREDEFINED
from crypto_data_engine.data.data_type import DataType


class SinglePartyFunction:
    def udf(self, index, inputs, outputs, *args, **kwargs):
        for idx, input_ in enumerate(inputs):
            rows = input_.read()
            for row in rows:
                row[2] = str(float(row[2]) * (index + 1))
            outputs[idx].save(rows)
        return True


with Context(work_directory='') as ctx:
    local = ctx.csv_dataset(
        file_path='../../test/data/a.csv',
        data_type=[DataType.Integer, DataType.String, DataType.Float]
    )
    # sorted src data
    local_sorted = ctx.csv_dataset(
        file_path=f'../../test/data/a_sorted.csv',
        data_type=[DataType.Integer, DataType.String, DataType.Float]
    )
    # final output
    # result = ctx.dataset(file_path='../../test/data/result.csv')
    result = ctx.csv_dataset()

    ctx.compute(PREDEFINED.SORT_BY_KEY, None, [local], [local_sorted], 1)

    ctx.compute(PREDEFINED.REPARTITION, None, [local_sorted], [], 1)
    ctx.compute(None, SinglePartyFunction().udf, [local_sorted], [result])
    ctx.execute()

    print(result.file_path)
```


## Contribution

Please check [Contributing](CONTRIBUTING.md) for more details.


## Code of Conduct

Please check [Code of Conduct](CODE_OF_CONDUCT.md) for more details.


## License

This project is licensed under the [Apache-2.0 License](LICENSE).


## Disclaimers

This software is not an officially supported product of TikTok. It is provided as-is, without any guarantees or warranties, whether express or implied.
