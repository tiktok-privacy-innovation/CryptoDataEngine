# Copyright 2024 TikTok Pte. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import base64
import dill
import json
import sys
from typing import List

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from crypto_data_engine.data.data_io import Partition
from crypto_data_engine.data.base import DatasetBase
from crypto_data_engine.data.csv_dataset import CSVDataset
from crypto_data_engine.data.scalar_dataset import ScalarDataset
from crypto_data_engine.utils.petace_utils import init_duet_vm

pickled_udf = base64.b64decode(sys.argv[1])
pickled_inputs = base64.b64decode(sys.argv[2])
pickled_outputs = base64.b64decode(sys.argv[3])
partition_num = int(sys.argv[4])
args = json.loads(sys.argv[5])
kwargs = json.loads(sys.argv[6])


def map_func(index, iterator):
    # parse parameters
    udf = dill.loads(pickled_udf)
    inputs: List["DatasetBase"] = dill.loads(pickled_inputs)
    outputs: List["DatasetBase"] = dill.loads(pickled_outputs)

    vm, vm_mode = None, kwargs.get('vm_mode', 'duet')
    if vm_mode == 'duet':
        vm = init_duet_vm(index, kwargs)
    udf(vm, [Partition(ds, index, partition_num) for ds in inputs],
        [Partition(ds, index, partition_num) for ds in outputs], *args, **kwargs)
    return iterator


if __name__ == '__main__':
    spark = None
    try:
        print("udf application started")
        conf = SparkConf().setAppName("Spark UDF Application")
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc)

        rdd = sc.parallelize(range(partition_num), numSlices=partition_num)
        result = rdd.mapPartitionsWithIndex(map_func)
        result.collect()
        print("udf application finished")
    except Exception as e:
        print(e)
        raise
    finally:
        if spark:
            spark.stop()
        print("udf application exit")
