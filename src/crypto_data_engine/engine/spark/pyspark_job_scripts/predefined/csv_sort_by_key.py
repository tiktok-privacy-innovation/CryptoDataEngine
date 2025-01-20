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
import uuid

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from crypto_data_engine.data.csv_dataset import CSVDataset
from crypto_data_engine.engine.spark.spark_utils import merge_csv_files

if __name__ == '__main__':
    spark = None
    try:
        print("csv_sort_by_key application started")
        # create spark context session
        conf = SparkConf().setAppName("Sort CSV by Key")
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc)

        # get args
        pickled_inputs = base64.b64decode(sys.argv[1])
        pickled_outputs = base64.b64decode(sys.argv[2])
        args = json.loads(sys.argv[3])
        sort_column_index = int(args[0])
        # 'true'/'false'
        # header = sys.argv[4]

        inputs: List["CSVDataset"] = dill.loads(pickled_inputs)
        outputs: List["CSVDataset"] = dill.loads(pickled_outputs)

        # read csv file
        if not isinstance(inputs[0], CSVDataset):
            raise ValueError("inputs should be CSVDataset object")
        if not isinstance(outputs[0], CSVDataset):
            raise ValueError("outputs should be CSVDataset object")

        print(inputs[0].file_path, outputs[0].file_path, sort_column_index)

        df = spark.read.format("csv").option("header", 'false').load(inputs[0].file_path)
        sorted_df = df.sort(df.columns[sort_column_index])
        random_file_path = f"/tmp/{uuid.uuid4()}"
        sorted_df.write.format('csv').option('header', 'false').mode("overwrite").save(random_file_path)
        merge_csv_files(random_file_path, outputs[0].file_path)
        print("csv_sort_by_key application finished")
    except Exception as e:
        print(e)
        raise
    finally:
        if spark:
            spark.stop()
        print("csv_sort_by_key application exit")
