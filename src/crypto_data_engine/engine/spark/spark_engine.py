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
import os

from crypto_data_engine.engine import ComputingNode, EngineBase
from crypto_data_engine.constants import PREDEFINED
from crypto_data_engine.engine.local.predefined import repartition, get_unique_rows_from_csv
from crypto_data_engine.engine.spark.spark_client import SparkStandaloneClient


class SparkEngine(EngineBase):
    _predefined = {
        PREDEFINED.SORT_BY_KEY: "csv_sort_by_key.py",
        PREDEFINED.REPARTITION: repartition,
        PREDEFINED.GET_UNIQUE_ROWS_FROM_CSV: "csv_get_unique_rows.py"
    }
    _base_path = "."

    def __init__(self, context, master_url=None, spark_home=None, **_):
        self._context = context
        self.master_url = os.environ.get('MASTER_URL', master_url)
        self.spark_home = os.environ.get('SPARK_HOME', spark_home)
        self._client = SparkStandaloneClient(self.master_url, self.spark_home)

    def run_predefined(self, node: "ComputingNode", *args, **kwargs):
        function_name = node.content
        if function_name not in self._predefined:
            raise ValueError(f"unknown predefined function {node.content}")
        else:
            if function_name == PREDEFINED.REPARTITION:
                repartition(node.inputs, node.outputs, *node.args, **node.kwargs)
            elif function_name == PREDEFINED.GET_UNIQUE_ROWS_FROM_CSV:
                get_unique_rows_from_csv(node.inputs, node.outputs, *node.args, **node.kwargs)
            else:
                script_path = f"{self._base_path}/pyspark_job_scripts/predefined/{self._predefined[function_name]}"
                spark_args = [
                    base64.b64encode(dill.dumps(node.inputs)),
                    base64.b64encode(dill.dumps(node.outputs)),
                    json.dumps(list(node.args)),
                    json.dumps(node.kwargs)
                ]
                self._client.submit_and_wait(function_name, script_path, spark_args)

    def run_udf(self, node: "ComputingNode", *args, **kwargs):
        try:
            args = [
                base64.b64encode(dill.dumps(node.content)),
                base64.b64encode(dill.dumps(node.inputs)),
                base64.b64encode(dill.dumps(node.outputs)),
                str(node.partition_num()),
                json.dumps(list(node.args)),
                json.dumps(node.kwargs)
            ]
            script_path = f"{self._base_path}/pyspark_job_scripts/udf.py"
            self._client.submit_and_wait("udf", script_path, args)
            # collect results
            for ds in node.outputs:
                ds.collect()
        except Exception as e:
            raise e

    def close(self):
        pass
