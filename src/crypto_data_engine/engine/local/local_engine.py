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
from concurrent.futures import ProcessPoolExecutor, wait
import dill
import logging

from crypto_data_engine.constants import PREDEFINED
from crypto_data_engine.data.data_io import Partition
from crypto_data_engine.engine import ComputingNode, EngineBase
from crypto_data_engine.engine.local.predefined import csv_sort_by_key, repartition, get_unique_rows_from_csv
from crypto_data_engine.utils.petace_utils import init_duet_vm


def udf(func_str, index, inputs, outputs, *args, **kwargs):
    func = dill.loads(func_str)
    vm, vm_mode = None, kwargs.get('vm_mode', 'duet')
    if vm_mode == 'duet':
        vm = init_duet_vm(index, kwargs)
    return func(vm, inputs, outputs, *args, **kwargs)


class LocalEngine(EngineBase):
    _predefined = {
        PREDEFINED.SORT_BY_KEY: csv_sort_by_key,
        PREDEFINED.REPARTITION: repartition,
        PREDEFINED.GET_UNIQUE_ROWS_FROM_CSV: get_unique_rows_from_csv
    }

    def __init__(self, context, max_workers=4, **_):
        self._context = context
        self._pool = ProcessPoolExecutor(max_workers=max_workers)

    def run_predefined(self, node: "ComputingNode", *args, **kwargs):
        function_name = node.content
        if function_name not in self._predefined:
            raise ValueError(f"unknown predefined function {node.content}")
        else:
            f = self._predefined[function_name]
            return f(node.inputs, node.outputs, *node.args, **node.kwargs)

    def run_udf(self, node: "ComputingNode", *args, **kwargs):
        try:
            partition_num = node.partition_num()
            futures = [
                self._pool.submit(udf, dill.dumps(node.content), i,
                                  [Partition(ds, i, partition_num)
                                   for ds in node.inputs], [Partition(ds, i, partition_num)
                                                            for ds in node.outputs], *node.args, **node.kwargs)
                for i in range(partition_num)
            ]

            wait(futures)
            success, results, errors = True, [], []
            for idx, future in enumerate(futures):
                try:
                    result = future.result()
                    results.append((idx, result))
                except Exception as e:
                    errors.append((idx, e))
            if errors:
                success = False

            if success is True:
                for ds in node.outputs:
                    ds.collect()

            logging.info("worker tasks finished")
            return success, results, errors
        except Exception as e:
            raise e

    def close(self):
        if self._pool is not None:
            self._pool.shutdown()
            self._pool = None
