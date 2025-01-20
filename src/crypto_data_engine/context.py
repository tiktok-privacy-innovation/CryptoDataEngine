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
from collections import OrderedDict
import logging
import logging.config
import time
from typing import Dict, List

from crypto_data_engine.constants import ComputeNodeType
from crypto_data_engine.data.csv_dataset import CSVDataset
from crypto_data_engine.data.scalar_dataset import ScalarDataset
from crypto_data_engine.data.data_type import DataTypeBase
from crypto_data_engine.engine import LocalEngine, SparkEngine, EngineBase, ComputingNode
from crypto_data_engine.settings import LOGGING_CONFIG

logging.config.dictConfig(LOGGING_CONFIG)


class Context:

    def __init__(self, **kwargs):
        self._datasets = []
        self._dag: Dict[int, "ComputingNode"] = OrderedDict()
        self._scheduler = None
        self._task_manager = None
        self._work_dir = kwargs["work_directory"]

        self._get_engine(**kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _get_engine(self, **kwargs):
        engine_type = kwargs.get('engine_type', 'local')
        if engine_type == 'local':
            self._engines: List["EngineBase"] = [LocalEngine(self, **kwargs)]
        elif engine_type == 'spark':
            self._engines: List["EngineBase"] = [SparkEngine(self, **kwargs)]
        else:
            raise ValueError(f"unknown engine type {engine_type}")

    def clear(self):
        self._dag.clear()

    def close(self):
        for engine in self._engines:
            engine.close()

    def csv_dataset(self,
                    file_path: str = None,
                    data_type: List["DataTypeBase"] = None,
                    placeholder: bool = False,
                    partitioner=None,
                    **kwargs):
        dataset = CSVDataset(id_=len(self._datasets),
                             file_path=file_path,
                             data_type=data_type,
                             work_dir=self._work_dir,
                             placeholder=placeholder,
                             partitioner=partitioner,
                             **kwargs)
        self._datasets.append(dataset)
        return dataset

    def scalar_dataset(self, value, **kwargs):
        dataset = ScalarDataset(id_=len(self._datasets), value=value, **kwargs)
        self._datasets.append(dataset)
        return dataset

    def compute(self, name, func, inputs, outputs, *args, **kwargs):
        if name is None and func is None:
            raise ValueError(f"name and func not initialized")
        function_type = ComputeNodeType.PREDEFINED if name is not None else ComputeNodeType.USER_DEFINED
        computing_node = ComputingNode(len(self._dag), function_type, name or func, inputs, outputs, args, kwargs, [])
        self._dag[computing_node.id_] = computing_node
        return computing_node

    def execute(self):
        engine = self._engines[0]
        for node in self._dag.values():
            start = time.time()
            if node.type == ComputeNodeType.PREDEFINED:
                ret = engine.run_predefined(node)
            else:
                ret = engine.run_udf(node)
            time_cost = time.time() - start
            logging.info(f"finished executing {node}, ret: {ret}, time cost: {time_cost}")
