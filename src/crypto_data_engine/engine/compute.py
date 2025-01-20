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
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from crypto_data_engine.constants import ComputeNodeType
from crypto_data_engine.data.base import DatasetBase


@dataclass
class ComputingNode:
    id_: int
    type: "ComputeNodeType"
    content: Any
    inputs: List["DatasetBase"]
    outputs: List["DatasetBase"]
    args: Tuple
    kwargs: Dict
    depends: List

    def partition_num(self) -> int:
        partition_num = self.kwargs.get("partition_num")
        if partition_num is None:
            partition_nums = [ds._partition_num for ds in self.inputs]
            if len(set(partition_nums)) != 1:
                raise ValueError("datasets partition num not equal")
            else:
                partition_num = partition_nums[0]
        return partition_num

    def __str__(self):
        return f"type={self.type}, inputs={self.inputs}, outputs={self.outputs}, args={self.args}, kwargs={self.kwargs}"

    def __repr__(self):
        return self.__str__()
