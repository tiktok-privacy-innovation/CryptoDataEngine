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
from typing import List

from crypto_data_engine.data.base import DatasetBase
from crypto_data_engine.data.data_type import DataTypeBase


class ScalarDataset(DatasetBase):

    def __init__(self,
                 id_: int,
                 value,
                 data_type: List["DataTypeBase"] = None,
                 partition_num: int = 1,
                 placeholder: bool = False):
        self._id = id_
        self._data_type = data_type
        self._partition_num = partition_num
        self._placeholder = placeholder
        self._value = value

    def __str__(self):
        return f"ScalarDataset(value={self._value}, partition_num={self.partition_num})"

    def __repr__(self):
        return self.__str__()

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, v):
        self._value = v

    def is_placeholder(self):
        return self._placeholder

    def size(self):
        return 1

    def read_partition(self, index, **kwargs):
        return self._value

    def save_partition(self, index, data: List[List[str]], data_type: List["DataTypeBase"] = None, **kwargs):
        pass

    def collect(self):
        pass
