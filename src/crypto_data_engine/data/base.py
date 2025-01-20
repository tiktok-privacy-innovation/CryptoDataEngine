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


class DatasetBase:
    _partition_num = 1

    def size(self):
        """get estimated data storage size"""
        raise NotImplementedError

    def shape(self):
        raise NotImplementedError

    def is_placeholder(self):
        raise NotImplementedError

    def read_partition(self, index, **kwargs):
        raise NotImplementedError

    def save_partition(self, index, data, **kwargs):
        raise NotImplementedError

    def collect(self):
        raise NotImplementedError

    @property
    def partition_num(self):
        return self._partition_num

    def repartition(self, partition_num: int):
        self._partition_num = partition_num

    def set_partitioner(self, udf):
        raise NotImplementedError
