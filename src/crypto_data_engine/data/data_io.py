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
from crypto_data_engine.data.base import DatasetBase


class Partition:

    def __init__(self, dataset: "DatasetBase", index: int, num_partitions: int = None):
        self._dataset = dataset
        self._index = index
        if num_partitions is not None:
            self._dataset.repartition(partition_num=num_partitions)

    def read(self, **kwargs):
        return self._dataset.read_partition(self._index, **kwargs)

    def save(self, data, **kwargs):
        return self._dataset.save_partition(self._index, data, **kwargs)
