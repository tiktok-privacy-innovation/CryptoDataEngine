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
import csv
from datetime import datetime
import dill
import os
from pathlib import Path
import random
import shutil
from typing import List

from crypto_data_engine.data.base import DatasetBase
from crypto_data_engine.data.data_type import DataTypeBase


class CSVDataset(DatasetBase):

    def __init__(self,
                 id_: int,
                 file_path: str = None,
                 data_type: List["DataTypeBase"] = None,
                 header: bool = False,
                 partition_num: int = 1,
                 work_dir: str = "",
                 placeholder: bool = False,
                 partitioner=None):
        self._id = id_
        self._has_header = header
        self._data_type = data_type
        self._shape = None
        self._partition_num = partition_num
        self._placeholder = placeholder
        self._work_dir = work_dir
        self._file_path = file_path or self._generate_random_filepath()
        Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)
        self._dilled_partitioner = dill.dumps(partitioner) if partitioner is not None else None
        self._partitioner = None

    def __str__(self):
        return f"CSVDataset(filepath={self.file_path}, partition_num={self.partition_num})"

    def __repr__(self):
        return self.__str__()

    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, path: str):
        self._file_path = path

    @property
    def partitioner(self):
        if self._partitioner is None:
            if self._dilled_partitioner is not None:
                self._partitioner = dill.loads(self._dilled_partitioner)
        return self._partitioner

    def set_partitioner(self, udf):
        self._dilled_partitioner = dill.dumps(udf)

    def is_placeholder(self):
        return self._placeholder

    def _generate_random_filepath(self):
        now = datetime.now()
        timestamp = now.strftime('%Y%m%d%H%M%S')
        rand_number = random.randint(1000, 9999)
        filename = "f_" + timestamp + "_" + str(rand_number) + '.tmp'
        return str((Path(self._work_dir) / 'tmp' / filename).absolute())

    def has_header(self):
        return self._has_header

    def size(self):
        if self.is_placeholder():
            return 0
        path = Path(self.file_path).absolute()
        if path.exists() is False:
            return 0
        else:
            return os.path.getsize(str(path))

    def shape(self):
        """
        return the shape of the csv file (number of rows, number of columns)
        :return: tuple (nrows, ncols)
        """
        if self.is_placeholder():
            self._shape = (0, 0)
        else:
            path = Path(self.file_path).absolute()
            if path.exists() is False:
                self._shape = (0, 0)
            else:
                with path.open("r") as f:
                    reader = csv.reader(f)
                    nrows = sum(1 for _ in reader)
                    f.seek(0)
                    ncols = len(next(reader)) if nrows > 0 else 0
                nrows = nrows - 1 if self.has_header() else nrows
                self._shape = (nrows, ncols)
        return self._shape

    def load_row(self, row: List[str], data_type: List["DataTypeBase"] = None):
        if data_type is None:
            return row
        else:
            return [data_type[idx].from_str(ele) for idx, ele in enumerate(row)]

    def save_row(self, row: List, data_type: List["DataTypeBase"] = None):
        if data_type is None:
            return row
        else:
            return [data_type[idx].to_str(ele) for idx, ele in enumerate(row)]

    def _read_by_row_index(self, start=0, end=None, data_type: List["DataTypeBase"] = None, **_):
        data_type = data_type or self._data_type
        result = []
        if self.is_placeholder():
            return result
        else:
            if self.has_header():
                start += 1
                if end is not None:
                    end += 1
            with open(self.file_path, 'r') as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if i < start:
                        continue
                    if end is not None and i >= end:
                        break
                    result.append(self.load_row(row, data_type))
            return result

    def _process_batch(self, batch, index, data_type):
        return [self.load_row(row, data_type) for row in batch if self.partitioner(row) % self._partition_num == index]

    def _read_by_partitioner(self, index, data_type: List["DataTypeBase"] = None, **_):
        data_type = data_type or self._data_type
        result = []
        if self.is_placeholder():
            pass
        else:
            batch_size = 10000
            with open(self.file_path, 'r') as f:
                reader = csv.reader(f)
                batch = []
                for row in reader:
                    batch.append(row)
                    if len(batch) >= batch_size:
                        result.extend(self._process_batch(batch, index, data_type))
                        batch = []
                if batch:
                    result.extend(self._process_batch(batch, index, data_type))
        return result

    def read_partition(self, index, data_type: List["DataTypeBase"] = None, **kwargs):
        if self.is_placeholder():
            return []
        else:
            nrows, _ = self.shape()
            if self.partitioner is None:
                chunk_size = max((nrows + self._partition_num - 1) // self._partition_num, 1)
                start = index * chunk_size
                end = min(start + chunk_size, nrows)
                return self._read_by_row_index(start, end, data_type=data_type, **kwargs)
            else:
                return self._read_by_partitioner(index, data_type=data_type, **kwargs)

    def save_partition(self, index, data: List[List[str]], data_type: List["DataTypeBase"] = None, **kwargs):
        data_type = data_type or self._data_type
        partition_filepath = f"{self.file_path}.partitions.{index}"
        with open(partition_filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows([self.save_row(ele, data_type) for ele in data])

    def collect(self):
        if self.is_placeholder():
            return
        partition_paths = [f"{self.file_path}.partitions.{index}" for index in range(self._partition_num)]
        with open(self.file_path, 'wb') as outfile:
            for path in partition_paths:
                path = Path(path)
                if path.exists() and path.is_file():
                    with open(path, 'rb') as infile:
                        shutil.copyfileobj(infile, outfile)
