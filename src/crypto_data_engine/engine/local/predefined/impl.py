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
from crypto_data_engine.data.csv_dataset import CSVDataset
from crypto_data_engine.data.scalar_dataset import ScalarDataset


def csv_sort_by_key_impl(src: "CSVDataset", dst: "CSVDataset", column_index: int):
    """
    in memory csv sort
    """
    import csv
    import operator

    with open(src.file_path, 'r') as f:
        reader = csv.reader(f)
        data = list(reader)

    data.sort(key=operator.itemgetter(column_index))

    with open(dst.file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)


def repartition_impl(src: "CSVDataset", partition_num: int):
    src.repartition(partition_num)


def get_unique_rows_from_csv_impl(src: "CSVDataset", dst: "ScalarDataset", skip_row: int):
    import csv
    import json

    if skip_row not in [0, 1]:
        raise ValueError("skip_row must be 0 or 1")
    unique_rows = set()
    with open(src.file_path, 'r') as f:
        reader = csv.reader(f)
        for i in range(skip_row):
            next(reader)
        for row in reader:
            unique_rows.add(tuple(row))

    unique_rows_sorted = sorted(unique_rows)
    dst.value = json.dumps(unique_rows_sorted)
