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
from crypto_data_engine.engine.local.predefined.impl import (csv_sort_by_key_impl, repartition_impl,
                                                             get_unique_rows_from_csv_impl)


def csv_sort_by_key(inputs, outputs, column_index, *args, **kwargs):
    return csv_sort_by_key_impl(inputs[0], outputs[0], column_index)


def repartition(inputs, outputs, partition_num, *args, **kwargs):
    return repartition_impl(inputs[0], partition_num)


def get_unique_rows_from_csv(inputs, outputs, skip_row, *args, **kwargs):
    return get_unique_rows_from_csv_impl(inputs[0], outputs[0], skip_row)
