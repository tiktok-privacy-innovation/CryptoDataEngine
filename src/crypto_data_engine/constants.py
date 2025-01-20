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


class ByteSize:
    B = 1
    KB = 1024 * B
    MB = 1024 * KB
    GB = 1024 * MB
    TB = 1024 * GB


class ComputeNodeType:
    PREDEFINED = "PREDEFINED"
    USER_DEFINED = "USER_DEFINED"

    all = [PREDEFINED, USER_DEFINED]


class PREDEFINED:
    REPARTITION = "REPARTITION"
    SORT_BY_KEY = "SORT_BY_KEY"
    SHUFFLE = "SHUFFLE"
    GET_UNIQUE_ROWS_FROM_CSV = "GET_UNIQUE_ROWS_FROM_CSV"
