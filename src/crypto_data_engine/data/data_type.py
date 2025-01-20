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


class DataTypeBase:

    def from_str(self, value: str):
        raise NotImplementedError

    def to_str(self, value):
        raise NotImplementedError


class String(DataTypeBase):

    def from_str(self, value: str) -> str:
        return value

    def to_str(self, value: str) -> str:
        return value


class Integer(DataTypeBase):

    def from_str(self, value: str) -> int:
        return int(value)

    def to_str(self, value: int) -> str:
        return str(value)


class Float(DataTypeBase):

    def from_str(self, value: str) -> float:
        return float(value)

    def to_str(self, value: float) -> str:
        return str(value)


class ZOBool(DataTypeBase):
    """
    0/1 boolean
    """

    def from_str(self, value: str) -> bool:
        if value == '1':
            return True
        elif value == '0':
            return False
        raise ValueError

    def to_str(self, value: bool) -> str:
        return '0' if value is False else '1'


class DataType:
    Integer = Integer()
    Float = Float()
    String = String()
    ZOBool = ZOBool()
