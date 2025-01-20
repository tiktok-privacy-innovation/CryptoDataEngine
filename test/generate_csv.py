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
import random
import string


def generate_csv(filename, num_rows):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        # 写入标题行
        # writer.writerow(['id', 'name', 'value'])
        # 写入数据行
        for i in range(1, num_rows + 1):
            # 生成一个随机字符串作为 'name'
            name = ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            # 生成一个随机数作为 'value'
            value = random.uniform(0, 100)
            writer.writerow([i, name, value])


if __name__ == '__main__':
    generate_csv('data/a.csv', 100)
