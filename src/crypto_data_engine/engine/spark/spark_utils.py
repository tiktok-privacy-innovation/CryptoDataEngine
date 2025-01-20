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
import os
from pathlib import Path
import shutil


def get_spark_work_dir():
    dir_ = None
    spark_workdir = os.environ.get("SPARK_WORKDIR")
    if spark_workdir is not None:
        dir_ = spark_workdir
    else:
        spark_home = os.environ.get("SPARK_HOME")
        if spark_home is not None:
            dir_ = f"{spark_home}/work"
        else:
            home = os.environ.get("HOME")
            if home is not None:
                dir_ = f"{home}/spark-3.5.1-bin-hadoop3/work"
    if dir_ is None or not Path(dir_).exists() or not Path(dir_).is_dir():
        raise Exception("spark work directory not found")
    return dir_


def merge_csv_files(output_path, final_output_path):
    # 获取所有分区文件的路径
    csv_files = []
    for root, _, files in os.walk(output_path):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))

    # 按文件名排序
    csv_files.sort()

    # 打开最终输出文件
    with open(final_output_path, 'wb') as outfile:
        # 遍历每个 CSV 文件
        for i, file in enumerate(csv_files):
            with open(file, 'rb') as infile:
                # 将整个文件内容读取并写入到最终输出文件
                shutil.copyfileobj(infile, outfile)
                # 如果不是最后一个文件，添加换行符
                if i < len(csv_files) - 1:
                    outfile.write(b'\n')


if __name__ == '__main__':
    merge_csv_files('/tmp/sorted', '/tmp/a_sorted.csv')
