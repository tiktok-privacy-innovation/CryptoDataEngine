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
import multiprocessing
from typing import List

from crypto_data_engine.context import Context
from crypto_data_engine.constants import PREDEFINED
from crypto_data_engine.data.data_type import DataType
from crypto_data_engine.data.data_io import Partition


class SinglePartyFunction:

    def udf(self, vm, index, inputs, outputs, *_, **__):
        if vm is None:
            print("vm is None")
        else:
            print("vm is initialized")
        for idx, input_ in enumerate(inputs):
            rows = input_.read()
            for row in rows:
                row[2] = str(float(row[2]) * (index + 1))
            outputs[idx].save(rows)
        return True


def single_party_demo(index=0):
    with Context(work_directory='') as ctx:
        local = ctx.csv_dataset(file_path='../../test/data/a.csv',
                                data_type=[DataType.Integer, DataType.String, DataType.Float])
        # sorted src data
        local_sorted = ctx.csv_dataset(file_path=f'../../test/data/a_sorted_{index}.csv',
                                       data_type=[DataType.Integer, DataType.String, DataType.Float])
        # final output
        # result = ctx.dataset(file_path='../../test/data/result.csv')
        result = ctx.csv_dataset()

        ctx.compute(PREDEFINED.SORT_BY_KEY, None, [local], [local_sorted], 1)

        ctx.compute(PREDEFINED.REPARTITION, None, [local_sorted], [], 1)
        ctx.compute(None, SinglePartyFunction().udf, [local_sorted], [result], vm_mode='none')
        ctx.execute()
        print(index, result.file_path)


def scalar_dataset_demo():
    with Context(work_directory='') as ctx:
        scalar = ctx.scalar_dataset(value=1.0)
        partition = Partition(dataset=scalar, index=0)
        print(partition.read())


def user_defined_partitioner_demo():

    def f():

        def g(_: List[str]):
            return 0

        return g

    ctx = Context(work_directory='')
    local = ctx.csv_dataset(file_path='../../test/data/a.csv',
                            data_type=[DataType.Integer, DataType.String, DataType.Float],
                            partitioner=None)

    local.set_partitioner(f())
    local.repartition(2)
    partition = Partition(dataset=local, index=0)
    print(partition.read())
    partition = Partition(dataset=local, index=1)
    print(partition.read())


def multiprocess():
    p1 = multiprocessing.Process(target=single_party_demo, args=(1,))
    p2 = multiprocessing.Process(target=single_party_demo, args=(2,))
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    print("end")


def get_unique_rows_from_csv_demo():
    with Context(work_directory='') as ctx:
        local = ctx.csv_dataset(file_path='../../test/data/unique.csv')
        local_unique = ctx.scalar_dataset(value=1.0)
        ctx.compute(PREDEFINED.GET_UNIQUE_ROWS_FROM_CSV, None, [local], [local_unique], 0)
        ctx.execute()
        print(local_unique.value)
        partition = Partition(dataset=local_unique, index=0)
        print(partition.read())
        # ctx.execute()


def new_context():
    ctx = Context(work_directory='.', engine_type='local', max_workers=8)
    ctx.close()

    ctx = Context(work_directory='.', engine_type='spark', master_url='1234')
    ctx.close()


def print_dag():
    with Context(work_directory='') as ctx:
        local = ctx.csv_dataset(file_path='../../test/data/a.csv',
                                data_type=[DataType.Integer, DataType.String, DataType.Float])
        # sorted src data
        local_sorted = ctx.csv_dataset(file_path=f'../../test/data/a_sorted.csv',
                                       data_type=[DataType.Integer, DataType.String, DataType.Float])
        # final output
        # result = ctx.dataset(file_path='../../test/data/result.csv')
        result = ctx.csv_dataset()

        ctx.compute(PREDEFINED.SORT_BY_KEY, None, [local], [local_sorted], 1)

        ctx.compute(PREDEFINED.REPARTITION, None, [local_sorted], [], 1)
        ctx.compute(None, SinglePartyFunction().udf, [local_sorted], [result], vm_mode='none')

        print(ctx._dag)


if __name__ == '__main__':
    # print_dag()
    # new_context()
    # multiprocess()
    single_party_demo()
    # scalar_dataset_demo()
    # user_defined_partitioner_demo()
    # get_unique_rows_from_csv_demo()
