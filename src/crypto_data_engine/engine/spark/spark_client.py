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
import subprocess
# import requests
# import re
# from typing import List

import logging


class SparkClient:

    def submit_job(self, app_name, script_path, app_args):
        raise NotImplementedError

    def status(self, app_id):
        raise NotImplementedError

    def stop_job(self, app_id):
        raise NotImplementedError


class SparkStandaloneClient(SparkClient):

    def __init__(self, master_url, spark_home):
        self.master_url = master_url
        self.spark_home = spark_home

    # todo: may need more flexible resources config
    def get_resource_config(self):
        return [
            "--executor-memory",
            "4g",
            "--executor-cores",
            "1",
            "--num-executors",
            "1",
        ]

    # def submit_job(self, app_name: str, script_path: str, app_args: List = None):
    #     submit_args = [
    #         f"{self.spark_home}/bin/spark-submit",
    #         "--master", f"spark://{self.master_url}:7077",
    #         "--name", app_name,
    #     ]
    #     submit_args += self.get_resource_config()
    #     submit_args += [script_path]
    #     if app_args is not None:
    #         submit_args += app_args
    #
    #     logging.info(submit_args)
    #
    #     process = subprocess.Popen(submit_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    #     app_id = None
    #     while True:
    #         line = process.stdout.readline().decode('utf-8')
    #         logging.info(line)
    #         if not line:
    #             break
    #         match = re.search(r"app ID (\S+)", line)
    #         if match:
    #             app_id = match.group(1)
    #             break
    #     return app_id
    #
    # def status(self, app_id):
    #     response = requests.get(f"http://{self.master_url}:8080/app/?appId={app_id}")
    #     if response.status_code != 200:
    #         raise Exception(f"Failed to get Spark Master Web UI: {response.status_code}")
    #
    #     pattern = r'<li><strong>State:</strong> (\w+)</li>'
    #     match = re.search(pattern, response.text)
    #     if match is None:
    #         raise Exception(f"failed to parse job status")
    #
    #     status = match.group(1)
    #     return status

    # def stop_job(self, app_id):
    #     stop_args = [
    #         f"{self.spark_home}/bin/spark-class",
    #         "org.apache.spark.deploy.Client",
    #         "kill",
    #         f"spark://{self.master_url}:7077",
    #         app_id
    #     ]
    #     process = subprocess.Popen(stop_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    #     stdout, stderr = process.communicate()
    #     logging.info(stdout)
    #     logging.info(stderr)

    def submit_and_wait(self, app_name, script_path, app_args):
        submit_args = [
            f"{self.spark_home}/bin/spark-submit",
            "--master",
            f"spark://{self.master_url}:7077",
            "--name",
            app_name,
        ]
        submit_args += self.get_resource_config()
        submit_args += [script_path]
        if app_args is not None:
            submit_args += app_args

        logging.info(submit_args)

        process = subprocess.Popen(submit_args, stdout=subprocess.PIPE)
        output, _ = process.communicate()
        logging.info(output.decode())
