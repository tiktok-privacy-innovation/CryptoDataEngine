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
from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(name='crypto-data-engine',
      version='0.1.0',
      description='crypto-data-engine is a bigdata computing framework for private computing',
      author="PrivacyGo-PETPlatform",
      author_email="privacygo-petplatform@tiktok.com",
      packages=find_packages('src'),
      package_dir={'': 'src'},
      install_requires=requirements)
