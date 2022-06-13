# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import bigquery
import pytest
from requests import Request
import main

# PROJECT = os.environ['GOOGLE_CLOUD_PROJECT']

# Variable Examples
dataset_name = 'cartoons'
dataset_location = 'US'
table_name = 'characters'
rows_to_insert = [
    {"full_name": "Phred Phlyntstone", "age": 32},
    {"full_name": "Wylma Phlyntstone", "age": 29},
    {"full_name": "Bambam Phlyntstone", "age": 5},
    {"full_name": "Pebbles Phlyntstone", "age": 2},
]
derived_table_name = 'characters_kids'


@pytest.fixture(scope="module", autouse=True)
def test_main():
    request = Request('GET', headers={
      'dataset_name'     : dataset_name,
      'dataset_location' : dataset_location,
      'table_name': table_name,
      'schema'  : [
            bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("age", "INTEGER", mode="REQUIRED")
        ],
      'rows_to_insert' : rows_to_insert,
      'derived_table_name': derived_table_name,
    })

    main.bigquery_create_dataset(request)
    main.bigquery_create_table(request)
    main.bigquery_populate_table(request)
    result = str(list(main.bigquery_derive_table(request)))
    print(result)
    assert """[Row(('Bammbamm Phlyntstone', 5), {'full_name': 0, 'age': 1}), Row(('Peabbles Phlyntstone', 2), {'full_name': 0, 'age': 1})]""" in result