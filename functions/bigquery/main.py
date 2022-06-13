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

# [START bigquery_functions_quickstart]
from google.cloud import bigquery

client = bigquery.Client()

def bigquery_create_dataset(request):
    dataset_id = "{}.{}".format(client.project,request.headers.get("dataset_name"))
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = request.headers.get("dataset_location")
    dataset = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}".format(dataset_id))

def bigquery_create_table(request):
    table_id = "{}.{}.{}".format(client.project,request.headers.get("dataset_name"),request.headers.get("table_name"))
    schema = request.headers.get("schema")
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print("Created table {}".format(table_id))

def bigquery_populate_table(request):
    table_id = "{}.{}.{}".format(client.project,request.headers.get("dataset_name"),request.headers.get("table_name"))
    rows_to_insert = request.headers.get("rows_to_insert")
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    print("Table {} populated".format(table_id))

def bigquery_derive_table(request):
    # table_id = "{}.{}.{}".format(request.headers.get("project"),request.headers.get("dataset_name"),request.headers.get("table_name"))
    table_id = "{}.{}.{}".format(client.project,request.headers.get("dataset_name"),request.headers.get("table_name"))
    derived_table_id = "{}.{}.{}".format(client.project,request.headers.get("dataset_name"),request.headers.get("derived_table_name"))
    job_config = bigquery.QueryJobConfig(destination=derived_table_id)
    derived_table_sql = """
        SELECT *
        FROM `{}`
        WHERE age < 18
    """.format(table_id)
    query_job = client.query(derived_table_sql, job_config=job_config)
    result = query_job.result()
    print("Query results loaded to the derived table {} from source table {}".format(derived_table_id,table_id))
    return result

# [END bigquery_functions_quickstart]