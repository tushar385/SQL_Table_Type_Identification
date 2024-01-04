## 1. Libraries    
import pandas as pd
from langchain_experimental.sql import SQLDatabaseChain
from langchain.utilities import SQLDatabase
from langchain.llms import GooglePalm
import google.generativeai as genai
import langchain 
from langchain.globals import set_debug
from langchain.globals import set_verbose
import json
from datetime import datetime, date
import snowflake.connector

langchain.verbose = False
langchain.debug = False
set_debug(False)
set_verbose(False)


## 2. API & LLM Model(with gemini pro)  
genai.configure(api_key="xyz-abc12")

generation_config = {
  "temperature": 0.1,
  "top_p": 1,
  "top_k": 1
}

model = genai.GenerativeModel(model_name="gemini-pro",
                              generation_config=generation_config)


## 3. Database connection - snowflake / postgress   
snowflake_account = 'agb63276'
snowflake_user = 'Tushar'
snowflake_password = '1239'
snowflake_warehouse = 'ABC'
snowflake_database = 'Test'
snowflake_schema = 'DATA_S3'

conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema
)


## 4. Storing table name in a list  
existing_file_path = 'table_names.txt'
json_file_path = 'output_data.json'

existing_table_names = set()
try:
    with open(existing_file_path, 'r') as existing_file:
        existing_table_names = {line.strip() for line in existing_file}
except FileNotFoundError:
    pass 

query = f"SHOW TABLES IN DATABASE {snowflake_database}"
cursor = conn.cursor()
cursor.execute(query)

tables_with_schema = [(table[2], table[1], table[3]) for table in cursor.fetchall()]

filtered_table_names = []
for database, table, schema_name in tables_with_schema:
    full_table_name = f"{database}.{schema_name}.{table}"
    
    if not (
        schema_name.startswith('CONNECTED_APPS') or 
        schema_name.startswith('ACTION_STUDIO') or 
        schema_name.startswith('DATA_CHURN') or
        schema_name.startswith('VISUALISATIONS') or
        table.startswith('AI_EXPERIMENT_PARAMETERS') or
        table.startswith('ASYNC_ACTIVITY_LOGGER') or
        table.startswith('AUTOMATED_PIPELINE_STATUS') or
        table.startswith('CATEGORY_LAST_UPDATED') or
        table.startswith('CATEGORY_SCHEMA_MAPPING') or
        table.startswith('CATEGORY_SCHEMA_MAPPING_BCP') or
        table.startswith('CLEANUP_AND_VALIDATIONS') or
        table.startswith('CONNECTION_DETAILS') or
        table.startswith('CUSTOMER') or
        table.startswith('DATADDO_CONNECTOR_DETAILS') or
        table.startswith('DATADDO_INFO') or
        table.startswith('DATADDO_LOGS') or
        table.startswith('DATADDO_QUEUE') or
        table.startswith('DATASOURCES') or
        table.startswith('DATA_FETCH_FREQUENCY') or
        table.startswith('FIVETRAN_CONNECTOR_DETAILS') or
        table.startswith('GOLDEN_SCHEMA') or
        table.startswith('MODIFICATION') or
        table.startswith('QUESTIONNAIRE') or
        table.startswith('QUESTIONNAIRE_DETAILS') or
        table.startswith('SAMPLING_FIELDS') or
        table.startswith('SCHEDULE_INGESTION') or
        table.startswith('STEPFUNCTION_STANDARD_COLUMNS') or
        table.startswith('SUGGESTIONS') or
        table.startswith('TARGET_DATASETS') or
        table.startswith('VALIDATIONS') or
        table.startswith('VISUALISATIONS_CHART_CONFIG') or
        table.startswith('DATADDVISUALISATIONS_CHART_CONFIG_BACKUPO_QUEUE') or
        table.startswith('VISUALISATIONS_STATUS') or
        table.startswith('TABLE_CATAGORY') 
    ):
        filtered_table_names.append(full_table_name)
        
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)
        
new_table_names = set(filtered_table_names) - existing_table_names


with open(existing_file_path, 'a') as file:
    for new_table_name in new_table_names:
        file.write(f"{new_table_name}\n")
        
if not new_table_names:
    print("No new data appended. Exiting.")
    cursor.close()
    conn.close()
    exit()

## 5. Storing schema into Json    

table_json_data = {}

for table_name in new_table_names:
    column_query = f"SHOW COLUMNS IN TABLE {table_name}"
    cursor.execute(column_query)
    columns = [col[2] for col in cursor.fetchall()]

    data_query = f"SELECT * FROM {table_name} LIMIT 3"
    cursor.execute(data_query)
    sample_records = cursor.fetchall()

    table_data = []
    for record in sample_records:
        record_dict = {columns[i]: record[i] for i in range(len(columns))}
        table_data.append(record_dict)

    table_json_data[table_name] = table_data

try:
    with open(json_file_path, 'r') as json_file:
        existing_json_data = json.load(json_file)
except FileNotFoundError:
    existing_json_data = {}

existing_json_data.update(table_json_data)

with open(json_file_path, 'w') as json_file:
    json.dump(existing_json_data, json_file, indent=2, cls=DateTimeEncoder)

cursor.close()
conn.close()
   
formatted_json_data = json.dumps(table_json_data, indent=2, cls=DateTimeEncoder)     


## 6. Read Json & pass a prompt in LLM + Behaviour setup 
prompt = f"""
you are designed to categorise tables related to Billing, Usage, or Support Tickets for a Software as a Service (SaaS) product.

`{formatted_json_data}` has details of SQL tables stored in JSON format. The field name in the JSON object is my database.shema.table_name, where the key is my column name.

If there is any column that has details related to the amount paid or amount to be paid, then classify it as Billing category table.

If there is any column that has details related to the following 3 points, then categorise it as Usage category table
1. usage of a product 
2. duration of product or services used by a user
3. no of users used a product or service

If there are any columns that have details related to the following 7 points, then categorise it as Support Tickets category table
1. Ticket ID
2. Case ID
3. Support ID
4. Priority
5. Severity
6. Ticket Open Date
7. Ticket Close Date

Note - table names & catagory relationship should be unique identifier 

As an output print table names from formatted_json_data as per their classification.
eg of output -
Billing - database.shema.table_name1 
Usage - database.shema.table_name2 
Usage - database.shema.table_name3
Support - database.shema.table_name4 
No_category - database.shema.table_name5 

and if no table matches any catagory then ignore priniting of that catagory

"""

response = model.generate_content(prompt)

## 7. Storing O/P in list 

lines = response.text.strip().split("\n")

output_dict = {}

lines = response.text.strip().split("\n")

output_dict = {}

for line in lines:
    if " - " in line:
        category, table_name = line.split(" - ")
        output_dict.setdefault(category.strip(), []).append(table_name.strip())

table_catagory_json_path = 'table_catagory.json'
try:
    with open(table_catagory_json_path, 'r') as json_file:
        existing_table_catagory = json.load(json_file)
except FileNotFoundError:
    existing_table_catagory = {}

for category, tables in output_dict.items():
    existing_table_catagory.setdefault(category, []).extend(tables)

existing_table_catagory = {category: [table for table in tables if "." in table] for category, tables in existing_table_catagory.items()}

with open(table_catagory_json_path, 'w') as json_file:
    json.dump(existing_table_catagory, json_file, indent=2)

print(f"JSON data appended in {table_catagory_json_path}")


# 8 dumping to database

if not existing_table_catagory:
    print("No new data in 'table_catagory.json'. Exiting.")
    exit()

update_conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema='Public'
)

create_table_query = """
CREATE TABLE IF NOT EXISTS Public.table_catagory (
    category STRING,
    table_name STRING
);
"""

update_cursor = update_conn.cursor()
update_cursor.execute(create_table_query)

check_query = "SELECT COUNT(*) FROM Public.table_catagory WHERE category = %s AND table_name = %s"

insert_query = "INSERT INTO Public.table_catagory (category, table_name) VALUES (%s, %s)"

for category, tables in existing_table_catagory.items():
    for table in tables:
        update_cursor.execute(check_query, (category, table))
        count = update_cursor.fetchone()[0]

        if count == 0:
            update_cursor.execute(insert_query, (category, table))
            
drop_query = "DELETE FROM Public.table_catagory WHERE category LIKE '%category%'"
update_cursor.execute(drop_query)

update_conn.commit()

update_cursor.close()
update_conn.close()
