# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "04428fdd-b859-4bbe-b9a3-9d1eff092896",
# META       "default_lakehouse_name": "DEV_Lakehouse",
# META       "default_lakehouse_workspace_id": "63a7c4a0-7e46-4131-b5ee-0b33d69e52eb",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Variables Initialization

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import *
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the file path
df = sqlContext.table("ddls")

tables = df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Functions Definition

# CELL ********************

## gather the count of all the loaded tables into a dictionary
def target_stats (tables):
    cnt_dict = {}
    for t in tables:
        df_t = sqlContext.table('stg_' + t)
        
        cnt_dict[t] = df_t.count()
    return cnt_dict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## gather the count of all the loaded tables using the systemic tables of the source system
def source_stats():
    source_stats = spark.sql('''
                        select distinct concat(lower(o.name0), "_", lower(o.name1)) as tbl_name, p.rows as cnt
                        from sys_objectIDs o 
                        inner join sys_partitions p 
                        on o.object_id = p.object_id
                        ''')
    results = source_stats.toJSON().map(lambda j: json.loads(j)).collect()
    result_dict = {item['tbl_name']: item['cnt'] for item in results}

    return result_dict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def dict_comparison(source, target):
    for key, value in source.items():
        if source[key] != target[key]:
            print("Loading of table", key, "failed.")
            print("Source system count of rows: ", source[key])
            print("Target system count of rows: ", target[key])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cnt_dict = target_stats (tables)
result_dict = source_stats()
dict_comparison(cnt_dict, result_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
