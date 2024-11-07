# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ffa3f62e-f1b0-47c4-bc3b-c0c79f617f96",
# META       "default_lakehouse_name": "AdventureWorks",
# META       "default_lakehouse_workspace_id": "f70edf09-787c-41b5-a762-45afe1ab1e08"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

table_name = 'employee'
schema_name = 'humanresources'
select_stm = 'select BusinessEntityID,NationalIDNumber,LoginID,OrganizationLevel,JobTitle,BirthDate,MaritalStatus,Gender,HireDate,SalariedFlag,VacationHours,SickLeaveHours,CurrentFlag,rowguid,ModifiedDate  from humanresources.employee'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use regular expression to find everything between SELECT and FROM (case-insensitive)
pattern = re.search(r"SELECT (.*?) FROM", select_stm, re.IGNORECASE)

# Extract the match, if found
if pattern:
    all_cols = pattern.group(1)
    print(all_cols)
else:
    print("No match found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = table_name.lower()
schema_name =schema_name.lower()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric_metadata_df = spark.read.format("delta")\
                        .load(f"abfss://f70edf09-787c-41b5-a762-45afe1ab1e08@onelake.dfs.fabric.microsoft.com/13c0117a-c4df-4ecf-a943-f6af0d4b52b7/Tables/CTRL/FABRIC_METADATA")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric_metadata_df.createOrReplaceTempView("fabric_metadata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pk_stm = fabric_metadata_df.select(col("PRIMARY_KEY")).filter((col("SOURCE_TABLE") == table_name) & (col("SOURCE_SCHEMA") == schema_name)).collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dst_argument_list = ['dst.' + item for item in pk_stm.split(',')]
stg_argument_list = ['stg.' + item for item in pk_stm.split(',')]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(stg_argument_list, dst_argument_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a list of strings with '=' in between
combined_list = [f"{a} = {b}" for a, b in zip(stg_argument_list, dst_argument_list)]

# Join the list into a single string with a separator (e.g., ', ' or ' AND ')
result_string = ' and '.join(combined_list)

# Print the result
print(result_string)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

where_cond = [f"{a} is null" for a in dst_argument_list]
where_cond_string = '\n\t and '.join(where_cond)
print(where_cond_string)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## final statements
stg_table = 'stg_' + schema_name + '_' + table_name
destination_table = schema_name + '.' + table_name
lakehouse_prefix = '[AdventureWorks].[dbo].'
warehouse_prefix = '[AdventureWorks_DWH].'

insert_stm = f'''INSERT INTO {destination_table}
    SELECT stg.*
      FROM {lakehouse_prefix}{stg_table} AS stg
      LEFT 
      JOIN {warehouse_prefix}{destination_table} AS dst
        ON {result_string}
     WHERE {where_cond_string}'''


print(insert_stm)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pk_df = spark.sql(f'''select B.PRIMARY_KEY
                        from fabric_metadata  B 
                       where trim(B.SOURCE_SCHEMA) = "{schema_name}"
                         and trim(B.SOURCE_TABLE) = "{table_name}"
                  ''')

pk = pk_df.collect()[0][0]
# all_cols = all_cols_df.collect()[0][0]

# Split the strings into lists of words
pk_list = pk.split(",")
all_cols_list = all_cols.split(",")

# Convert the lists into sets
pk_set = set(pk_list)
all_cols_set = set(all_cols_list)

# Find the words that are in set2 but not in set1
upd_cols = all_cols_set.difference(pk_set)

# print("Columns not in primary key:", upd_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

upd_cols_list = list(upd_cols)
# upd_cols_list = upd_cols.split(",")
set_stm = [f"{a} = stg.{a}" for a in upd_cols_list]
set_stm_string = '\n\t, '.join(set_stm)
print(set_stm_string)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

update_stm = f'''
    UPDATE {warehouse_prefix}{destination_table}
       SET {set_stm_string}
    FROM {warehouse_prefix}{destination_table} AS dst
   INNER 
    JOIN {lakehouse_prefix}{stg_table} AS stg
      ON {result_string}'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit({'ins': insert_stm, 'upd': update_stm})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
