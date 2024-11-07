# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "04428fdd-b859-4bbe-b9a3-9d1eff092896",
# META       "default_lakehouse_name": "DEV_Ingestion_DP_Lakehouse",
# META       "default_lakehouse_workspace_id": "63a7c4a0-7e46-4131-b5ee-0b33d69e52eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "04428fdd-b859-4bbe-b9a3-9d1eff092896"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

table_name = 'person'
schema_name = 'person'
select_stm = ''
destination_table = ''

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

# Define the file path
# file_path = 'abfss://AdventureWorks@onelake.dfs.fabric.microsoft.com/AdventureWorks.Lakehouse/Files/ddls.csv'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.read.option("delimiter", ",").csv(file_path, header=True)
df = sqlContext.table("ddls")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

select_stm = df.select(col(schema_name + "_" + table_name)).collect()[0][0]
select_stm_tbl = select_stm.split('from')[1]
select_stm_col = select_stm.split('from')[0]
select_stm_tbl = schema_name + "." + table_name
destination_table = schema_name + "_" + table_name
select_stm = select_stm_col + ' from ' + select_stm_tbl
# print(select_stm)
# print(select_stm_tbl)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit({'sql_stmt': select_stm, 'tbl': destination_table})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
