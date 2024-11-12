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
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from delta.tables import *
import re
import datetime
from datetime import datetime
import time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

LH = 'DEV_Ingestion_DP_Lakehouse'
table_name = 'department'
schema_name = 'humanresources'
select_stm = 'select DepartmentID,Name,GroupName,ModifiedDate  from humanresources.department'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehousePath = f"abfss://Ingestion_DP_DEV_workspace@onelake.dfs.fabric.microsoft.com/{LH}.Lakehouse"
tableName = schema_name + '_' + table_name
fileName = schema_name + '_' + table_name
extract_year = datetime.now().strftime('%Y')  
extract_month = datetime.now().strftime('%m') 
extract_day = datetime.now().strftime('%d')
dateColumn = "ModifiedDate"

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

fabric_metadata_df = spark.sql("SELECT * FROM DEV_Ingestion_DP_Lakehouse.fabric_metadata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sourceSystem = fabric_metadata_df.select(col("SOURCE_SYSTEM")).filter((col("SOURCE_TABLE") == table_name) & (col("SOURCE_SCHEMA") == schema_name)).collect()[0][0]
sourceSystem = sourceSystem.replace(' ', '')
deltaTablePath = f"{lakehousePath}/Tables/aw_{tableName}"
parquetFilePath = f"{lakehousePath}/Files/incremental/{sourceSystem}/filename={fileName}/execution_date={extract_year}-{extract_month}-{extract_day}/"

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

# Create a list of strings with '=' in between
combined_list = [f"{a} = {b}" for a, b in zip(stg_argument_list, dst_argument_list)]

# Join the list into a single string with a separator (e.g., ', ' or ' AND ')
mergeKeyExpr = ' and '.join(combined_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(deltaTablePath)
deltaTable = DeltaTable.forPath(spark,deltaTablePath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.read.parquet(parquetFilePath)
numUpdated = 0
numInserted = 0

#Check if table already exists; if it does, do an upsert and return how many rows were inserted and update; if it does not exist, return how many rows were inserted
if DeltaTable.isDeltaTable(spark,deltaTablePath):
    
    deltaTable = DeltaTable.forPath(spark,deltaTablePath)
    deltaTable.alias("dst").merge(
        df2.alias("stg"),
        mergeKeyExpr
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    history = deltaTable.history(1).select("operationMetrics")
    operationMetrics = history.collect()[0]["operationMetrics"]
    if operationMetrics is None:
        
        numInserted = 0
    else: 
        numUpdated = operationMetrics["numTargetRowsUpdated"]
        
else:
    df2.write.format("delta").save(deltaTablePath)  
    deltaTable = DeltaTable.forPath(spark,deltaTablePath)
    operationMetrics = history.collect()[0]["operationMetrics"]
    numInserted = operationMetrics["numTargetRowsInserted"]
    numUpdated = 0

#Get the latest date loaded into the table - this will be used for watermarking; return the max date, the number of rows inserted and number updated

df3 = spark.read.format("delta").load(deltaTablePath)
maxdate = df3.agg(max(dateColumn)).collect()[0][0]
maxdate_str = maxdate.strftime("%Y-%m-%d %H:%M:%S")

result = "maxdate="+maxdate_str +  "|numInserted="+str(numInserted)+  "|numUpdated="+str(numUpdated)

mssparkutils.notebook.exit(str(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filename = f'{lakehousePath}/Files/tmp_ctrl_log_delta'
print(filename)
df = spark.read.option("delimiter", ",").option("encoding", "utf-8").parquet(filename, header=True)
df.createOrReplaceTempView(f"temp_aggregated_metadata_delta_load")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
