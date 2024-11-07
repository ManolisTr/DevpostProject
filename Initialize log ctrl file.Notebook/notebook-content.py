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

from pyspark.sql.types import StructType, StructField, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

filepath = ''
filename = ''

# df = spark.sql("SELECT * FROM DEV_Ingestion_DP_Lakehouse.tmp_fabric_metadata_delta LIMIT 1000")
# display(df)

# df = spark.read.parquet("Files/tmp_ctrl_log_delta/execution_date=2024-10-25/part-00000-6ec240f2-efad-4dad-903b-f6b3c23dbc7d.c000.snappy.parquet")
# # df now is a Spark DataFrame containing parquet data from "Files/tmp_ctrl_log_delta/execution_date=2024-10-25/part-00000-6ec240f2-efad-4dad-903b-f6b3c23dbc7d.c000.snappy.parquet".
# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if notebookutils.fs.exists(filepath+filename):
    notebookutils.fs.rm(filepath+filename, recurse= True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField("execution_date", StringType(), True),
    StructField("log_id", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("start_dt_tm", StringType(), True),
    StructField("end_dt_tm", StringType(), True),
    StructField("status", StringType(), True),
    StructField("task", StringType(), True),
    StructField("rows_cnt", StringType(), True),
    StructField("pipeline", StringType(), True)
])

# Create an empty DataFrame with the defined schema
empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

# Write the empty DataFrame to CSV
# empty_df.write.csv(filepath+filename, mode='overwrite', header=True)
empty_df.write.partitionBy('execution_date').parquet(filepath+filename, mode='overwrite')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
