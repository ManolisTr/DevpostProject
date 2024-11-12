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

# PARAMETERS CELL ********************

schema_name = 'person'
table_name = 'person'
destination_table = 'person_person'
filepath = 'abfss://Ingestion_DP_DEV_workspace@onelake.dfs.fabric.microsoft.com/DEV_Ingestion_DP_Lakehouse.Lakehouse/Files/'
filename = 'tmp_ctrl_log'
suffix = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## gather the count of all the loaded tables using the systemic tables of the source system
def source_stats(schema_name, table_name):
    source_stats = spark.sql(f'''
                        select distinct p.rows as cnt
                          from sys_object_ID o
                         inner 
                          join sys_partitions p 
                            on o.object_id = p.object_id
                         where lower(o.SCHEMA_NAME) = "{schema_name}"
                           and lower(o.TABLE_NAME) = "{table_name}"
                        ''')
    source_cnt = source_stats.collect()[0][0]
    
    return source_cnt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# parquet_df = spark.read.parquet(filepath + filename + suffix)
# parquet_df = parquet_df.withColumn("status", parquet_df["status"].cast("string"))
# parquet_df.createOrReplaceTempView("tmp_log_file")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(parquet_df)
parquet_df = spark.read.parquet(filepath + filename + suffix)
parquet_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## gather the count of all the loaded tables into a dictionary
def staging_stats (stg_table):
    ## read logs file
    df = spark.read.option("delimiter", ",").option("encoding", "utf-8").parquet(filepath+filename+suffix, header=True)
    df.createOrReplaceTempView(f"tmp_log_file")
    display(df)
    stg_stats = spark.sql(f'''select case 
                                        when status = 'Succeeded' 
                                            and task = 'Copy Data to staging table of Lakehouse'
                                            then rows_cnt
                                        else -100
                                        end as rows_cnt
                                    from tmp_log_file o
                                    where 'aw_' || lower(o.SCHEMA_NAME)|| '_' || lower(o.TABLE_NAME) = "aw_" || "{destination_table}"
                    
                    ''')
    print(stg_stats.collect()[0][0])
    stg_cnt = stg_stats.collect()[0][0]
    
    return stg_cnt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## gather the count of all the loaded tables into a dictionary
def target_stats (destination_table):
    ## read logs file
    trg_df = spark.read.format("delta")\
                        .load(f"abfss://Admin_test@onelake.dfs.fabric.microsoft.com/DEV_Lakehouse.Lakehouse/Tables/{schema_name}/{table_name}")
 
    trg_cnt = trg_df.count()
    
    return trg_cnt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_cnt = source_stats(schema_name, table_name)
stage_cnt  = staging_stats(destination_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# if suffix == '':
#     if source_cnt == target_cnt and stage_cnt == target_cnt:
#         exitVal = True 
#     else: 
#         exitVal = False
# elif suffix == '_delta':
#     if source_cnt == target_cnt:
#         exitVal = True 
#     else: 
#         exitVal = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if suffix == '':
    if source_cnt == stage_cnt:
        exitVal = True 
    else: 
        exitVal = False
elif suffix == '_delta':
    if source_cnt == stage_cnt:
        exitVal = True 
    else: 
        exitVal = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if exitVal:
    mssparkutils.notebook.exit("Succeeded")
else:
    mssparkutils.notebook.exit("Failed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
