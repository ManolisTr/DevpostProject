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
# META       "default_lakehouse_workspace_id": "63a7c4a0-7e46-4131-b5ee-0b33d69e52eb"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

table_name = 'salesorderdetail'
schema_name = 'sales'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fabric_metadata_df = spark.sql(f"SELECT * \
#                                   FROM DEV_Ingestion_DP_Lakehouse.fabric_metadata\
#                                   WHERE active = 1 ")
                                  
# #   and source_table = {table_name} and source_schema = {schema_name}")
# display(fabric_metadata_df)


# final_source_tables = []

# for row in fabric_metadata_df.collect():

#     final_source_tables.append(row[0])

# print(final_source_tables)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## create query to be executed on source system
select_stm = f"SELECT DISTINCT schemas.name as schema_name,\
                tables.name as table_name, \
                rows as cnt\
          from sys.partitions  as partitions\
          inner \
           join sys.tables     as tables\
             on partitions.object_id = tables.object_id\
          inner \
           join sys.schemas    as schemas\
             on tables.schema_id = schemas.schema_id\
          where trim(lower(schemas.name)) = '{schema_name}' and trim(lower(tables.name)) = '{table_name}'"


print(select_stm)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(select_stm)

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
