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

# CELL ********************

spark.sql(""" MERGE INTO DEV_Ingestion_DP_Lakehouse.fabric_metadata AS DWH
                USING DEV_Ingestion_DP_Lakehouse.tmp_fabric_metadata AS LH
                ON DWH.DESTINATION_TABLE = LH.DESTINATION_TABLE
                WHEN MATCHED AND DWH.VERSION <> LH.version THEN
                    UPDATE SET DWH.VERSION = LH.version   
         """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

d = spark.sql("SELECT * from DEV_Ingestion_DP_Lakehouse.fabric_metadata")

display(d)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
