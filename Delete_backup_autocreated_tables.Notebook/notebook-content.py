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

# MARKDOWN ********************

# [Issue Explained](https://community.fabric.microsoft.com/t5/Data-Engineering/System-Created-Backup-Tables/m-p/4045371)
# 
# The above issue has occurred during the past days. A backup table is created for large tables transferred from an sql server to a fabric lakehouse. The following code deletes all the files 

# CELL ********************

from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls("abfss://Ingestion_DP_DEV_workspace@onelake.dfs.fabric.microsoft.com/DEV_Ingestion_DP_Lakehouse.Lakehouse/Tables/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for file in mssparkutils.fs.ls("abfss://Ingestion_DP_DEV_workspace@onelake.dfs.fabric.microsoft.com/DEV_Ingestion_DP_Lakehouse.Lakehouse/Tables/"):
    if 'backup' in file.name or 'stg' in file.name:
        print(file.name)
        mssparkutils.fs.rm(file.path, True) # Set the last parameter as True to remove all files and directories recursively


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
