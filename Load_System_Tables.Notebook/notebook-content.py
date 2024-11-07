# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "04428fdd-b859-4bbe-b9a3-9d1eff092896"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

table_name = 'COLUMNS'
schema_name = 'INFORMATION_SCHEMA'
select_stm = ''
destination_table = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema_name = schema_name.lower()
table_name = table_name.lower()

if table_name == 'object_id':
    select_stm = "select distinct s.name AS SCHEMA_NAME , t.name AS TABLE_NAME , p.object_id AS OBJECT_ID\
        from sys.tables t\
        inner join\
            sys.partitions p\
            on t.object_id  = p.object_id\
    inner join sys.schemas s\
        on s.schema_id  = t.schema_id\
        inner join information_schema.tables it\
        on it.TABLE_TYPE = 'BASE TABLE'\
        AND IT.TABLE_CATALOG = 'AdventureWorks'"
else: 
    select_stm = f'SELECT * FROM {schema_name}.{table_name}' 

destination_table = schema_name + "_" + table_name

mssparkutils.notebook.exit({'sql_stmt': select_stm, 'tbl': destination_table})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
