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

try: 
    spark.sql("""select * from dev_ingestion_dp_lakehouse.FABRIC_METADATA""")
    print('Try')
except:
    print('except')
    spark.sql("""CREATE TABLE dev_ingestion_dp_lakehouse.FABRIC_METADATA
            (
                SOURCE_SYSTEM varchar(100)  NOT NULL,
                LH_NAME varchar(100)  NOT NULL,
                SOURCE_SCHEMA varchar(100)  NOT NULL,
                SOURCE_TABLE varchar(100)  NOT NULL,
                DESTINATION_TABLE varchar(100)  NOT NULL,
                TABLE_TYPE varchar(100)  NOT NULL,
                PRIMARY_KEY varchar(100)  NOT NULL,
                LOADING_METHOD varchar(100)  NOT NULL,
                ACTIVE int  NOT NULL,
                FREQUENCY varchar(2)  NOT NULL,
                VERSION timestamp  NOT NULL
            )""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql("""WITH ALL_TABLES AS (
select distinct 
          t.TABLE_CATALOG           						 AS LH_NAME
        , lower(t.TABLE_SCHEMA) 							 as SOURCE_SCHEMA
        , lower(t.TABLE_NAME) 								 as SOURCE_TABLE
        , concat(lower(t.TABLE_SCHEMA) ,'_', lower(t.TABLE_NAME))  AS DESTINATION_TABLE
        , t.TABLE_TYPE  									 AS TABLE_TYPE
      from dev_ingestion_dp_lakehouse.information_schema_tables as t 
    inner
      join dev_ingestion_dp_lakehouse.information_schema_columns as c 
        on t.TABLE_SCHEMA  = c.TABLE_SCHEMA 
      and t.TABLE_NAME    = c.TABLE_NAME 
      and t.TABLE_CATALOG = 'AdventureWorks'
      and t.TABLE_TYPE    = 'BASE TABLE'
)
, pk AS (
         SELECT ku.TABLE_CATALOG
              , ku.TABLE_SCHEMA
              , ku.TABLE_NAME
              , ku.COLUMN_NAME
           FROM dev_ingestion_dp_lakehouse.information_schema_table_constraints AS tc
          INNER 
           JOIN dev_ingestion_dp_lakehouse.information_schema_key_column_usage AS ku
             ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
        )
, all_pks as (
 SELECT c.TABLE_SCHEMA
       , c.TABLE_NAME
       , c.COLUMN_NAME
       , c.DATA_TYPE
       , c.COLUMN_DEFAULT
       , c.CHARACTER_MAXIMUM_LENGTH
       , c.NUMERIC_PRECISION
       , c.IS_NULLABLE       
       , CASE 
            WHEN pk.COLUMN_NAME IS NOT NULL 
              THEN 'PRIMARY KEY' 
            ELSE '' 
         END                 AS KeyType
    FROM dev_ingestion_dp_lakehouse.information_schema_columns c 
    inner 
    JOIN   pk 
      ON  c.TABLE_CATALOG = pk.TABLE_CATALOG
     AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
     AND c.TABLE_NAME = pk.TABLE_NAME
     AND c.COLUMN_NAME = pk.COLUMN_NAME 
)
, CONCATENATED_PK AS (
select TABLE_SCHEMA
   , TABLE_NAME
   , array_join(collect_set(COLUMN_NAME), ',') AS PRIMARY_KEY
  from all_pks 
  group by TABLE_SCHEMA, TABLE_NAME 
  )
INSERT INTO dev_ingestion_dp_lakehouse.FABRIC_METADATA
(
      SOURCE_SYSTEM,
      LH_NAME ,
      SOURCE_SCHEMA ,
      SOURCE_TABLE ,
      DESTINATION_TABLE ,
      TABLE_TYPE ,
      PRIMARY_KEY,
      LOADING_METHOD,  
      ACTIVE,
      FREQUENCY ,
      VERSION
)
SELECT  'Adventure Works', 
		ALL_TABLES.LH_NAME,
		ALL_TABLES.SOURCE_SCHEMA,
		ALL_TABLES.SOURCE_TABLE,
		ALL_TABLES.DESTINATION_TABLE,
		ALL_TABLES.TABLE_TYPE,
		CONCATENATED_PK.PRIMARY_KEY,
    'Full' as LOADING_METHOD,
     1 as ACTIVE,
     '1D' AS FREQUENCY,
     CURRENT_TIMESTAMP()
  FROM ALL_TABLES 
  INNER 
  JOIN CONCATENATED_PK 
   ON upper(trim(ALL_TABLES.SOURCE_SCHEMA)) = upper(trim(CONCATENATED_PK.TABLE_SCHEMA))
   AND upper(trim(ALL_TABLES.SOURCE_TABLE)) = upper(trim(CONCATENATED_PK.TABLE_NAME))
  LEFT
  JOIN dev_ingestion_dp_lakehouse.FABRIC_METADATA AS CURRENT_TABLES
    ON ALL_TABLES.LH_NAME = CURRENT_TABLES.LH_NAME
   AND ALL_TABLES.DESTINATION_TABLE = CURRENT_TABLES.DESTINATION_TABLE
 WHERE  CURRENT_TABLES.DESTINATION_TABLE IS NULL;""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from dev_ingestion_dp_lakehouse.FABRIC_METADATA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
