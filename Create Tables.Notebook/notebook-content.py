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

# ### Initialize variables

# CELL ********************

from pyspark.sql.functions import *
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

LH = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The list of reserved words in T-SQL

TSQL_reserved_words = \
        ["ADD","EXTERNAL","PROCEDURE","ALL","FETCH","PUBLIC","ALTER","FILE","RAISERROR","AND","FILLFACTOR"
        ,"READ","ANY","FOR","READTEXT","AS","FOREIGN","RECONFIGURE","ASC","FREETEXT","REFERENCES","AUTHORIZATION"
        ,"FREETEXTTABLE","REPLICATION","BACKUP","FROM","RESTORE","BEGIN"
        ,"FULL","RESTRICT","BETWEEN","FUNCTION","RETURN","BREAK","GOTO","REVERT"
        ,"BROWSE","GRANT","REVOKE","BULK","GROUP","RIGHT","BY","HAVING","ROLLBACK","CASCADE","HOLDLOCK","ROWCOUNT"
        ,"CASE","IDENTITY","ROWGUIDCOL","CHECK","IDENTITY_INSERT","RULE","CHECKPOINT","IDENTITYCOL","SAVE","CLOSE"
        ,"IF","SCHEMA","CLUSTERED","IN","SECURITYAUDIT","COALESCE"
        ,"INDEX","SELECT","COLLATE","INNER","SEMANTICKEYPHRASETABLE","COLUMN","INSERT","SEMANTICSIMILARITYDETAILSTABLE"
        ,"COMMIT","INTERSECT","SEMANTICSIMILARITYTABLE","COMPUTE","INTO"
        ,"SESSION_USER","CONSTRAINT","IS","SET","CONTAINS","JOIN","SETUSER","CONTAINSTABLE","KEY","SHUTDOWN","CONTINUE","KILL"
        ,"SOME","CONVERT","LEFT","STATISTICS","CREATE","LIKE","SYSTEM_USER"
        ,"CROSS","LINENO","TABLE","CURRENT","LOAD","TABLESAMPLE","CURRENT_DATE","MERGE","TEXTSIZE"
        ,"CURRENT_TIME","NATIONAL","THEN","CURRENT_TIMESTAMP","NOCHECK","TO"
        ,"CURRENT_USER","NONCLUSTERED","TOP","CURSOR","NOT","TRAN","DATABASE","NULL","TRANSACTION"
        ,"DBCC","NULLIF","TRIGGER","DEALLOCATE","OF","TRUNCATE","DECLARE","OFF"
        ,"TRY_CONVERT","DEFAULT","OFFSETS","TSEQUAL","DELETE","ON","UNION","DENY","OPEN","UNIQUE"
        ,"DESC","OPENDATASOURCE","UNPIVOT","DISK","OPENQUERY","UPDATE","DISTINCT"
        ,"OPENROWSET","UPDATETEXT","DISTRIBUTED","OPENXML","USE","DOUBLE","OPTION","USER"
        ,"DROP","OR","VALUES","DUMP","ORDER","VARYING","ELSE","OUTER","VIEW","END"
        ,"OVER","WAITFOR","ERRLVL","PERCENT","WHEN","ESCAPE","PIVOT","WHERE","EXCEPT","PLAN","WHILE","EXEC","PRECISION","WITH","EXECUTE"
        ,"PRIMARY","WITHIN GROUP","EXISTS","PRINT","WRITETEXT","EXIT","PROC"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Define functions

# MARKDOWN ********************

# #### system_information 

# CELL ********************

def system_information ():

  table_columns_dict = {}

  df = spark.sql(f"""
  select distinct lower(t.TABLE_SCHEMA) as table_schema
        , lower(t.TABLE_NAME) as table_name
        , c.COLUMN_NAME
        , c.COLUMN_DEFAULT
        , c.IS_NULLABLE
        , c.DATA_TYPE
        , c.ORDINAL_POSITION
        , c.CHARACTER_MAXIMUM_LENGTH
        , c.NUMERIC_PRECISION
        , c.NUMERIC_SCALE 
      from {LH}.information_schema_tables as t 
    inner
      join {LH}.information_schema_columns as c 
        on t.TABLE_SCHEMA  = c.TABLE_SCHEMA 
      and t.TABLE_NAME    = c.TABLE_NAME 
      and t.TABLE_CATALOG = 'AdventureWorks'
      and t.TABLE_TYPE    = 'BASE TABLE'
      where  t.TABLE_SCHEMA <> 'dbo' 
        """)

  distinct_tables_df = spark.sql(f"""
    select distinct lower(t.TABLE_SCHEMA), lower(t.TABLE_NAME)
      from {LH}.information_schema_tables as t 
    inner
      join {LH}.information_schema_columns as c 
        on t.TABLE_SCHEMA = c.TABLE_SCHEMA 
      and t.TABLE_NAME = c.TABLE_NAME 
      and t.TABLE_CATALOG = 'AdventureWorks'
      and t.TABLE_TYPE = 'BASE TABLE'
      where  t.TABLE_SCHEMA <> 'dbo' """)

  for i in distinct_tables_df.collect():
      list_of_columns = df.select("column_name", "data_type"\
                                , "is_nullable", "column_default"\
                                , "ordinal_position", "CHARACTER_MAXIMUM_LENGTH"\
                                , "numeric_precision", "numeric_scale"\
                                )\
                          .filter((df["table_name"]==i[1]) & (df["table_schema"]==i[0]))\
                          .collect()

      table_columns_dict[i[0] + "_" + i[1]] = [{i["column_name"]:[i["ordinal_position"]
                                              , i["data_type"], i["CHARACTER_MAXIMUM_LENGTH"]
                                              , i["numeric_precision"], i["numeric_scale"]
                                              , i["column_default"], i["is_nullable"]]} for i in list_of_columns]
  return table_columns_dict


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### initialize_variables

# CELL ********************

def initialize_variables(col_LoV):
    order = col_LoV[0]                  # the order that the columns are placed in the source table
    data_type = col_LoV[1]              # the data type of the column
    max_length = col_LoV[2]             # the maximun length of the column
    max_precision = col_LoV[3]          # the precision of the column (used in case of decimal datatypes)
    max_scale = col_LoV[4]              # the scale of the column (used in case of decimal datatypes)
    default_value = col_LoV[5]          # the default value given to the column
    nullable = col_LoV[6]               # indicates whether the column is nullable
    # is_pk = col_LoV[7]                # indicates whether the column is primary key
    
    return (order, data_type, max_length, max_precision, max_scale, default_value, nullable)#, is_pk)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### convert_to_t_sql

# CELL ********************

def convert_to_t_sql (intial_dict, TSQL_reserved_words):
    sql_col = []
    sql_col_dict = {}

    select_stm = []
    select_stm_dict = {}

    cols = []

    for key,value in table_columns_dict.items():
        for val in value:
            for col, v in val.items():
                (order, data_type, max_length, max_precision, max_scale, default_value, nullable) = initialize_variables(v)
                
                orig_col = col

                ## use keyword 'default' to set the default value of a column, as given in the source table
                if default_value != None:
                    default_value = 'default ' + default_value
                
                ## Format datatypes correctly 
                # For character fields, nchar and nvarchar datatypes are converted to 
                # char and varchar.
                # The max length of the field is also calculated 

                if data_type in ('nchar', 'nvarchar', 'varchar', 'char') :
                    if max_length >=0:
                        data_type = data_type.replace('n', '') + "(" + str(max_length) + ")"
                    else:
                        continue;
                
                # Datetime data types are converted to timestamp
                # The default value of the field is the current timestamp
                elif data_type == 'datetime':
                    data_type = data_type.replace('datetime', 'timestamp')
                    if default_value != None and 'getdate' in default_value:
                        default_value = default_value.replace('(getdate())', 'current_timestamp')
                
                # Uniqueidentifier data types are converted to varchar(256)
                # The default value is ''
                elif data_type == 'uniqueidentifier':
                    data_type = 'varchar(256)'
                    default_value = ''
                    nullable = 'not null'

                # Bit data types are converted to int
                elif 'bit' in data_type:
                    data_type = 'int'
                    if default_value != None:
                        default_value =  default_value.replace('(', '').replace(')', '')

                elif 'int' in data_type:
                    data_type = 'int'
                    if default_value != None:
                        default_value = default_value.replace('(', '').replace(')', '')
                
                # hierarchyid, xml, geography and varbinary data types are skipped
                elif data_type in ('hierarchyid', 'xml', 'geography', 'varbinary'):
                    continue;

                # Money data types are converted to decimal(15,4)
                elif 'money' in data_type :
                    data_type = 'decimal(15,4)'

                elif data_type == 'decimal':
                    data_type = data_type + "(" + str(max_precision) + "," + str(max_scale) + ")"
                
                # Time data types are converted to time(7)
                elif data_type == 'time':
                    data_type = 'varchar(100)' 

                if default_value != None and 'getdate' in default_value:
                    default_value = default_value.replace('(getdate())', 'current_date')

                elif default_value == None and nullable == 'NO': 
                    default_value = 'not null'

                elif default_value == None and nullable == 'YES':
                    default_value = 'default null'

                
                ## In case the column name is a reserved word, then the suffix "_new" is added to the column
                 # Brackets are used when selecting it from the source system
                if col.upper() in TSQL_reserved_words:
                    col = col + "_new"
                    orig_col = '[' + orig_col + '] as ' + col

                ## In case the column name contains spaces, they are replaced with '_'
                 # Brackets are used when selecting it from the source system    
                if ' ' in col:
                    col = col.replace(' ', '_')
                    orig_col = '[' + orig_col + '] as ' + col

                sql_col.append({col : {'order': order, 'Data_types': data_type + " " +  " " + default_value}})
                select_stm.append({orig_col : {'order': order, 'Data_types': data_type + " " +  " " + default_value}})
               
                sql_col_dict[key] = sql_col
                select_stm_dict[key] = select_stm

        sql_col_dict[key]  = sorted(sql_col_dict[key], key=lambda x: next(iter(x.values()))['order'])
        select_stm_dict[key] =  sorted(select_stm_dict[key], key=lambda x: next(iter(x.values()))['order'])
        
        sql_col = []
        cols = []
        select_stm = []

    return sql_col_dict, select_stm_dict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### create_table

# CELL ********************

## Creates the tables that do not already exist in the target environment
 # The tables are created with delta format
def create_table (sorted_dict):

    ddls = {}

    for key,value in sorted_dict.items():
        cnt = 0

        if spark.catalog.tableExists(key):
            continue;
        else:
            str_init = 'create'
        str_q = str_init + f" table {LH}.stg_{key} ("

        for dictionary in value:
            for k,v in dictionary.items():
                str_q = str_q + k + " "+ v["Data_types"]
                if cnt < len(value) - 1:
                    str_q = str_q + ","

                cnt = cnt + 1

        str_q = str_q  +  " ) USING DELTA TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')"
        ddls[key] = str_q
    return ddls



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get_columns

# CELL ********************

def get_columns (sorted_dict):
    columns = {}
    for key,value in sorted_dict.items():
        cnt = 0
        
        str_q =  "select "
        for dictionary in value:
            for k,v in dictionary.items():
                if 'time(7)' in v['Data_types']:
                   k = 'cast( ' + k + ' as varchar(100)) as ' + k    
                str_q = str_q + k 
                if cnt < len(value) - 1:
                    str_q = str_q + ","

                cnt = cnt + 1
        str_q = str_q  +  f" from {key} "
        columns[key] = str_q

    return columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### save_select_stms

# CELL ********************

def save_select_stms(data):

    # creates a json file that contains the select statements to be executed on the source system 
    # in order for the data to be loaded

    jsonData = json.dumps(data)
    jsonDataList = []
    jsonDataList.append(jsonData)
    jsonRDD = sc.parallelize(jsonDataList)

    # json is converted to dataframe
    df = spark.read.json(jsonRDD)
    
    ## Dataframe is saved as a table in the Lakehouse and is used in the rest of the notebooks
    df.write.mode("overwrite").saveAsTable("ddls")

    # ## Dataframe is saved as a csv file and is used in the rest of the notebooks
    # # Define the file path
    # file_path = 'abfss://AdventureWorks@onelake.dfs.fabric.microsoft.com/AdventureWorks.Lakehouse/Files/ddls.csv'

    # # Read the file into a Spark DataFrame
    # df.write.format('com.databricks.spark.csv').mode('overwrite').options(header='true').save(file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### drop_all_tables

# CELL ********************

def drop_all_tables(table_columns_dict):
    for key,value in table_columns_dict.items():
           try:
            spark.sql(f"""drop table  {LH}.stg_{key}""")
           except:
            print(f"""Table  {LH}.stg_{key} not found""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### loading_stats

# CELL ********************

def loading_stats(table_columns_dict):
    tables = list(table_columns_dict.keys())
    not_loaded = []
    loaded = []

    for t in tables:
        df = spark.sql(f"""select * from  {LH}.stg_{t}""")
        cnt = df.count()
        
        if cnt == 0:
            not_loaded.append(t)
        else:
            loaded.append(t)
    print("Loaded Tables:", loaded)
    print("Not Loaded Tables", not_loaded)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Execute functions

# CELL ********************

table_columns_dict = system_information()
sorted_dict, select_stm_dict = convert_to_t_sql(table_columns_dict, TSQL_reserved_words)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

drop_all_tables(table_columns_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ddls = create_table(sorted_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = get_columns(select_stm_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_select_stms(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for key,value in ddls.items():
    ## The create statements are executed so that the tables are created in the target system
    # An exception occurs if the sql command has failed executing
    try: spark.sql(value)    
    except: 
        print("ISSUE WITH \n", value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
