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
# META     },
# META     "warehouse": {}
# META   }
# META }

# CELL ********************

import datetime
import time
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

filename = 'tmp_ctrl_log'
filepath = 'abfss://Ingestion_DP_DEV_workspace@onelake.dfs.fabric.microsoft.com/DEV_Ingestion_DP_Lakehouse.Lakehouse/Files/'
suffix = '_delta'
LH = 'DEV_Ingestion_DP_Lakehouse'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("delimiter", ",").option("encoding", "utf-8").parquet(filepath+filename+suffix, header=True)
df.createOrReplaceTempView(f"temp_aggregated_metadata_full_load{suffix}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

aggregated_metadata_full_load = spark.sql(f"""
    with start_time as (
    select log_id, min(start_dt_tm) as start_dt_tm
    from temp_aggregated_metadata_full_load{suffix}
    group by log_id 
)
, end_time as (
    select log_id, max(end_dt_tm) as end_dt_tm
    from temp_aggregated_metadata_full_load{suffix}
    group by log_id   
), final as (
select  distinct m.execution_date
      , m.log_id
      , m.schema_name
      , m.table_name
      , s.start_dt_tm
      , e.end_dt_tm
      , TIMESTAMPDIFF(SECOND, CAST(s.start_dt_tm AS timestamp), CAST(e.end_dt_tm AS timestamp)) AS duration_sec
      , case 
            when m.status = 'Failed'
              then 'Load failed'
            else 'Load succeeded'  
        end as status
  from temp_aggregated_metadata_full_load{suffix} m 
  left 
  join start_time s
    on m.log_id = s.log_id 
  left 
  join end_time e 
    on e.log_id = m.log_id
 )
 , cnt_status as (
      SELECT log_id, count(*) as cnt 
        FROM final
       GROUP 
          BY log_id 
      having count(*) > 1
 ) select distinct  final.execution_date
                , final.log_id
                , final.schema_name
                , final.table_name
                , final.start_dt_tm
                , final.end_dt_tm
                , final.duration_sec
                , case 
                    when 
                       ifnull(cnt_status.cnt, 1) > 1
                     then  'Load failed'
                    else final.status 
                  end as status
   from final 
   left 
   join cnt_status
     on final.log_id = cnt_status.log_id
  order
     by log_id

""")
aggregated_metadata_full_load.write.mode("overwrite").format("delta").saveAsTable(f"aggregated_metadata_full_load{suffix}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

full_load_run_stats = spark.sql(f"""
      with start_end_time as (
    select  min(start_dt_tm) as start_dt_tm
          , max(end_dt_tm) as end_dt_tm
      from temp_aggregated_metadata_full_load{suffix}
    )
    select start_dt_tm
         , end_dt_tm
         , TIMESTAMPDIFF(SECOND, CAST(start_dt_tm AS timestamp), CAST(end_dt_tm AS timestamp)) AS duration_sec
         , TIMESTAMPDIFF(MINUTE, CAST(start_dt_tm AS timestamp), CAST(end_dt_tm AS timestamp)) AS duration_minutes
         , TIMESTAMPDIFF(HOUR, CAST(start_dt_tm AS timestamp), CAST(end_dt_tm AS timestamp)) AS duration_hours
      from start_end_time 
""")
full_load_run_stats.write.mode("overwrite").format("delta").saveAsTable(f"full_load_run_stats{suffix}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric_metadata_df = spark.sql(f"SELECT * FROM {LH}.FABRIC_METADATA")

joined_df = aggregated_metadata_full_load.join(
    fabric_metadata_df,
    fabric_metadata_df["DESTINATION_TABLE"] == concat(
        aggregated_metadata_full_load["schema_name"], 
        lit("_"), 
        aggregated_metadata_full_load["table_name"]
    ),
    "inner"
)

# Update the 'status' and 'version' columns based on the condition
updated_df = joined_df.withColumn(
    "version",
    when(
        col("status") == "Load succeeded", 
        col("end_dt_tm").cast(TimestampType())  # Replace "new_version" with the value you want to set
    ).otherwise(
        col("version")
    )
)


updated_df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true")\
  .saveAsTable(f"tmp_fabric_metadata{suffix}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
