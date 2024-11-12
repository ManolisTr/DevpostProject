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

import datetime
import time
import multiprocessing
import pandas as pd
from io import StringIO
from pyspark.sql.functions import when, col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_date = datetime.date.today().strftime("%Y-%m-%d")
current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

table_name = ''
schema_name = ''
start_dt_tm = current_time
end_dt_tm = '1900-01-01 00:00:01'
task = ''
pipeline = ''
status = '-'
log_id = ''
header = 'execution_date,log_id,schema_name,table_name,start_dt_tm,end_dt_tm,status,task,rows_cnt,pipeline'
filename = ''
filepath = ''
rows_cnt = '-100'
reconciliation_status = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if rows_cnt == None:
    rows_cnt = '-100'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if task == 'Copy Data to staging table of Lakehouse' or task == 'Merge to lakehouse' or task == 'Transfer to DWH':
    end_dt_tm = current_time
elif task == 'Transfer to DWH' and not reconciliation_status:
    status = 'Failed due to reconciliation checks'
elif task == 'Merge to lakehouse' and not reconciliation_status:
    status = 'Failed due to reconciliation checks'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_data = header + '\n' + current_date + ',' + log_id + ',' + schema_name + ',' + table_name + ',' + start_dt_tm + ',' + end_dt_tm + ',' + status + ',' + task + ',' + str(rows_cnt) + ',' + pipeline

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def worker(data, queue):
    queue.put(data)

def writer(queue, filename_):
    while True:
        data = queue.get()
        if data is None:
            break
            
        # Convert the CSV string into a pandas DataFrame
        pandas_df = pd.read_csv(StringIO(data))

        # Convert pandas DataFrame to PySpark DataFrame
        spark_df = spark.createDataFrame(pandas_df)
        
        spark_df.write.partitionBy('execution_date').mode('append').parquet(filename_)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if __name__ == '__main__':
    # List of data to be written
    data_list = [final_data]

    # Create a queue
    try:
        queue = multiprocessing.Queue()

        file_ =  filepath + filename
        # Create a writer process
        try:
            writer_process = multiprocessing.Process(target=writer, args=(queue, file_))
            writer_process.start()

            # Create worker processes
            processes = []
            for data in data_list:
                p = multiprocessing.Process(target=worker, args=(data, queue))
                processes.append(p)
                p.start()

            # Wait for worker processes to finish
            for p in processes:
                p.join()

            # Signal the writer process to finish
            queue.put(None)
            writer_process.join()
        except:
            print('An error occured when creating the writer process')
    except:
        print('An error occured when creating the queue')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
