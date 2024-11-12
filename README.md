# Microsoft Fabric Real-World Solution: AI-Driven Analytics with Azure SQL DB

## Overview

This project showcases a real-world solution leveraging **Microsoft Fabric**, **SQL Database**, and AI capabilities. Using data from the **AdventureWorks** database, it integrates ingestion, transformation, and AI-driven analytics to deliver actionable insights and efficient workflows.

[AdventureWorks Database](https://github.com/Microsoft/sql-server-samples/tree/master/samples/databases/adventure-works) is used as the on-premise database source, providing a rich dataset for enterprise-level demonstrations.

---

## Key Features

1. **Data Ingestion**:
   - Data is extracted from the **AdventureWorks** database.
   - Supports **Full and Delta Integration**.
   - Pipelines built with **Data Factory Pipelines and Notebooks**.

2. **Data Transformation**:
   - Utilizes **Notebooks** for processing delta datasets.
   - Ensures metadata consistency across **SQL Database** and **Lakehouse**.

3. **AI-Driven Insights**:
   - Includes reconciliation and validation for data consistency.

4. **Orchestration**:
   - Automated pipelines manage the entire data workflow, ensuring seamless progression from ingestion to preparation.
   - The system structures and organizes data effectively, making it ready for downstream analytics and decision-making processes.

5. **Logging and Monitoring**:
   - Detailed logging tracks every step of the ingestion process for transparency and auditing.
   - Monitoring systems provide real-time insights into pipeline performance, ensuring data flows smoothly and potential issues are detected early.


---

## Repository Structure

- **Notebooks**:
  - `Aggregation of Log data.Notebook`: Summarizes log data for monitoring and debugging.
  - `Create Tables.Notebook`: Automates the creation of required tables in Lakehouse.
  - `Create query for source system.Notebook`: Generates queries to extract data from the source system.
  - `Delta_Join_Parquet.Notebook`: Identifies and processes daily delta updates for ingestion, using the fields that individualize the records of each table.
  - `Get Columns.Notebook`: Retrieves metadata for columns across source system.
  - `Initialize log ctrl file.Notebook`: Sets up and initializes logging controls for ingestion.
  - `Load_System_Tables.Notebook`: Automates the loading of reference and system tables.
  - `Load_fabric_metadata.Notebook`: Creates the metadata control table in order to parameterize the pipelines.
  - `Reconciliation Check.Notebook`: Ensures consistency and validation of ingested data.
  - `Reconciliation Checks Total.Notebook`: Aggregation of reconciliation checks regarding all loaded tables.
  - `Truncate Table.Notebook`: Used for re-run purposes of the source system daily counts.
  - `Update_fabric_metadata_table.Notebook`: Updates metadata table after ingestion process is completed.
  - `Log.Notebook`: Monitors processes for debugging and auditing.

- **Pipelines**:
  - `Load_System_Tables`: Manages the ingestion of system and reference tables.
  - `Full_Load_LH`: Executes a full load data for scenarios requiring complete dataset ingestion, bypassing delta constraints.
  - `Delta_Load_LH`: Handles incremental delta loading for the Lakehouse environment.
  - `Load_Counts_Daily`: Automates daily ingestion and metadata tracking processes.
  - `Orchestration`: Oversees and automates workflow execution.
    

This repository provides the foundation for building an integrated, scalable data platform using Microsoft Fabric and on-premise SQL Server Database.

## Technologies Used

- **Microsoft Fabric**: Seamlessly integrates Lakehouse and SQL services.
- **SQL Server Database**: Centralized data storage for query and analysis.
- **AdventureWorks**: Rich on-premise dataset for demonstration.
- **AI**: Enable advanced insights.
- **Data Factory Pipelines**: Automate data ingestion and processing.
- **Notebooks**: Enable interactive and advanced transformation workflows.

---
### Workflow of Operations

The workflow for this project follows a structured sequence to ensure efficient data handling, transformation, and analysis:

0. **DDL Generation from On-Premise Database**:
   - Extracts the DDL definitions of system tables from the **AdventureWorks** on-premise database.
   - Automatically generates equivalent DDL scripts for the Microsoft Fabric environment.
   - Notebooks are used to analyze and modify the data types as needed to ensure compatibility with Fabric's data processing capabilities.

1. **Data Ingestion**:
   - The process begins with extracting data from the **AdventureWorks** database.
   - The data is loaded into staging areas in the Lakehouse.

2. **System Table Loading**:
   - The `Load_system_tables.DataPipeline` initializes and populates metadata and reference tables, ensuring the foundational system data is ready for subsequent operations.

3. **Data Delta Processing**:
   - Pipelines and notebooks identify and extract the daily differences (delta) in data from the previous day.
   - This approach ensures efficient handling of only the changed data, minimizing processing overhead and keeping the system up-to-date.
   - The `Reconciliation Check.Notebook` validates the accuracy of the deltas, ensuring consistency between the source and target systems.

4. **Orchestration and Logging**:
   - The `Orchestration.DataPipeline` automates the execution of all workflows.
   - Logs are recorded in `Log.Notebook` to track processes and identify any issues.

This end-to-end workflow enables robust and scalable data operations, delivering meaningful insights from raw data efficiently.

---

## Getting Started

### Prerequisites

- Azure Subscription with Microsoft Fabric enabled.
- AdventureWorks Database (download from [here](https://github.com/Microsoft/sql-server-samples/tree/master/samples/databases/adventure-works)).
- Azure SQL Database or SQL Server for cloud storage.

### Steps to Deploy the Project

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/ManolisTr/DevpostProject.git
   ```

2. **Set Up Resources**
   - **AdventureWorks Database**:
     - Download the AdventureWorks database from [AdventureWorks GitHub](https://github.com/Microsoft/sql-server-samples/tree/master/samples/databases/adventure-works).
     - Configure it as the on-premise source.
   - **Azure SQL Database**:
     - Set up an Azure SQL Database instance for cloud data integration.
   - **Pipelines and Connectors**:
     - Deploy the Data Factory Pipelines to handle data ingestion and transformation.
     - Use Eventstream connectors for Change Data Capture (CDC) support.

3. **Run Pipelines**
   - Trigger the orchestration pipeline to start the data ingestion process.
   - Monitor the transformation and reconciliation notebooks to validate data accuracy.

   
