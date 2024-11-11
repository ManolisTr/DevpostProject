# Microsoft Fabric Real-World Solution: AI-Driven Analytics with Azure SQL DB

## Overview

This project showcases a real-world solution leveraging **Microsoft Fabric**, **Azure SQL Database**, and AI capabilities. Using data from the **AdventureWorks** database, it integrates ingestion, transformation, and AI-driven analytics to deliver actionable insights and efficient workflows.

[AdventureWorks Database](https://github.com/Microsoft/sql-server-samples/tree/master/samples/databases/adventure-works) is used as the on-premise database source, providing a rich dataset for enterprise-level demonstrations.

---

## Key Features

1. **Data Ingestion**:
   - Data is extracted from the **AdventureWorks** database.
   - Supports **Change Data Capture (CDC)** via Eventstream connectors.
   - Pipelines built with **Dataflows (Gen2)** and **Data Factory Pipelines**.

2. **Data Transformation**:
   - Utilizes **Notebooks** for transforming and processing delta datasets.
   - Merges metadata from **Azure SQL Database** and **Lakehouse**.

3. **AI-Driven Insights**:
   - AI models applied for trend analysis and predictions.
   - Includes reconciliation and validation for data consistency.

4. **Orchestration**:
   - Fully automated pipelines from ingestion to analytics.

---

## Repository Structure

- **Notebooks**:
  - `Aggregation_of_Log_data`: Summarizes log data for monitoring.
  - `Delta_Join_Parquet`: Combines delta datasets using advanced joins.
  - `Reconciliation_Check`: Ensures data accuracy across sources.

- **Pipelines**:
  - `Delta_Load_LH`: Incremental delta load for Lakehouse integration.
  - `Orchestration`: Automates workflow execution and monitoring.
  - `Load_Counts_Daily`: Daily data ingestion and transformation pipeline.

- **Supporting Files**:
  - `Log.Notebook`: Tracks all processes for debugging and auditing.

---

## Technologies Used

- **Microsoft Fabric**: Seamlessly integrates Lakehouse and SQL services.
- **Azure SQL Database**: Centralized data storage for query and analysis.
- **AdventureWorks**: Rich on-premise dataset for demonstration.
- **AI and ML Models**: Enable advanced predictions and insights.
- **Data Factory Pipelines**: Automate data ingestion and processing.
- **Notebooks**: Enable interactive and advanced transformation workflows.

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

   
