# Youtube data-pipeline using aws-s3-lambda -glue-athena-stepfunction
A cloud-native ETL pipeline that ingests YouTube trending video data across 10 regions, transforms it through a medallion architecture (Bronze > Silver > Gold), enforces data quality gates, and produces analytics-ready aggregations — all orchestrated by AWS Step Functions.
<img width="2784" height="1536" alt="YouTube Trending Data Pipeline" src="https://github.com/user-attachments/assets/173dcea9-4d2b-430b-9f1e-300855332da1" />


## Overview
This pipeline automates the end-to-end process of collecting, cleaning, and analyzing YouTube trending video data. It replaces manual Kaggle dataset downloads with live YouTube Data API v3 integration and produces three sets of business analytics tables:

Trending Analytics — daily trending metrics per region (total videos, views, engagement rates)
Channel Analytics — channel-level performance and ranking across regions
Category Analytics — category-level breakdowns with view share percentages
The pipeline supports 10 regions and runs on a configurable schedule via AWS EventBridge.    


## Architecture
The pipeline follows the Medallion Architecture pattern with three data layers:

<img width="1536" height="1024" alt="ChatGPT Image Jun 17, 2026, 01_31_39 PM" src="https://github.com/user-attachments/assets/fd1d5143-b673-4aed-a512-f02784bee29c" />
Orchestration
is handled by AWS Step Functions, which coordinates the full pipeline with retry logic, parallel execution, and failure notifications.

## Tech Stack

|Component        | Technology       |
|-----------------|------------------|
|Compute          |AWS lambda,AWS Glue(Pyspark)|
|Storage          |Amazon S3 (Parquet, Snappy)|
|Orchestration    |AWS Step Functions |
|Scheduling       |Amazon EventBridge |
|Metadata         |AWS Glue Data Catalog|
|Query Engine     |Amazon Athena|
|Alerting         |Amazon SNS |
|Security         |AWS IAM|
|Languages        |Python 3, PySpark, SQL|


## 📁 Project Structure

```text
youtube-data-pipeline-2026/
│
├── lambdas/
│   ├── youtube_api_ingestion/                 # Ingestion Lambda
│   │   └── lambda_function.py                 # Extracts trending videos & category metadata from YouTube API
│   │
│   └── json_to_parquet/                       # Transformation Lambda
│       └── lambda_function.py                 # Converts category mappings from JSON to Parquet
│
├── glue_jobs/
│   ├── bronze_to_silver_statistics.py         # Bronze → Silver ETL (Cleaning, standardization)
│   └── silver_to_gold_analytics.py            # Silver → Gold ETL (Aggregations & analytics)
│
├── data_quality/
│   └── dq_lambda.py                           # Schema, null, duplicate & quality validations
│
├── step_functions/
│   └── pipeline_orchestration.json            # Workflow orchestration definition
│
├── scripts/
│   ├── aws_copy.sh                            # Historical data upload automation
│   └── information.md                         # AWS resource inventory and configuration
│
├── data/
│   ├── {region}videos.csv                     # Historical YouTube trending datasets
│   └── {region}_category_id.json              # Category lookup reference data
│
└── YouTube Trending Data Pipeline.png         # End-to-end AWS architecture diagram
```

### Component Description

| Component | Purpose |
|------------|----------|
| YouTube API Ingestion Lambda | Extracts trending videos and category metadata from YouTube Data API |
| JSON to Parquet Lambda | Converts category mapping files from JSON to Parquet |
| Bronze to Silver Glue Job | Cleans, validates, and standardizes raw video statistics |
| Silver to Gold Glue Job | Builds business-ready analytics datasets and aggregations |
| Data Quality Lambda | Performs schema, null, and duplicate validation checks |
| Step Functions | Orchestrates end-to-end pipeline execution |
| EventBridge | Schedules automated pipeline execution |
| Amazon S3 | Stores Bronze, Silver, and Gold data layers |
| AWS Glue Data Catalog | Maintains metadata for analytics datasets |
| Amazon Athena | Enables serverless SQL analytics |
| Amazon SNS | Sends pipeline success and failure notifications |

## 🏗️ Architecture Overview

The pipeline follows a Medallion Architecture (Bronze → Silver → Gold) pattern:

1. EventBridge triggers the pipeline.
2. Lambda extracts YouTube trending data.
3. Raw data lands in Bronze S3.
4. Glue ETL transforms data into Silver.
5. Data Quality checks validate records.
6. Glue ETL creates Gold analytical datasets.
7. Athena queries Gold datasets.
8. SNS sends operational alerts.

## 🚀 Technologies Used

- AWS Lambda
- AWS Glue
- Amazon S3
- AWS Step Functions
- Amazon Athena
- Amazon SNS
- Amazon EventBridge
- PySpark
- Python
- YouTube Data API v3
- Parquet
- AWS IAM

## 📊 Data Architecture

```text
YouTube API
     │
     ▼
 Lambda Ingestion
     │
     ▼
 Bronze Layer (S3)
     │
     ▼
 Glue ETL
     │
     ▼
 Silver Layer (S3)
     │
     ▼
 Data Quality Validation
     │
     ▼
 Glue Aggregation
     │
     ▼
 Gold Layer (S3)
     │
     ▼
 Athena Analytics
     │
     ▼
 Dashboards / Insights
```

## 📈 Key Features

- Automated daily ingestion
- Serverless architecture
- Data quality validation
- Medallion architecture implementation
- Partitioned Parquet storage
- Athena analytics layer
- Workflow orchestration using Step Functions
- Notification system using SNS
- Scalable PySpark transformations
