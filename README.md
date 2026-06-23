# Youtube data-pipeline using aws-s3-lambda -glue-athena-stepfunction
A cloud-native ETL pipeline that ingests YouTube trending video data across 10 regions, transforms it through a medallion architecture (Bronze > Silver > Gold), enforces data quality gates, and produces analytics-ready aggregations — all orchestrated by AWS Step Functions.
<img width="2784" height="1536" alt="YouTube Trending Data Pipeline" src="https://github.com/user-attachments/assets/173dcea9-4d2b-430b-9f1e-300855332da1" />


## Overview
This pipeline automates the end-to-end process of collecting, cleaning, and analyzing YouTube trending video data. It replaces manual Kaggle dataset downloads with live YouTube Data API v3 integration and produces three sets of business analytics tables:

Trending Analytics — daily trending metrics per region (total videos, views, engagement rates)
Channel Analytics — channel-level performance and ranking across regions
Category Analytics — category-level breakdowns with view share percentages
The pipeline supports 10 regions and runs on a configurable schedule via AWS EventBridge.    


## Architecture Diagram(Solution Architecture)
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

## 🥉 Bronze Layer (Raw Data)

The ingestion Lambda (`youtube_api_ingestion`) extracts trending YouTube video statistics and category metadata from the YouTube Data API v3.

### Data Sources

- **Trending Videos** – Top 50 trending videos per region
- **Category Mappings** – Video category ID-to-name reference dataset

### Storage Pattern

Raw data is stored in Amazon S3 as JSON and partitioned for efficient querying and downstream processing.

```text
s3://bronze-bucket/youtube/raw_statistics/region=US/date=2026-04-01/hour=12/
s3://bronze-bucket/youtube/raw_statistics_reference_data/region=US/
```

### Key Features

- API-driven ingestion
- Incremental data collection
- Region-based partitioning
- Event-driven architecture using AWS Lambda
- Historical backfill support using Kaggle datasets

Historical CSV datasets can be uploaded into the Bronze layer using the `aws_copy.sh` utility script.

---

## 🥈 Silver Layer (Cleansed & Standardized Data)

The Silver layer applies data cleansing, schema standardization, enrichment, and validation to raw datasets.

Two parallel transformations process Bronze data:

### 1. Statistics Transformation (`bronze_to_silver_statistics`)

#### Processing Steps

- Schema enforcement across API JSON and Kaggle CSV datasets
- Data type standardization
  - Views → BIGINT
  - Likes → BIGINT
  - Comments → BIGINT
  - Dates → TIMESTAMP
- Region code normalization
- Null value handling
- Duplicate record removal
- Business metric generation

#### Derived Metrics

- `like_ratio`
- `engagement_rate`
- `comment_rate`
- `views_per_day`

#### Output

```text
Format: Apache Parquet
Compression: Snappy
Partitioning: region
Storage Layer: Silver
```

### 2. Reference Data Transformation (`json_to_parquet`)

#### Processing Steps

- Converts category mappings from JSON to tabular format
- Removes duplicate category records
- Standardizes category metadata
- Converts data to analytics-friendly Parquet format

#### Output

```text
Format: Apache Parquet
Compression: Snappy
Partitioning: region
Storage Layer: Silver
```

### Benefits of the Silver Layer

- Improved query performance
- 
- Reduced storage footprint
- Standardized schema across regions
- Analytics-ready datasets
- Optimized for AWS Athena and Glue Catalog

---

# ✅ Data Quality Gate

Before data is promoted from the Silver layer to the Gold layer, a dedicated Data Quality Lambda (`dq_lambda`) performs validation checks to ensure data reliability and integrity.

## Validation Rules

| Check | Validation Rule | Purpose |
|---------|---------|---------|
| Row Count | >= 10 rows | Prevent empty datasets |
| Null Percentage | <= 5% on critical columns | Ensure completeness |
| Schema Validation | Required columns present | Maintain schema consistency |
| Value Range Validation | Views, Likes, Comments >= 0 | Detect invalid values |
| Data Freshness | Latest data within 48 hours | Prevent stale analytics |

## Failure Handling

If any validation fails:

- Pipeline execution stops immediately
- Failure details are logged to CloudWatch
- SNS notification is sent to stakeholders
- Gold layer generation is blocked

## Benefits

- Prevents bad data from reaching analytics consumers
- Improves trust in reporting datasets
- Detects ingestion and transformation issues early
- Enforces data governance standards

---

# 🥇 Gold Layer (Business Analytics)

The Gold layer contains curated business-ready datasets optimized for reporting, dashboarding, and analytical workloads.

The Glue ETL job (`silver_to_gold_analytics`) generates three analytical data marts:

1. `trending_analytics`
2. `channel_analytics`
3. `category_analytics`

All Gold datasets are stored as:

```text
Format: Apache Parquet
Compression: Snappy
Partitioning: region
Catalog: AWS Glue Data Catalog
Query Engine: Amazon Athena
```

---

# 📊 Gold Layer Output Tables

## 1. trending_analytics

Daily trending metrics aggregated by region.

| Column | Description |
|----------|----------|
| region | Country code (US, GB, IN, etc.) |
| trending_date_parsed | Trending snapshot date |
| total_videos | Number of trending videos |
| total_views | Sum of views |
| total_likes | Sum of likes |
| avg_views_per_video | Average views per trending video |
| avg_like_ratio | Average like-to-view ratio |
| avg_engagement_rate | Average engagement rate |
| unique_channels | Distinct channel count |
| unique_categories | Distinct category count |

### Business Questions Answered

- Which regions generate the most engagement?
- How do trending patterns vary by country?
- Which regions have the highest average video performance?

---

## 2. channel_analytics

Channel-level performance, ranking, and engagement analysis.

| Column | Description |
|----------|----------|
| channel_title | YouTube channel name |
| region | Country code |
| total_videos | Number of trending videos |
| total_views | Total views across trending videos |
| avg_engagement_rate | Average engagement rate |
| times_trending | Number of appearances in trending lists |
| rank_in_region | Performance rank within region |
| categories | Categories associated with channel |

### Business Questions Answered

- Which creators dominate trending content?
- Which channels trend most frequently?
- What channels drive the highest engagement?

---

## 3. category_analytics

Category-level performance and market-share analysis.

| Column | Description |
|----------|----------|
| category | Video category name |
| region | Country code |
| trending_date_parsed | Trending snapshot date |
| video_count | Number of trending videos |
| total_views | Total category views |
| avg_engagement_rate | Average engagement rate |
| view_share_pct | Percentage of total regional views |

### Business Questions Answered

- Which categories are growing fastest?
- What categories capture the largest share of views?
- How does engagement differ by category?

---

## 📈 Analytics Layer Summary

The Gold layer transforms raw YouTube data into business-ready insights that support:

- Trend analysis
- Creator performance tracking
- Category benchmarking
- Regional content comparisons
- Executive dashboards
- Athena-powered ad-hoc analytics

---

# ✅ Data Quality Gate

Before data is promoted from the Silver layer to the Gold layer, a dedicated Data Quality Lambda (`dq_lambda`) performs validation checks to ensure data reliability and integrity.

## Validation Rules

| Check | Validation Rule | Purpose |
|---------|---------|---------|
| Row Count | >= 10 rows | Prevent empty datasets |
| Null Percentage | <= 5% on critical columns | Ensure completeness |
| Schema Validation | Required columns present | Maintain schema consistency |
| Value Range Validation | Views, Likes, Comments >= 0 | Detect invalid values |
| Data Freshness | Latest data within 48 hours | Prevent stale analytics |

## Failure Handling

If any validation fails:

- Pipeline execution stops immediately
- Failure details are logged to CloudWatch
- SNS notification is sent to stakeholders
- Gold layer generation is blocked

## Benefits

- Prevents bad data from reaching analytics consumers
- Improves trust in reporting datasets
- Detects ingestion and transformation issues early
- Enforces data governance standards

---

# 🥇 Gold Layer (Business Analytics)

The Gold layer contains curated business-ready datasets optimized for reporting, dashboarding, and analytical workloads.

The Glue ETL job (`silver_to_gold_analytics`) generates three analytical data marts:

1. `trending_analytics`
2. `channel_analytics`
3. `category_analytics`

All Gold datasets are stored as:

```text
Format: Apache Parquet
Compression: Snappy
Partitioning: region
Catalog: AWS Glue Data Catalog
Query Engine: Amazon Athena
```

---

# 📊 Gold Layer Output Tables

## 1. trending_analytics

Daily trending metrics aggregated by region.

| Column | Description |
|----------|----------|
| region | Country code (US, GB, IN, etc.) |
| trending_date_parsed | Trending snapshot date |
| total_videos | Number of trending videos |
| total_views | Sum of views |
| total_likes | Sum of likes |
| avg_views_per_video | Average views per trending video |
| avg_like_ratio | Average like-to-view ratio |
| avg_engagement_rate | Average engagement rate |
| unique_channels | Distinct channel count |
| unique_categories | Distinct category count |

### Business Questions Answered

- Which regions generate the most engagement?
- How do trending patterns vary by country?
- Which regions have the highest average video performance?

---

## 2. channel_analytics

Channel-level performance, ranking, and engagement analysis.

| Column | Description |
|----------|----------|
| channel_title | YouTube channel name |
| region | Country code |
| total_videos | Number of trending videos |
| total_views | Total views across trending videos |
| avg_engagement_rate | Average engagement rate |
| times_trending | Number of appearances in trending lists |
| rank_in_region | Performance rank within region |
| categories | Categories associated with channel |

### Business Questions Answered

- Which creators dominate trending content?
- Which channels trend most frequently?
- What channels drive the highest engagement?

---

## 3. category_analytics

Category-level performance and market-share analysis.

| Column | Description |
|----------|----------|
| category | Video category name |
| region | Country code |
| trending_date_parsed | Trending snapshot date |
| video_count | Number of trending videos |
| total_views | Total category views |
| avg_engagement_rate | Average engagement rate |
| view_share_pct | Percentage of total regional views |

### Business Questions Answered

- Which categories are growing fastest?
- What categories capture the largest share of views?
- How does engagement differ by category?

---

## 📈 Analytics Layer Summary

The Gold layer transforms raw YouTube data into business-ready insights that support:

- Trend analysis
- Creator performance tracking
- Category benchmarking
- Regional content comparisons
- Executive dashboards
- Athena-powered ad-hoc analytics

All Gold datasets are optimized for serverless querying using Amazon Athena and registered in the AWS Glue Data Catalog.

# ☁️ AWS Infrastructure Setup

This project uses a fully serverless AWS architecture built on Amazon S3, AWS Lambda, AWS Glue, Step Functions, Athena, EventBridge, and SNS.

## S3 Data Lake Buckets

Create the following S3 buckets for the Medallion Architecture:

```bash
aws s3 mb s3://yt-data-pipeline-bronze-<region>-<env>
aws s3 mb s3://yt-data-pipeline-silver-<region>-<env>
aws s3 mb s3://yt-data-pipeline-gold-<region>-<env>
aws s3 mb s3://yt-data-pipeline-script-<region>-<env>
```

### Bucket Purpose

| Bucket | Purpose |
|----------|----------|
| Bronze | Raw ingestion data |
| Silver | Cleansed and standardized datasets |
| Gold | Business-ready analytics datasets |
| Script | Glue ETL scripts and deployment artifacts |

---

## AWS Glue Catalog Databases

Create separate Glue databases for each processing layer.

```bash
aws glue create-database --database-input '{"Name":"yt_pipeline_bronze_<env>"}'

aws glue create-database --database-input '{"Name":"yt_pipeline_silver_<env>"}'

aws glue create-database --database-input '{"Name":"yt_pipeline_gold_<env>"}'
```

---

## SNS Alerting Configuration

Create an SNS topic for operational alerts and monitoring.

```bash
aws sns create-topic \
  --name yt-data-pipeline-alerts-<env>

aws sns subscribe \
  --topic-arn <topic-arn> \
  --protocol email \
  --notification-endpoint <your-email>
```

Notifications are generated for:

- Data quality failures
- Glue job failures
- Step Function failures
- Successful pipeline completion

---

# ⚙️ Configuration

## Lambda Environment Variables

### YouTube Ingestion Lambda

| Variable | Description | Example |
|----------|----------|----------|
| YOUTUBE_API_KEY | YouTube Data API v3 key | AIzaSyXXXX |
| S3_BUCKET_BRONZE | Bronze bucket name | yt-data-pipeline-bronze-dev |
| YOUTUBE_REGIONS | Regions to ingest | US,GB,CA,DE,FR,IN,JP |

---

### Data Quality Lambda

| Variable | Description | Default |
|----------|----------|----------|
| S3_BUCKET_SILVER | Silver bucket name | Required |
| GLUE_DB_SILVER | Silver Glue database | yt_pipeline_silver_dev |
| SNS_ALERT_TOPIC_ARN | SNS topic ARN | Required |
| DQ_MIN_ROW_COUNT | Minimum acceptable row count | 10 |
| DQ_MAX_NULL_PERCENT | Maximum allowed null percentage | 5.0 |

---

## Glue Job Runtime Parameters

Glue jobs receive parameters through AWS Step Functions.

| Parameter | Description |
|----------|----------|
| --bronze_database | Bronze Glue database |
| --bronze_table | Bronze table |
| --silver_database | Silver Glue database |
| --silver_bucket | Silver bucket |
| --gold_database | Gold Glue database |
| --gold_bucket | Gold bucket |

---

# 🚀 Deployment Guide

## Step 1: Upload Glue Scripts

```bash
aws s3 cp glue_jobs/bronze_to_silver_statistics.py \
s3://yt-data-pipeline-script-<region>-<env>/glue_jobs/

aws s3 cp glue_jobs/silver_to_gold_analytics.py \
s3://yt-data-pipeline-script-<region>-<env>/glue_jobs/
```

---

## Step 2: Deploy Lambda Functions

### YouTube Ingestion Lambda

```bash
cd lambdas/youtube_api_ingestion

zip -r function.zip lambda_function.py

aws lambda create-function \
  --function-name yt-data-pipeline-youtube-ingestion-<env> \
  --runtime python3.9 \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://function.zip \
  --role <lambda-execution-role-arn> \
  --timeout 300 \
  --memory-size 256
```

Repeat the process for:

- json_to_parquet Lambda
- dq_lambda

---

## Step 3: Create AWS Glue Jobs

```bash
aws glue create-job \
  --name yt-data-pipeline-bronze-to-silver-<env> \
  --role <glue-role-arn> \
  --command '{"Name":"glueetl","ScriptLocation":"s3://yt-data-pipeline-script-<region>-<env>/glue_jobs/bronze_to_silver_statistics.py"}' \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X
```

Create an additional Glue Job for:

```text
silver_to_gold_analytics.py
```

---

## Step 4: Deploy Step Functions Workflow

```bash
aws stepfunctions create-state-machine \
  --name yt-data-pipeline-workflow \
  --definition file://pipeline_orchestration.json \
  --role-arn <step-function-role-arn>
```

---

## Step 5: Configure EventBridge Scheduler

```bash
cron(0 */6 * * ? *)
```

Pipeline execution frequency:

- Every 6 hours
- Fully automated
- Event-driven orchestration

---

# 🔒 Security Controls

- IAM least-privilege access model
- Encrypted S3 storage
- Secure API key management
- CloudWatch logging enabled
- SNS monitoring and alerting
- Resource-level IAM permissions

---

# 💰 Cost Optimization

This project minimizes operational costs by leveraging:

- Serverless AWS Lambda
- On-demand AWS Glue
- Athena pay-per-query model
- Partitioned Parquet datasets

# ▶️ Running the Pipeline

The pipeline can be executed either automatically through EventBridge scheduling or manually using AWS Step Functions.

---

## Automated Execution (Recommended)

Amazon EventBridge triggers the Step Functions workflow on a predefined schedule.

### Create EventBridge Schedule

```bash
aws events put-rule \
  --name yt-pipeline-schedule \
  --schedule-expression "rate(6 hours)"
```

### Attach Step Function Target

```bash
aws events put-targets \
  --rule yt-pipeline-schedule \
  --targets '[{
    "Id":"1",
    "Arn":"<step-function-arn>",
    "RoleArn":"<eventbridge-role-arn>"
  }]'
```

### Benefits

- Fully automated ingestion
- No manual intervention required
- Supports continuous data collection
- Near real-time analytics updates

---

## Manual Execution

The workflow can also be started on demand.

```bash
aws stepfunctions start-execution \
  --state-machine-arn <state-machine-arn>
```

### Common Use Cases

- Testing new deployments
- Historical data backfills
- Debugging ETL workflows
- Data validation checks

---

# 🔄 Pipeline Execution Flow

```text
┌──────────────────────────────┐
│ 1. YouTube API Ingestion     │
│    Lambda Function           │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ 2. Bronze Layer              │
│    Raw JSON Storage in S3    │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ 3. Parallel Silver Jobs      │
├──────────────────────────────┤
│ Glue: bronze_to_silver       │
│ Lambda: json_to_parquet      │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ 4. Data Quality Validation   │
│    dq_lambda                 │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ 5. Gold Aggregations         │
│    silver_to_gold_analytics  │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ 6. Athena Analytics          │
│    Query Ready Datasets      │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ 7. SNS Notifications         │
│    Success / Failure Alerts  │
└──────────────────────────────┘
```

---

## Step Functions Workflow

### Stage 1: Data Ingestion

- Calls YouTube API
- Retrieves trending video statistics
- Retrieves category metadata
- Writes raw JSON files to Bronze S3

### Stage 2: Silver Transformations

Runs in parallel:

#### Statistics ETL

- Cleans raw video statistics
- Standardizes schema
- Calculates derived metrics
- Writes Silver Parquet datasets

#### Reference Data ETL

- Converts category mappings to Parquet
- Standardizes category reference data
- Writes Silver reference datasets

### Stage 3: Data Quality Validation

Validates:

- Row counts
- Null thresholds
- Required schema
- Value ranges
- Data freshness

If validation fails:

- Workflow stops
- SNS alert is sent
- Gold layer is not generated

### Stage 4: Gold Analytics Generation

Creates:

- `trending_analytics`
- `channel_analytics`
- `category_analytics`

### Stage 5: Notifications

SNS sends:

- Success notifications
- Failure notifications
- Validation alerts

---

## Reliability Features

### Retry Policy

Every workflow stage supports:

- Maximum retries: 3
- Exponential backoff
- Automatic transient error recovery

### Monitoring

- AWS CloudWatch Logs
- AWS Step Functions Execution History
- SNS Email Notifications
- Glue Job Metrics

### Error Handling

- Failed workflows are logged
- Detailed error messages captured
- Alert notifications sent automatically
- Data Quality failures block downstream processing

---

## Expected Runtime

| Component | Typical Runtime |
|------------|------------|
| Lambda Ingestion | 1–2 minutes |
| Silver ETL | 2–5 minutes |
| Data Quality Checks | <1 minute |
| Gold Aggregation | 2–4 minutes |
| Total Pipeline Runtime | 5–10 minutes |
- Snappy compression
- Event-driven processing

Estimated monthly cost for moderate workloads: **$10–$30/month**

# 📥 Data Sources

The pipeline combines real-time API ingestion with historical datasets to support analytics, testing, and backfill operations.

---

## 1. YouTube Data API v3 (Primary Source)

The primary source for trending video analytics.

### Data Collected

- Trending videos by region
- Video statistics
  - Views
  - Likes
  - Comments
- Channel metadata
- Video category information
- Publish dates
- Trending dates

### Benefits

- Near real-time data ingestion
- Global region coverage
- Reliable and authoritative source
- Supports automated incremental loads

### Sample API Endpoint

```text
https://www.googleapis.com/youtube/v3/videos
```

---

## 2. Kaggle YouTube Trending Dataset (Historical Source)

Used for historical backfills, development, testing, and validation.

### Dataset Coverage

- 10 countries/regions
- Historical trending snapshots
- Video metadata
- Engagement metrics
- Category mappings

### Use Cases

- Historical trend analysis
- ETL testing
- Data quality validation
- Pipeline benchmarking
- Development and debugging

### Dataset Files

```text
USvideos.csv
GBvideos.csv
INvideos.csv
CAvideos.csv
DEvideos.csv
FRvideos.csv
JPvideos.csv
KRvideos.csv
MXvideos.csv
RUvideos.csv
```

---

## Data Source Strategy

| Source | Purpose |
|----------|----------|
| YouTube Data API v3 | Real-time ingestion |
| Kaggle Trending Dataset | Historical backfill |
| Category Mapping JSON | Reference data enrichment |

---

## Supported Regions

The pipeline currently supports:

| Region Code | Country |
|------------|------------|
| US | United States |
| GB | United Kingdom |
| CA | Canada |
| DE | Germany |
| FR | France |
| IN | India |
| JP | Japan |
| KR | South Korea |
| MX | Mexico |
| RU | Russia |

---

## Data Volume

Typical ingestion cycle:

- 50 trending videos per region
- 10 supported regions
- ~500 video records per execution
- Executed every 6 hours
- Approximately 60,000+ records per month

This hybrid ingestion strategy enables both real-time analytics and historical trend analysis while maintaining a scalable and cost-efficient data lake architecture.
