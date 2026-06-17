# Youtube data-pipeline using aws-s3-lambda -glue-athena-stepfunction
A cloud-native ETL pipeline that ingests YouTube trending video data across 10 regions, transforms it through a medallion architecture (Bronze > Silver > Gold), enforces data quality gates, and produces analytics-ready aggregations — all orchestrated by AWS Step Functions.
<img width="2784" height="1536" alt="YouTube Trending Data Pipeline" src="https://github.com/user-attachments/assets/173dcea9-4d2b-430b-9f1e-300855332da1" />
#Overview
This pipeline automates the end-to-end process of collecting, cleaning, and analyzing YouTube trending video data. It replaces manual Kaggle dataset downloads with live YouTube Data API v3 integration and produces three sets of business analytics tables:

Trending Analytics — daily trending metrics per region (total videos, views, engagement rates)
Channel Analytics — channel-level performance and ranking across regions
Category Analytics — category-level breakdowns with view share percentages
The pipeline supports 10 regions and runs on a configurable schedule via AWS EventBridge.
