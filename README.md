# Universal AWS Data Engineering Pipeline

A production-ready, source-agnostic data pipeline built on AWS services for ingesting, processing, and storing data from multiple sources.

## Features

- Source-agnostic data ingestion from multiple input types:
  - REST APIs
  - Email attachments (AWS WorkMail/SES)
  - S3 file uploads (manual or automated)
  - Streaming sources (Kinesis/Kafka)
- Modular architecture for easy onboarding of new data sources
- Comprehensive data transformation with AWS Glue (PySpark)
- Schema evolution and variation handling
- High-performance data warehousing with Amazon Redshift
- End-to-end orchestration with AWS Step Functions
- Robust monitoring, logging, and alerting
- Security with IAM roles and KMS encryption

## Architecture


The pipeline consists of the following layers:

1. **Ingestion Layer**
   - Lambda functions for handling various input sources
   - S3 buckets for initial data landing
   - Event-driven triggers for pipeline initiation

2. **Processing Layer**
   - AWS Glue jobs for data transformation (using DataFrame API)
   - Glue Crawlers for schema discovery and evolution
   - Lambda functions for metadata extraction and processing

3. **Storage Layer**
   - S3 for raw, processed, and curated data (organized by partitions)
   - Redshift for data warehousing with performance optimizations

4. **Orchestration Layer**
   - Step Functions workflows for pipeline coordination
   - Error handling and retry mechanisms
   - CloudWatch and SNS for monitoring and alerting

## Project Structure

```
├── cdk/                        # AWS CDK Infrastructure as Code
│   ├── lib/                    # CDK stack definitions
│   ├── bin/                    # CDK app entry point
│   └── cdk.json                # CDK configuration
├── src/
│   ├── lambda/                 # Lambda function code
│   │   ├── api_ingestion/      # REST API data ingestion
│   │   ├── email_processor/    # SES/WorkMail email processing
│   │   ├── file_processor/     # S3 file handling
│   │   ├── stream_processor/   # Kinesis/Kafka stream processing
│   │   └── redshift_loader/    # Redshift COPY/UNLOAD operations
│   ├── glue/                   # Glue PySpark scripts
│   │   ├── common/             # Common transformation functions
│   │   ├── cleaning/           # Data cleaning jobs
│   │   ├── transformation/     # Data transformation jobs
│   │   └── validation/         # Data validation jobs
│   ├── config/                 # Configuration files
│   │   ├── sources/            # Data source configurations
│   │   ├── schemas/            # Schema definitions
│   │   └── pipelines/          # Pipeline workflow definitions
│   └── utils/                  # Shared utility functions
├── tests/                      # Test files
└── requirements.txt            # Python dependencies
```

## Prerequisites

- AWS Account with appropriate permissions
- AWS CLI configured
- Python 3.8+ (for CDK and Lambda development)
- Node.js 14+ (for CDK)

## Setup and Deployment

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/aws-data-pipeline.git
   cd aws-data-pipeline
   ```

2. Install dependencies:
   ```bash
   npm install -g aws-cdk
   pip install -r requirements.txt
   ```

3. Configure the pipeline for your environment:
   - Update `src/config/sources` with your data source configurations
   - Modify `cdk/lib/config.ts` with your AWS account details and preferences

4. Deploy the infrastructure:
   ```bash
   cd cdk
   cdk bootstrap
   cdk deploy
   ```

## Adding New Data Sources

New data sources can be easily added by creating a configuration file in YAML or JSON format:

1. Create a new configuration file in `src/config/sources/` (see example below)
2. Deploy the updated infrastructure with `cdk deploy`

Example configuration (`src/config/sources/new_source.yaml`):
```yaml
source:
  name: customer_data_api
  type: rest_api
  config:
    endpoint: https://api.example.com/customers
    method: GET
    headers:
      Authorization: ${SECRET:customer_api_key}
    schedule: rate(1 hour)
  schema:
    type: json
    mapping:
      customerId: id
      customerName: name
      customerEmail: email
  destination:
    bucket: ${BUCKET:processed}
    prefix: customers/
    partitionBy:
      - year
      - month
      - day
  redshift:
    schema: customers
    table: customer_data
    distkey: customerId
    sortkey: customerName
```

## Performance Optimization

### Redshift Best Practices

- DISTKEY and SORTKEY are configured based on query patterns
- COPY commands are used for efficient data loading
- UNLOAD operations export data for external processing

### Glue Job Optimization

- PySpark DataFrame API is used for all transformations
- Partition pruning for efficient data access
- Dynamic allocated resources based on data volume

## Monitoring and Alerting

- CloudWatch dashboards track pipeline performance
- SNS notifications for job failures and data quality issues
- Step Function execution visualization for workflow monitoring

## Security

- IAM roles follow least privilege principle
- KMS encryption for data at rest
- VPC endpoints for secure service communication
- Secrets management for sensitive information

