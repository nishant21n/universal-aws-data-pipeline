import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_date, year, month, day, current_timestamp, when, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
import boto3
import json
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
ssm_client = boto3.client('ssm')

def get_source_config(source_name, config_param_path):
    """
    Retrieve source configuration from Parameter Store
    """
    try:
        param_name = f"{config_param_path}/{source_name}"
        response = ssm_client.get_parameter(
            Name=param_name,
            WithDecryption=True
        )
        return json.loads(response['Parameter']['Value'])
    except Exception as e:
        logger.error(f"Error retrieving configuration for source {source_name}: {str(e)}")
        raise

def apply_schema_mapping(df, schema_mapping):
    """
    Apply schema mapping to dataframe
    """
    # Create a new dataframe with mapped column names
    for target_col, source_path in schema_mapping.items():
        if '.' in source_path:
            # Handle nested fields
            parts = source_path.split('.')
            curr_col = parts[0]
            for part in parts[1:]:
                curr_col = col(curr_col)[part]
            df = df.withColumn(target_col, curr_col)
        else:
            # Simple column rename
            df = df.withColumnRenamed(source_path, target_col)
    
    return df

def apply_transformations(df, transformations):
    """
    Apply specified transformations to the dataframe
    """
    if not transformations:
        return df
    
    for transform in transformations:
        field = transform.get('field')
        transform_type = transform.get('type')
        
        if not field or not transform_type:
            continue
        
        if transform_type == 'datetime':
            format_str = transform.get('format', 'yyyy-MM-dd HH:mm:ss')
            df = df.withColumn(field, to_date(col(field), format_str))
        
        elif transform_type == 'string':
            df = df.withColumn(field, col(field).cast(StringType()))
        
        elif transform_type == 'integer':
            df = df.withColumn(field, col(field).cast(IntegerType()))
        
        elif transform_type == 'double':
            df = df.withColumn(field, col(field).cast(DoubleType()))
        
        elif transform_type == 'trim':
            df = df.withColumn(field, trim(col(field)))
        
        elif transform_type == 'regexp_replace':
            pattern = transform.get('pattern', '')
            replacement = transform.get('replacement', '')
            df = df.withColumn(field, regexp_replace(col(field), pattern, replacement))
    
    return df

def handle_missing_values(df, required_fields):
    """
    Handle missing values in dataframe
    """
    if not required_fields:
        return df
    
    # Drop rows where required fields are null
    for field in required_fields:
        df = df.filter(col(field).isNotNull())
    
    return df

def add_metadata_columns(df, source_name):
    """
    Add metadata columns to dataframe
    """
    # Add standard metadata columns
    return df.withColumn("source_name", lit(source_name)) \
             .withColumn("processed_timestamp", current_timestamp())

def partition_dataframe(df, partition_columns):
    """
    Add partition columns to dataframe if they don't exist
    """
    if not partition_columns:
        return df
    
    for partition in partition_columns:
        if partition == 'year' and 'year' not in df.columns:
            # Check if there's a date column we can extract year from
            date_columns = [c for c in df.columns if 'date' in c.lower()]
            if date_columns:
                df = df.withColumn('year', year(col(date_columns[0])))
            else:
                # Use current year if no date column found
                from datetime import datetime
                current_year = datetime.now().year
                df = df.withColumn('year', lit(current_year))
                
        elif partition == 'month' and 'month' not in df.columns:
            date_columns = [c for c in df.columns if 'date' in c.lower()]
            if date_columns:
                df = df.withColumn('month', month(col(date_columns[0])))
            else:
                from datetime import datetime
                current_month = datetime.now().month
                df = df.withColumn('month', lit(current_month))
                
        elif partition == 'day' and 'day' not in df.columns:
            date_columns = [c for c in df.columns if 'date' in c.lower()]
            if date_columns:
                df = df.withColumn('day', day(col(date_columns[0])))
            else:
                from datetime import datetime
                current_day = datetime.now().day
                df = df.withColumn('day', lit(current_day))
                
    return df

def main():
    # Parse job arguments
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 
        'source_name', 
        'input_path', 
        'output_path', 
        'config_param_path',
        'data_format'
    ])
    
    # Initialize Spark and Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Get parameters
    source_name = args['source_name']
    input_path = args['input_path']
    output_path = args['output_path']
    config_param_path = args['config_param_path']
    data_format = args.get('data_format', 'json')
    
    logger.info(f"Starting transformation job for source: {source_name}")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")
    
    try:
        # Get source configuration
        source_config = get_source_config(source_name, config_param_path)
        
        # Read input data
        if data_format.lower() == 'json':
            df = spark.read.json(input_path)
        elif data_format.lower() == 'csv':
            df = spark.read.option("header", "true").csv(input_path)
        elif data_format.lower() == 'parquet':
            df = spark.read.parquet(input_path)
        else:
            raise ValueError(f"Unsupported data format: {data_format}")
        
        # Get schema configuration
        schema_config = source_config.get('schema', {})
        schema_mapping = schema_config.get('mapping', {})
        required_fields = schema_config.get('required', [])
        transformations = schema_config.get('transformations', [])
        
        # Get destination configuration
        dest_config = source_config.get('destination', {}).get('processed', {})
        partition_by = dest_config.get('partitionBy', [])
        
        # Apply transformations
        if schema_mapping:
            df = apply_schema_mapping(df, schema_mapping)
        
        df = apply_transformations(df, transformations)
        df = handle_missing_values(df, required_fields)
        df = add_metadata_columns(df, source_name)
        df = partition_dataframe(df, partition_by)
        
        # Write transformed data
        output_format = dest_config.get('format', 'parquet')
        
        if partition_by:
            # Write with partitioning
            df.write \
              .format(output_format) \
              .partitionBy(*partition_by) \
              .mode("overwrite") \
              .save(output_path)
        else:
            # Write without partitioning
            df.write \
              .format(output_format) \
              .mode("overwrite") \
              .save(output_path)
        
        logger.info(f"Successfully transformed data for source: {source_name}")
        logger.info(f"Output written to: {output_path}")
        
        # Log record counts
        input_count = df.count()
        output_count = df.count()
        logger.info(f"Input record count: {input_count}")
        logger.info(f"Output record count: {output_count}")
        
        # Commit job
        job.commit()
        
    except Exception as e:
        logger.error(f"Error in transformation job: {str(e)}")
        raise

if __name__ == "__main__":
    main() 