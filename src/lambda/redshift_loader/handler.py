import json
import boto3
import os
import psycopg2
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
ssm_client = boto3.client('ssm')
sns_client = boto3.client('sns')
secretsmanager_client = boto3.client('secretsmanager')

# Constants
REDSHIFT_SECRET_NAME = os.environ.get('REDSHIFT_SECRET_NAME')
ALERT_TOPIC_ARN = os.environ.get('ALERT_TOPIC_ARN')
CONFIG_PARAM_PATH = os.environ.get('CONFIG_PARAM_PATH', '/data-pipeline/sources')
IAM_ROLE = os.environ.get('REDSHIFT_COPY_ROLE_ARN')

def get_redshift_credentials():
    """
    Retrieve Redshift credentials from Secrets Manager
    """
    try:
        response = secretsmanager_client.get_secret_value(
            SecretId=REDSHIFT_SECRET_NAME
        )
        secret = json.loads(response['SecretString'])
        return {
            'host': secret.get('host'),
            'port': secret.get('port', 5439),
            'dbname': secret.get('dbname'),
            'username': secret.get('username'),
            'password': secret.get('password')
        }
    except ClientError as e:
        logger.error(f"Error retrieving Redshift credentials: {str(e)}")
        raise

def get_source_config(source_name):
    """
    Retrieve source configuration from Parameter Store
    """
    try:
        param_name = f"{CONFIG_PARAM_PATH}/{source_name}"
        response = ssm_client.get_parameter(
            Name=param_name,
            WithDecryption=True
        )
        return json.loads(response['Parameter']['Value'])
    except ClientError as e:
        logger.error(f"Error retrieving source configuration: {str(e)}")
        raise

def connect_to_redshift(credentials):
    """
    Connect to Redshift cluster
    """
    try:
        conn = psycopg2.connect(
            host=credentials['host'],
            port=credentials['port'],
            dbname=credentials['dbname'],
            user=credentials['username'],
            password=credentials['password']
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Redshift: {str(e)}")
        raise

def execute_copy_command(conn, source_config, s3_path, execution_id):
    """
    Execute COPY command to load data into Redshift
    """
    redshift_config = source_config.get('redshift', {})
    schema = redshift_config.get('schema', 'public')
    table = redshift_config.get('table')
    distkey = redshift_config.get('distkey')
    sortkey = redshift_config.get('sortkey', [])
    
    if not table:
        raise ValueError("Table name not specified in source configuration")
    
    # Create staging table
    staging_table = f"{table}_staging_{execution_id}"
    
    # Determine source format
    dest_config = source_config.get('destination', {}).get('processed', {})
    format_type = dest_config.get('format', 'parquet').upper()
    
    try:
        with conn.cursor() as cursor:
            # Set search path
            cursor.execute(f"SET search_path TO {schema}")
            
            # Get table definition
            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            
            if not columns:
                raise ValueError(f"Table {schema}.{table} does not exist or has no columns")
            
            # Create staging table like the target table
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{staging_table}")
            cursor.execute(f"CREATE TABLE {schema}.{staging_table} (LIKE {schema}.{table})")
            
            # Build the COPY command
            column_list = ", ".join([col[0] for col in columns])
            
            copy_options = []
            if format_type == 'PARQUET':
                copy_options.append("FORMAT AS PARQUET")
            elif format_type == 'JSON':
                copy_options.append("FORMAT JSON 'auto'")
            elif format_type == 'CSV':
                copy_options.append("CSV DELIMITER ',' IGNOREHEADER 1")
            
            # Add compression option if applicable
            copy_options.append("COMPUPDATE ON")
            
            # Add error handling options
            copy_options.append("MAXERROR 10")
            
            # Join all options
            options_str = " ".join(copy_options)
            
            # Execute COPY command
            copy_cmd = f"""
                COPY {schema}.{staging_table} ({column_list})
                FROM '{s3_path}'
                IAM_ROLE '{IAM_ROLE}'
                {options_str}
            """
            
            logger.info(f"Executing COPY command: {copy_cmd}")
            cursor.execute(copy_cmd)
            
            # Get row count from staging table
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{staging_table}")
            row_count = cursor.fetchone()[0]
            
            # Begin transaction for the swap
            conn.autocommit = False
            
            # Rename tables to swap staging with production
            old_table = f"{table}_old_{execution_id}"
            cursor.execute(f"ALTER TABLE {schema}.{table} RENAME TO {old_table}")
            cursor.execute(f"ALTER TABLE {schema}.{staging_table} RENAME TO {table}")
            
            # Commit the transaction
            conn.commit()
            conn.autocommit = True
            
            # Drop the old table
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{old_table}")
            
            # Vacuum and analyze the table
            if redshift_config.get('vacuum_strategy') == 'auto':
                cursor.execute(f"VACUUM {schema}.{table}")
            
            if redshift_config.get('analyze', False):
                cursor.execute(f"ANALYZE {schema}.{table}")
            
            return {
                'status': 'success',
                'rows_loaded': row_count,
                'table': f"{schema}.{table}"
            }
            
    except Exception as e:
        logger.error(f"Error executing COPY command: {str(e)}")
        conn.rollback()
        raise

def lambda_handler(event, context):
    """
    Main Lambda handler for loading data into Redshift
    """
    try:
        # Extract parameters from event
        source_name = event.get('source_name')
        s3_path = event.get('s3_path')
        execution_id = event.get('execution_id', datetime.now().strftime("%Y%m%d%H%M%S"))
        
        if not source_name or not s3_path:
            error_message = "Missing required parameters: source_name and s3_path"
            logger.error(error_message)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_message})
            }
        
        logger.info(f"Starting Redshift load job for source: {source_name}")
        logger.info(f"S3 path: {s3_path}")
        
        # Get source configuration
        source_config = get_source_config(source_name)
        
        # Get Redshift credentials
        credentials = get_redshift_credentials()
        
        # Connect to Redshift
        conn = connect_to_redshift(credentials)
        
        try:
            # Execute COPY command
            result = execute_copy_command(conn, source_config, s3_path, execution_id)
            
            # Send success notification
            notification = {
                'source': 'redshift_loader',
                'timestamp': datetime.now().isoformat(),
                'source_name': source_name,
                's3_path': s3_path,
                'execution_id': execution_id,
                'result': result
            }
            
            sns_client.publish(
                TopicArn=ALERT_TOPIC_ARN,
                Message=json.dumps(notification),
                Subject=f"Redshift load completed: {source_name}"
            )
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Redshift load completed successfully',
                    'result': result
                })
            }
            
        finally:
            # Close the connection
            if conn:
                conn.close()
        
    except Exception as e:
        error_message = f"Error loading data into Redshift: {str(e)}"
        logger.error(error_message)
        
        # Send failure notification
        try:
            sns_client.publish(
                TopicArn=ALERT_TOPIC_ARN,
                Message=json.dumps({
                    'source': 'redshift_loader',
                    'error': error_message,
                    'timestamp': datetime.now().isoformat(),
                    'source_name': source_name if source_name else 'unknown',
                    's3_path': s3_path if s3_path else 'unknown',
                    'execution_id': execution_id
                }),
                Subject=f"ERROR: Redshift load failed for {source_name if source_name else 'unknown'}"
            )
        except Exception as sns_error:
            logger.error(f"Failed to send error notification: {str(sns_error)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }
 