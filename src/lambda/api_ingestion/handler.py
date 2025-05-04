import json
import boto3
import os
import requests
import logging
from datetime import datetime
import uuid
from botocore.exceptions import ClientError
import base64

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')
sns_client = boto3.client('sns')
secretsmanager_client = boto3.client('secretsmanager')

# Constants
RAW_BUCKET = os.environ.get('RAW_BUCKET')
ALERT_TOPIC_ARN = os.environ.get('ALERT_TOPIC_ARN')
CONFIG_PARAM_PATH = os.environ.get('CONFIG_PARAM_PATH', '/data-pipeline/sources')

def get_secret(secret_name):
    """
    Retrieve a secret from AWS Secrets Manager
    """
    try:
        response = secretsmanager_client.get_secret_value(
            SecretId=secret_name
        )
        return response['SecretString']
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise

def process_template_values(value, secrets_cache=None):
    """
    Process template values in configuration strings (e.g., ${SECRET:my_secret})
    """
    if not isinstance(value, str):
        return value
        
    if secrets_cache is None:
        secrets_cache = {}
        
    # Process secret references
    if value.startswith('${SECRET:') and value.endswith('}'):
        secret_name = value[9:-1]
        if secret_name in secrets_cache:
            return secrets_cache[secret_name]
        
        secret_value = get_secret(secret_name)
        secrets_cache[secret_name] = secret_value
        return secret_value
        
    # Process environment variable references
    if value.startswith('${ENV:') and value.endswith('}'):
        env_var = value[6:-1]
        return os.environ.get(env_var, '')
        
    return value

def process_config_values(config, secrets_cache=None):
    """
    Process all template values in a configuration object
    """
    if secrets_cache is None:
        secrets_cache = {}
        
    if isinstance(config, dict):
        processed_config = {}
        for key, value in config.items():
            processed_config[key] = process_config_values(value, secrets_cache)
        return processed_config
    elif isinstance(config, list):
        return [process_config_values(item, secrets_cache) for item in config]
    else:
        return process_template_values(config, secrets_cache)

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
        config = json.loads(response['Parameter']['Value'])
        
        # Process any template values in the configuration
        return process_config_values(config)
    except Exception as e:
        logger.error(f"Error retrieving source configuration: {str(e)}")
        raise

def make_api_request(api_config):
    """
    Make a request to the configured API
    """
    url = api_config.get('endpoint')
    method = api_config.get('method', 'GET').upper()
    headers = api_config.get('headers', {})
    params = api_config.get('parameters', {})
    body = api_config.get('body')
    timeout = api_config.get('timeout', 30)
    auth_type = api_config.get('auth_type')
    
    # Handle authentication
    auth = None
    if auth_type == 'basic':
        username = api_config.get('username', '')
        password = api_config.get('password', '')
        auth = (username, password)
    elif auth_type == 'bearer':
        token = api_config.get('token', '')
        headers['Authorization'] = f"Bearer {token}"
    
    # Make the request
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, params=params, auth=auth, timeout=timeout)
        elif method == 'POST':
            response = requests.post(url, headers=headers, params=params, json=body, auth=auth, timeout=timeout)
        elif method == 'PUT':
            response = requests.put(url, headers=headers, params=params, json=body, auth=auth, timeout=timeout)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, params=params, auth=auth, timeout=timeout)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        # Check if the request was successful
        response.raise_for_status()
        
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise

def handle_pagination(api_config, initial_response):
    """
    Handle API pagination if configured
    """
    pagination_config = api_config.get('pagination', {})
    if not pagination_config.get('enabled', False):
        # Return the initial response if pagination is not enabled
        return [initial_response.json()]
    
    # Initialize results with the first page
    results = [initial_response.json()]
    
    # Get pagination configuration
    limit_param = pagination_config.get('limit_param')
    offset_param = pagination_config.get('offset_param')
    page_param = pagination_config.get('page_param')
    next_token_param = pagination_config.get('next_token_param')
    next_url_path = pagination_config.get('next_url_path')
    max_pages = pagination_config.get('max_pages', 10)
    
    # Create a copy of the original parameters
    params = api_config.get('parameters', {}).copy()
    
    # Handle offset-based pagination
    if offset_param and limit_param:
        offset = int(params.get(offset_param, 0))
        limit = int(params.get(limit_param, 100))
        
        for page in range(1, max_pages):
            offset += limit
            params[offset_param] = str(offset)
            
            try:
                response = requests.get(
                    api_config.get('endpoint'),
                    headers=api_config.get('headers', {}),
                    params=params,
                    timeout=api_config.get('timeout', 30)
                )
                response.raise_for_status()
                
                page_data = response.json()
                
                # Check if we've reached the end of data
                if not page_data or (isinstance(page_data, list) and len(page_data) == 0):
                    break
                    
                results.append(page_data)
            except Exception as e:
                logger.warning(f"Pagination request failed: {str(e)}")
                break
    
    # Handle page-based pagination
    elif page_param:
        current_page = int(params.get(page_param, 1))
        
        for page in range(current_page + 1, current_page + max_pages):
            params[page_param] = str(page)
            
            try:
                response = requests.get(
                    api_config.get('endpoint'),
                    headers=api_config.get('headers', {}),
                    params=params,
                    timeout=api_config.get('timeout', 30)
                )
                response.raise_for_status()
                
                page_data = response.json()
                
                # Check if we've reached the end of data
                if not page_data or (isinstance(page_data, list) and len(page_data) == 0):
                    break
                    
                results.append(page_data)
            except Exception as e:
                logger.warning(f"Pagination request failed: {str(e)}")
                break
    
    # Handle token-based pagination
    elif next_token_param:
        response_json = initial_response.json()
        
        # Extract the next token based on the configured path
        next_token = None
        if next_url_path:
            parts = next_url_path.split('.')
            current = response_json
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    current = None
                    break
            next_token = current
        
        # Continue fetching pages until no more next token
        page = 1
        while next_token and page < max_pages:
            params[next_token_param] = next_token
            
            try:
                response = requests.get(
                    api_config.get('endpoint'),
                    headers=api_config.get('headers', {}),
                    params=params,
                    timeout=api_config.get('timeout', 30)
                )
                response.raise_for_status()
                
                page_data = response.json()
                results.append(page_data)
                
                # Get the next token for the next page
                if next_url_path:
                    current = page_data
                    for part in next_url_path.split('.'):
                        if isinstance(current, dict) and part in current:
                            current = current[part]
                        else:
                            current = None
                            break
                    next_token = current
                else:
                    next_token = None
                    
                page += 1
            except Exception as e:
                logger.warning(f"Pagination request failed: {str(e)}")
                break
    
    return results

def upload_to_s3(data, source_config):
    """
    Upload API response data to S3
    """
    try:
        # Generate a unique key
        timestamp = datetime.now().strftime("%Y/%m/%d/%H/%M/%S")
        source_name = source_config.get('name', 'unknown')
        unique_id = str(uuid.uuid4())
        
        # Get destination configuration
        dest_config = source_config.get('destination', {}).get('raw', {})
        prefix = dest_config.get('prefix', f"api/{source_name}")
        
        # Define the S3 key with appropriate partitioning
        s3_key = f"{prefix}/{timestamp}/{unique_id}.json"
        
        # Upload the data to S3
        s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType='application/json',
            Metadata={
                'source_name': source_name,
                'timestamp': datetime.now().isoformat()
            }
        )
        
        logger.info(f"Successfully uploaded data to s3://{RAW_BUCKET}/{s3_key}")
        return {
            'bucket': RAW_BUCKET,
            'key': s3_key
        }
    except Exception as e:
        logger.error(f"Error uploading data to S3: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    Main Lambda handler for API ingestion
    """
    try:
        # Get source name from event
        source_name = event.get('source_name')
        if not source_name:
            error_message = "Missing required parameter: source_name"
            logger.error(error_message)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_message})
            }
        
        logger.info(f"Starting API ingestion for source: {source_name}")
        
        # Get source configuration
        source_config = get_source_config(source_name)
        
        # Validate source configuration
        if source_config.get('type') != 'rest_api':
            error_message = f"Invalid source type for {source_name}: {source_config.get('type')}"
            logger.error(error_message)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_message})
            }
        
        # Get API configuration
        api_config = source_config.get('config', {})
        if not api_config.get('endpoint'):
            error_message = f"Missing API endpoint in source configuration for {source_name}"
            logger.error(error_message)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_message})
            }
        
        # Make API request
        response = make_api_request(api_config)
        
        # Handle pagination if configured
        all_pages = handle_pagination(api_config, response)
        
        # Process and upload each page of results
        uploaded_results = []
        for page_data in all_pages:
            result = upload_to_s3(page_data, source_config)
            uploaded_results.append(result)
        
        # Send success notification
        notification = {
            'source': 'api_ingestion',
            'timestamp': datetime.now().isoformat(),
            'source_name': source_name,
            'uploaded_files': uploaded_results
        }
        
        sns_client.publish(
            TopicArn=ALERT_TOPIC_ARN,
            Message=json.dumps(notification),
            Subject=f"API ingestion completed: {source_name}"
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f"Successfully ingested data from API for source: {source_name}",
                'uploaded_files': uploaded_results
            })
        }
    
    except Exception as e:
        error_message = f"Error ingesting API data: {str(e)}"
        logger.error(error_message)
        
        # Send failure notification
        try:
            sns_client.publish(
                TopicArn=ALERT_TOPIC_ARN,
                Message=json.dumps({
                    'source': 'api_ingestion',
                    'error': error_message,
                    'timestamp': datetime.now().isoformat(),
                    'source_name': source_name if source_name else 'unknown'
                }),
                Subject=f"ERROR: API ingestion failed for {source_name if source_name else 'unknown'}"
            )
        except Exception as sns_error:
            logger.error(f"Failed to send error notification: {str(sns_error)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        } 