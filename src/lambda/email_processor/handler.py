import json
import boto3
import os
import email
import tempfile
import uuid
from datetime import datetime
from email.header import decode_header
import logging
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
ssm_client = boto3.client('ssm')

# Constants
RAW_BUCKET = os.environ.get('RAW_BUCKET')
ALERT_TOPIC_ARN = os.environ.get('ALERT_TOPIC_ARN')
CONFIG_PARAM_PATH = os.environ.get('CONFIG_PARAM_PATH', '/data-pipeline/email-sources')

def get_email_source_config(from_address):
    """
    Retrieves configuration for the email source based on sender address
    """
    try:
        # Get configuration from Parameter Store
        response = ssm_client.get_parameters_by_path(
            Path=CONFIG_PARAM_PATH,
            Recursive=True,
            WithDecryption=True
        )
        
        for param in response.get('Parameters', []):
            config = json.loads(param['Value'])
            # Check if the from_address matches any configured sources
            if 'allowed_senders' in config and from_address in config['allowed_senders']:
                return config
        
        # If no matching configuration, use default if available
        default_param = f"{CONFIG_PARAM_PATH}/default"
        try:
            default_config = ssm_client.get_parameter(
                Name=default_param,
                WithDecryption=True
            )
            return json.loads(default_config['Parameter']['Value'])
        except ClientError:
            logger.warning(f"No configuration found for sender: {from_address}")
            return None
            
    except Exception as e:
        logger.error(f"Error retrieving email source configuration: {str(e)}")
        return None

def process_attachment(attachment, filename, source_config, metadata):
    """
    Process attachment and upload to S3
    """
    try:
        # Generate a unique key for the S3 object
        timestamp = datetime.now().strftime("%Y/%m/%d/%H/%M/%S")
        source_name = source_config.get('name', 'unknown')
        unique_id = str(uuid.uuid4())
        
        # Define the S3 key with appropriate partitioning
        s3_key = f"{source_config.get('destination_prefix', 'email-ingestion')}/{source_name}/{timestamp}/{unique_id}_{filename}"
        
        # Create temporary file to store attachment
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(attachment)
            temp_file_path = temp_file.name
        
        # Upload attachment to S3
        s3_client.upload_file(
            temp_file_path, 
            RAW_BUCKET, 
            s3_key,
            ExtraArgs={
                'Metadata': metadata
            }
        )
        
        # Clean up temporary file
        os.unlink(temp_file_path)
        
        logger.info(f"Successfully uploaded attachment to s3://{RAW_BUCKET}/{s3_key}")
        return {
            'bucket': RAW_BUCKET,
            'key': s3_key,
            'size': len(attachment),
            'filename': filename
        }
        
    except Exception as e:
        logger.error(f"Error processing attachment: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    Main Lambda handler for processing email from SES
    """
    try:
        # SES stores the email in S3 and sends a notification
        if 'Records' in event and len(event['Records']) > 0:
            # Get the SES message
            ses_notification = event['Records'][0]['ses']
            message_id = ses_notification['mail']['messageId']
            
            # Get the email object from S3
            s3_object = s3_client.get_object(
                Bucket=RAW_BUCKET,
                Key=f"emails/{message_id}"
            )
            
            # Parse the email
            email_message = email.message_from_bytes(s3_object['Body'].read())
            
            # Extract email metadata
            subject = str(decode_header(email_message['Subject'])[0][0])
            if isinstance(subject, bytes):
                subject = subject.decode()
                
            from_address = email.utils.parseaddr(email_message['From'])[1]
            to_address = email.utils.parseaddr(email_message['To'])[1]
            received_date = email_message['Date']
            
            # Get configuration for this email source
            source_config = get_email_source_config(from_address)
            if not source_config:
                logger.warning(f"No configuration found for email from: {from_address}. Skipping processing.")
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Unknown email source'})
                }
            
            # Prepare metadata for S3 uploads
            metadata = {
                'subject': subject,
                'from': from_address,
                'to': to_address,
                'date': received_date,
                'source_name': source_config.get('name', 'unknown'),
                'message_id': message_id
            }
            
            # Process attachments
            processed_attachments = []
            for part in email_message.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                    
                # Check if this part is an attachment
                if part.get('Content-Disposition') and 'attachment' in part.get('Content-Disposition'):
                    # Get filename, handling potential encoding issues
                    filename = part.get_filename()
                    if decode_header(filename)[0][1] is not None:
                        filename = decode_header(filename)[0][0].decode(decode_header(filename)[0][1])
                    
                    # Check if this attachment type is allowed
                    if 'allowed_extensions' in source_config:
                        file_ext = os.path.splitext(filename)[1].lower()
                        if file_ext not in source_config['allowed_extensions']:
                            logger.warning(f"Attachment type {file_ext} not allowed. Skipping {filename}")
                            continue
                    
                    # Get attachment content
                    attachment_data = part.get_payload(decode=True)
                    
                    # Process and upload attachment
                    result = process_attachment(attachment_data, filename, source_config, metadata)
                    processed_attachments.append(result)
            
            # Send notification about processed email
            if processed_attachments:
                notification = {
                    'source': 'email_processor',
                    'timestamp': datetime.now().isoformat(),
                    'message_id': message_id,
                    'from': from_address,
                    'subject': subject,
                    'attachments': processed_attachments,
                    'source_config': source_config.get('name', 'unknown')
                }
                
                sns_client.publish(
                    TopicArn=ALERT_TOPIC_ARN,
                    Message=json.dumps(notification),
                    Subject=f"Email processed: {subject}"
                )
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': f"Processed {len(processed_attachments)} attachments",
                        'attachments': processed_attachments
                    })
                }
            else:
                logger.info(f"No attachments processed for email: {message_id}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'No attachments to process'})
                }
                
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid event data'})
        }
        
    except Exception as e:
        error_message = f"Error processing email: {str(e)}"
        logger.error(error_message)
        
        # Send alert for processing failure
        try:
            sns_client.publish(
                TopicArn=ALERT_TOPIC_ARN,
                Message=json.dumps({
                    'source': 'email_processor',
                    'error': error_message,
                    'timestamp': datetime.now().isoformat(),
                    'event': event
                }),
                Subject="ERROR: Email Processing Failed"
            )
        except Exception as sns_error:
            logger.error(f"Failed to send error notification: {str(sns_error)}")
            
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        } 