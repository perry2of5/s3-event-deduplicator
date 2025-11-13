import boto3
from datetime import datetime, timedelta
import json
import logging
import os
import uuid

from aws_lambda_powertools.utilities.data_classes import SQSEvent, SQSRecord
from aws_lambda_powertools.utilities.data_classes import S3Event
from aws_lambda_powertools.utilities.typing import LambdaContext
from enum import Enum
from botocore.exceptions import ClientError as BotocoreClientError
from mypy_boto3_sns import SNSClient
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_dynamodb.service_resource import Table
from mypy_boto3_dynamodb.type_defs import GetItemOutputTableTypeDef
from typing import Any, Dict, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# a unique id for this lambda invocation to use in lock records
lambda_id = uuid.uuid4().hex


_TOPIC_CONFIG = {
    "sns_client": boto3.client('sns'),
    "topic_arn": os.environ.get('SNS_TOPIC_ARN', 's3_create_topic.fifo')
}

_DYNAMODB_CONFIG = {
    "dynamodb_client": boto3.client('dynamodb'),
    "table_name": os.environ.get('DYNAMODB_TABLE', 's3_event_log_table')
}


class LambdaSNSClass:
    def __init__(self, topic_config: Dict[str, Any]):
        self.client: SNSClient = topic_config['sns_client']
        self.topic_arn: str = topic_config['topic_arn']

class LambdaDyanamoDBClass:
    def __init__(self, table_config: Dict[str, Any]):
        self.client: DynamoDBClient = table_config['dynamodb_client']
        self.table_name: str = table_config['table_name']
        self.table: Table = self.client.Table(self.table_name)


class LockStatus(Enum):
    PENDING_STATUS = "pending"
    FAILED_STATUS = "failed"
    COMPLETED_STATUS = "completed"

class LockOutcome(Enum):
    LOCK_ACQUIRED = "lock_acquired"
    ALREADY_COMPLETE = "already_complete"
    ALREADY_LOCKED = "already_locked"


logger.debug("Will use dynamodb %s to check for duplicates and forward originals to sns %s",
        _DYNAMODB_CONFIG['table_name'], _TOPIC_CONFIG['topic_arn'])

def handler(event: SQSEvent, context: LambdaContext):
    """Handler for AWS Lambda to call"""
    global _DYNAMODB_TABLE, _TOPIC_CONFIG
    sns = LambdaSNSClass(_TOPIC_CONFIG)
    dynamodb = LambdaDyanamoDBClass(_DYNAMODB_CONFIG)
    handle(event, sns, dynamodb)

def handle(event: SQSEvent, sns: LambdaSNSClass, dynamodb: LambdaDyanamoDBClass) -> Dict[str, Any]:
    """Handler with dependencies injected for easier testing"""
    if event.get('Service', '') == 'Amazon S3' and event.get('Event', '') == 's3:TestEvent':
        logger.debug("Skipping S3 test event %s", event)
        return {
            'statusCode': 200,
            'body': 'Object processed successfully'
        }

    exception: Exception | None = None
    for sqs_record in event.records:
        try:
            process_sqs_record(sqs_record, sns, dynamodb)
        except Exception as e:
            logger.error("Error Processing S3 event %s", event)
            logger.error("Error processing S3 event: %s", e, exc_info=True)
            exception = e
    if exception:
        # Fail with last exception, others get logged. This provides best effort to process all
        # records as quickly as possible.
        return {
            'statusCode': 400,
            'body': 'Object processing failed: ' + str(exception)
        }

    return {
      'statusCode': 200,
      'body': 'Object processed successfully'
    }


def process_sqs_record(sqs_record: SQSRecord, sns: LambdaSNSClass, dynamodb: LambdaDyanamoDBClass):
    exception: Exception | None = None
    try:
        sqs_record_body = sqs_record.json_body
        s3_records: list[S3Event] = sqs_record_body.get('Records', [])
        logger.debug("Found %d S3 records in SQS record", len(s3_records))
        for s3_record in s3_records:
            process_file_event(s3_record, sns, dynamodb)
    except Exception as e:
        logger.error("Processing SQS record %s", sqs_record)
        logger.error("Error processing SQS record: %s", e, exc_info=True)
        exception = e
    if exception:
        raise exception


def process_file_event(s3_event: S3Event, sns: LambdaSNSClass, dynamodb: LambdaDyanamoDBClass):
    file_start: datetime = datetime.now()
    expiration = file_start + timedelta(days=7)
    lock_expiration = file_start + timedelta(milliseconds=300)
    try:
        input_full_key = calc_s3_object_key(s3_event)
        message_group = calc_group_id(s3_event)
        dynamodb.table.put_item(
            Item={
                's3_event_id': input_full_key,
                'status': LockStatus.PENDING_STATUS.value,
                'lock_owner_id': lambda_id,
                'locked_until': lock_expiration.isoformat(),
                'record_expiration': expiration.isoformat(),
            },
            ConditionExpression=('attribute_not_exists(s3_event_id)'
                    + 'OR '
                    + f'status = :failed_status'
            ),
            ExpressionAttributeValues={
                    ':s3_event_id': input_full_key,
                    ':failed_status': LockStatus.FAILED_STATUS.value,
                    }
            )
        logger.debug("Wrote to dynamoDB %s %s", dynamodb.table_name, input_full_key)
        logger.debug("Sending to SNS %s %s", sns.topic_arn, input_full_key, extra={"message_group": message_group})
        publish_response = sns.client.publish(
            TopicArn=sns.topic_arn,
            Message=json.dumps(s3_event),
            MessageDeduplicationId=input_full_key,
            MessageGroupId=message_group,
        )
        logger.debug("Sent to SNS %s %s: %s", sns.topic_arn, input_full_key, publish_response)
        duration = (datetime.now() - file_start).total_seconds()
        eTag = s3_event['s3']['object']['eTag']
        logger.info("Processed S3 event %s with etag %s in %s seconds", input_full_key, eTag, duration)
    except Exception as e:
        logger.error("Processing S3 record %s", s3_event)
        logger.error("Error processing S3 event: %s", e, exc_info=True)

def calc_lock_expiration() -> Tuple[datetime, datetime]:
    """Calculate lock expiration and record expiration for DynamoDB TTL."""
    now: datetime = datetime.now()
    lock_expiration = now + timedelta(milliseconds=300)
    record_expiration = now + timedelta(days=7) # for DynamoDB TTL
    return lock_expiration, record_expiration

def lock_message(
    s3_event_id: str,
    dynamodb: LambdaDyanamoDBClass,
) -> Tuple[LockOutcome, datetime]:
    """Attempt to acquire a lock for processing the given s3_event_id. Return LockOutcome and the datetime the lock expires."""
    # LOCK_ACQUIRED = "lock_acquired"
    # ALREADY_COMPLETE = "already_complete"
    # ALREADY_LOCKED = "already_locked"
    # if not known or known and previously failed, create new lock.
    try:
        lock_expiration, record_expiration = calc_lock_expiration()
        dynamodb.table.put_item(
            Item={
                's3_event_id': s3_event_id,
                'status': LockStatus.PENDING_STATUS.value,
                'lock_owner_id': lambda_id,
                'locked_until': lock_expiration.isoformat(),
                'record_expiration': record_expiration.isoformat(),
            },
            ConditionExpression=(
                f'attribute_not_exists(s3_event_id) '
                + 'OR '
                + f'(status = :failed_status AND s3_event_id = :s3_event_id)'
            ),
            ExpressionAttributeValues={
                ':s3_event_id': s3_event_id,
                ':failed_status': LockStatus.FAILED_STATUS.value,
            }
        )
        return LockOutcome.LOCK_ACQUIRED, lock_expiration
    except BotocoreClientError as e:
        if not (e.response['Error']['Code'] == 'ConditionalCheckFailedException'):
            raise

    return handle_existing_lock(s3_event_id, dynamodb)

def handle_existing_lock(
    s3_event_id: str,
    dynamodb: LambdaDyanamoDBClass,
    attempt_number: int = 0,
) -> Tuple[LockOutcome, datetime]:
    if (attempt_number > 5):
        raise Exception(f"Exceeded max attempts to acquire lock for {s3_event_id}")
    # lock not raised, either another thread is processing or already completed.
    existing_lock_qry_result: GetItemOutputTableTypeDef = dynamodb.table.get_item(
            Key={'s3_event_id': s3_event_id},
            ConsistentRead=True)
    item = existing_lock_qry_result.get('Item')
    if item is not None:
        locked_until = datetime.fromisoformat(str(item.get('locked_until')))
        if item.get('status') == LockStatus.COMPLETED_STATUS.value:
            return LockOutcome.ALREADY_COMPLETE, locked_until
        elif item.get('status') == LockStatus.FAILED_STATUS.value:
            raise Exception(f"Previous processing of {s3_event_id} failed. Logic error above. Please fix.")
        elif item.get('status') == LockStatus.PENDING_STATUS.value:
            if locked_until < datetime.now():
                logger.warning("Lock on %s expired at %s. Transferring to current lambda", s3_event_id, item.get('locked_until'))
                return transfer_lock(s3_event_id, dynamodb, str(item.get("lock_owner_id")), locked_until)
            else:
                logger.warning("Lock on %s not expired, waiting until %s.", s3_event_id, item.get('locked_until'))
                now = datetime.now()
                if locked_until > now:
                    sleep_time = (locked_until - now).total_seconds() + 0.001
                    import time
                    time.sleep(sleep_time)
                    return handle_existing_lock(s3_event_id, dynamodb, attempt_number + 1)
    raise Exception(f"Could not find lock record for {s3_event_id} after failed put and get.")

def transfer_lock(
    s3_event_id: str,
    dynamodb: LambdaDyanamoDBClass,
    previous_lock_owner_id: str,
    previous_lock_expiration: datetime,
) -> Tuple[LockOutcome, datetime]:
    """Update the lock to the current lambda's ID and set a new expiration if the
    previous owner's lock has expired and the lock ownership and expiration are
    unchanged."""
    try:
        lock_expiration, _ = calc_lock_expiration()
        dynamodb.table.update_item(
            Key={'s3_event_id': s3_event_id},
            UpdateExpression=('SET '
                'lock_owner_id = :lambda_id, ' +
                'record_expiration = :new_record_expiration' +
                'locked_until = :new_locked_until, '
            ),
            ConditionExpression=(
                's3_event_id = :s3_event_id AND ' +
                'lock_owner_id = :previous_lock_owner_id AND ' +
                'locked_until = :previous_locked_until AND ' +
                'status = :pending_status AND ' +
                'locked_until < :now'
            ),
            ExpressionAttributeValues={
                ':lambda_id': lambda_id,
                ':new_record_expiration': lock_expiration.isoformat(),
                ':new_locked_until': lock_expiration.isoformat(),
                ':previous_lock_owner_id': previous_lock_owner_id,
                ":previous_locked_until": previous_lock_expiration.isoformat(),
                ':pending_status': LockStatus.PENDING_STATUS.value,
                ':now': datetime.now().isoformat(),
            }
        )
        return LockOutcome.LOCK_ACQUIRED, lock_expiration
    except BotocoreClientError as e:
        if not (e.response['Error']['Code'] == 'ConditionalCheckFailedException'):
            raise
        return LockOutcome.ALREADY_LOCKED, previous_lock_expiration


def calc_group_id(record: S3Event) -> str:
    input_bucket = record['s3']['bucket']['name']
    input_key = record['s3']['object']['key']
    return f"{input_bucket}/{input_key}"


def calc_s3_object_key(record: S3Event) -> str:
    input_bucket = record['s3']['bucket']['name']
    input_key = record['s3']['object']['key']
    input_size = record['s3']['object']['size']
    input_version_id = record['s3']['object'].get('version-id')
    version_suffix = (input_version_id or '')
    input_sequencer = record['s3']['object']['sequencer']
    input_full_key = f"{input_bucket}/{input_key}#{input_sequencer}#{version_suffix}#{input_size}"
    return input_full_key
