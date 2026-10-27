
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

from constants.aws_constants import AWSConstants


logger = logging.getLogger()
logger.setLevel(logging.INFO)


# --------------------------------------------------
# Custom exceptions
# --------------------------------------------------

class LambdaConfigurationException(Exception):
    """Raised when Lambda configuration is invalid."""


class S3ReadyEventException(Exception):
    """Raised when an EventBridge event is invalid."""


class GlueJobTriggerException(Exception):
    """Raised when the Glue job cannot be started."""


# --------------------------------------------------
# Configuration
# --------------------------------------------------

@dataclass(frozen=True)
class LambdaConfiguration:
    glue_job_name: str
    raw_bucket: str
    processed_bucket: str
    curated_bucket: str
    aws_region: str
    environment: str
    sns_topic_arn: str = ""
    data_quality_threshold: str = ""
    enable_xlsx: str = ""
    write_mode: str = ""
    glue_workflow_name: str = ""

    @classmethod
    def from_environment(cls):
        """Load configuration from Lambda environment variables."""

        required_variables = [
            "RAW_BUCKET",
            "PROCESSED_BUCKET",
            "CURATED_BUCKET",
        ]

        missing = [
            name for name in required_variables
            if not os.environ.get(name)
        ]

        if missing:
            raise LambdaConfigurationException(
                f"Missing environment variables: {missing}"
            )

        glue_job_name = os.environ.get("GLUE_JOB_NAME", "")
        glue_workflow_name = os.environ.get("GLUE_WORKFLOW_NAME", "")
        if not glue_job_name and not glue_workflow_name:
            raise LambdaConfigurationException(
                "Set GLUE_JOB_NAME for direct job starts or "
                "GLUE_WORKFLOW_NAME for a three-stage workflow."
            )

        return cls(
            glue_job_name=glue_job_name,
            raw_bucket=os.environ["RAW_BUCKET"],
            processed_bucket=os.environ["PROCESSED_BUCKET"],
            curated_bucket=os.environ["CURATED_BUCKET"],
            aws_region=os.environ.get(
                "AWS_REGION", AWSConstants.REGION
            ),
            environment=os.environ.get(
                "ENVIRONMENT", AWSConstants.DEFAULT_ENVIRONMENT
            ),
            sns_topic_arn=os.environ.get(
                "SNS_TOPIC_ARN", ""
            ),
            data_quality_threshold=os.environ.get(
                "DATA_QUALITY_THRESHOLD", ""
            ),
            enable_xlsx=os.environ.get("ENABLE_XLSX", ""),
            write_mode=os.environ.get("WRITE_MODE", ""),
            glue_workflow_name=glue_workflow_name,
        )


# --------------------------------------------------
# EventBridge event parser
# --------------------------------------------------

class S3ReadyEventParser:
    """Validates the S3 event and extracts its batch date."""

    def __init__(self, expected_bucket: str):
        self.expected_bucket = expected_bucket

    def extract_input_date(self, event: Dict[str, Any]) -> str:
        detail = event.get("detail", {})

        bucket_name = detail.get(
            "bucket", {}
        ).get("name")

        object_key = detail.get(
            "object", {}
        ).get("key", "")

        if bucket_name != self.expected_bucket:
            raise S3ReadyEventException(
                f"Unexpected bucket: {bucket_name}"
            )

        pattern = (
            rf"{re.escape(AWSConstants.RAW_PREFIX)}/"
            r"(\d{4}-\d{2}-\d{2})/"
            rf"{re.escape(AWSConstants.READY_FILE)}"
        )
        match = re.fullmatch(pattern, object_key)

        if not match:
            raise S3ReadyEventException(
                f"Invalid _READY object key: {object_key}"
            )

        input_date = match.group(1)

        try:
            datetime.strptime(input_date, "%Y-%m-%d")
        except ValueError as exc:
            raise S3ReadyEventException(
                f"Invalid batch date: {input_date}"
            ) from exc

        return input_date


# --------------------------------------------------
# Glue job trigger
# --------------------------------------------------

class GlueJobTrigger:
    """Triggers the configured AWS Glue ETL job."""

    def __init__(
            self,
            config: LambdaConfiguration,
            glue_client=None,
    ):
        self.config = config

        self.glue_client = glue_client or boto3.client(
            "glue",
            region_name=config.aws_region,
        )

    def build_job_arguments(self, input_date: str) -> Dict[str, str]:
        """Build the parameters passed to the Glue job."""

        arguments = {
            "--INPUT_DATE": input_date,
            "--RAW_BUCKET": self.config.raw_bucket,
            "--PROCESSED_BUCKET": self.config.processed_bucket,
            "--CURATED_BUCKET": self.config.curated_bucket,
            "--AWS_REGION": self.config.aws_region,
            "--ENVIRONMENT": self.config.environment,
        }

        if self.config.sns_topic_arn:
            arguments["--SNS_TOPIC_ARN"] = (
                self.config.sns_topic_arn
            )

        optional_arguments = {
            "--DATA_QUALITY_THRESHOLD": (
                self.config.data_quality_threshold
            ),
            "--ENABLE_XLSX": self.config.enable_xlsx,
            "--WRITE_MODE": self.config.write_mode,
        }
        arguments.update({
            name: value
            for name, value in optional_arguments.items()
            if value
        })

        return arguments

    def trigger_glue_job(self, input_date: str) -> Dict[str, str]:
        """Start the Glue job and return its execution details."""

        if self.config.glue_workflow_name:
            try:
                workflow_properties = {
                    "INPUT_DATE": input_date,
                    "RAW_BUCKET": self.config.raw_bucket,
                    "PROCESSED_BUCKET": self.config.processed_bucket,
                    "CURATED_BUCKET": self.config.curated_bucket,
                    "AWS_REGION": self.config.aws_region,
                    "ENVIRONMENT": self.config.environment,
                }
                optional_properties = {
                    "SNS_TOPIC_ARN": self.config.sns_topic_arn,
                    "DATA_QUALITY_THRESHOLD": (
                        self.config.data_quality_threshold
                    ),
                    "ENABLE_XLSX": self.config.enable_xlsx,
                    "WRITE_MODE": self.config.write_mode,
                }
                workflow_properties.update({
                    key: value
                    for key, value in optional_properties.items()
                    if value
                })
                response = self.glue_client.start_workflow_run(
                    Name=self.config.glue_workflow_name,
                    RunProperties=workflow_properties,
                )
                workflow_run_id = response["RunId"]
                logger.info(
                    "Glue workflow started successfully. "
                    "WorkflowName=%s, WorkflowRunId=%s, InputDate=%s",
                    self.config.glue_workflow_name,
                    workflow_run_id,
                    input_date,
                )
                return {
                    "workflow_name": self.config.glue_workflow_name,
                    "workflow_run_id": workflow_run_id,
                    "input_date": input_date,
                    "status": "STARTED",
                }
            except (ClientError, KeyError) as exc:
                logger.exception("Failed to trigger Glue workflow")
                raise GlueJobTriggerException(
                    "Failed to start the AWS Glue workflow."
                ) from exc

        arguments = self.build_job_arguments(input_date)

        logger.info(
            "Triggering Glue job: %s",
            self.config.glue_job_name,
        )

        logger.info("Glue job arguments: %s", arguments)

        try:
            response = self.glue_client.start_job_run(
                JobName=self.config.glue_job_name,
                Arguments=arguments,
            )

            job_run_id = response["JobRunId"]

            logger.info(
                "Glue job started successfully. "
                "JobName=%s, JobRunId=%s, InputDate=%s",
                self.config.glue_job_name,
                job_run_id,
                input_date,
            )

            return {
                "job_name": self.config.glue_job_name,
                "job_run_id": job_run_id,
                "input_date": input_date,
                "status": "STARTED",
            }

        except (ClientError, KeyError) as exc:
            logger.exception("Failed to trigger Glue job")

            raise GlueJobTriggerException(
                "Failed to start the AWS Glue job."
            ) from exc


# --------------------------------------------------
# Lambda application orchestrator
# --------------------------------------------------

class LambdaHandler:
    """Coordinates event validation and Glue execution."""

    def __init__(
            self,
            config: LambdaConfiguration,
            event_parser: S3ReadyEventParser,
            glue_trigger: GlueJobTrigger,
    ):
        self.config = config
        self.event_parser = event_parser
        self.glue_trigger = glue_trigger

    def handle(self, event: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(
            "Received EventBridge event: %s",
            json.dumps(event),
        )

        input_date = self.event_parser.extract_input_date(
            event
        )

        result = self.glue_trigger.trigger_glue_job(input_date)

        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }


# --------------------------------------------------
# AWS Lambda entry point
# --------------------------------------------------

def lambda_handler(event, context):
    """AWS invokes this function for each Lambda invocation."""

    config = LambdaConfiguration.from_environment()

    event_parser = S3ReadyEventParser(
        expected_bucket=config.raw_bucket
    )

    glue_trigger = GlueJobTrigger(config)

    application = LambdaHandler(
        config=config,
        event_parser=event_parser,
        glue_trigger=glue_trigger,
    )

    return application.handle(event)
