from common.pipeline_logger import PipelineLogger
from constants.aws_constants import AWSConstants
from exceptions.pipeline_exceptions import InputDataException
from glue_etl.configuration.glue_config import PipelineConfig
import boto3

class SourceReadinessService:
    """Verifies that the _READY marker exists before ETL starts."""

    def __init__(self, config: PipelineConfig, logger: PipelineLogger):
        self.config = config
        self.logger = logger
        self.s3 = boto3.client("s3", region_name=config.aws_region)

    def assert_ready(self) -> None:
        key = (
            f"{AWSConstants.RAW_PREFIX}/{self.config.input_date}/"
            f"{AWSConstants.READY_FILE}"
        )
        try:
            self.s3.head_object(
                Bucket=self.config.raw_bucket,
                Key=key,
            )
            self.logger.info(
                "READY marker verified",
                bucket=self.config.raw_bucket,
                key=key,
            )
        except Exception as exc:
            raise InputDataException(
                f"READY marker not found: s3://{self.config.raw_bucket}/{key}"
            ) from exc