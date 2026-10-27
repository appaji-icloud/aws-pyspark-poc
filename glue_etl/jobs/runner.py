from typing import Optional, Type

from pyspark.sql import SparkSession

from common.pipeline_logger import PipelineLogger
from constants.aws_constants import AWSConstants
from exceptions.pipeline_exceptions import (
    ConfigurationException,
    YouTubePipelineException,
)
from glue_etl.configuration.glue_config import (
    ConfigurationLoader,
    PipelineConfig,
)
from glue_etl.configuration.spark_session_factory import SparkSessionFactory
from glue_etl.pipeline_orchestrator import PipelineOrchestrator


class GlueJobApplication:
    """Shared Glue lifecycle used by the three independent job scripts."""

    def __init__(
            self,
            orchestrator_class: Type[PipelineOrchestrator],
            default_job_name: str,
    ) -> None:
        self.logger = PipelineLogger()
        self.orchestrator_class = orchestrator_class
        self.default_job_name = default_job_name

        self.config: Optional[PipelineConfig] = None
        self.spark: Optional[SparkSession] = None
        self.glue_context = None
        self.job = None

    def initialize(self) -> None:
        loader = ConfigurationLoader(self.logger)
        self.config = loader.load(default_job_name=self.default_job_name)

        self.spark, self.glue_context = SparkSessionFactory(
            self.logger
        ).create()

        try:
            from awsglue.job import Job
        except ImportError as exc:
            raise ConfigurationException(
                "This entry point requires the AWS Glue runtime."
            ) from exc

        self.job = Job(self.glue_context)
        self.job.init(
            self.config.job_name,
            {
                "JOB_NAME": self.config.job_name,
                "INPUT_DATE": self.config.input_date,
                "RAW_BUCKET": self.config.raw_bucket,
                "PROCESSED_BUCKET": self.config.processed_bucket,
                "CURATED_BUCKET": self.config.curated_bucket,
                "AWS_REGION": self.config.aws_region,
                "ENVIRONMENT": self.config.environment,
                "DATA_QUALITY_THRESHOLD": str(
                    self.config.data_quality_threshold
                ),
                "ENABLE_XLSX": str(self.config.enable_xlsx).lower(),
                "WRITE_MODE": self.config.write_mode,
                "SNS_TOPIC_ARN": self.config.sns_topic_arn or "",
            },
        )
        self.logger.info(
            "Glue job initialized",
            job_name=self.config.job_name,
            input_date=self.config.input_date,
        )

    def execute(self) -> None:
        if self.config is None or self.spark is None:
            raise ConfigurationException(
                "Glue job has not been initialized."
            )

        self.orchestrator_class(
            self.spark,
            self.config,
            self.logger,
        ).run()

    def commit(self) -> None:
        if self.job is None:
            raise ConfigurationException(
                "Glue job has not been initialized."
            )
        self.job.commit()
        self.logger.info("Glue job committed successfully")

    def run(self) -> None:
        try:
            self.initialize()
            self.execute()
            self.commit()
        except YouTubePipelineException:
            raise
        except Exception as exc:
            self.logger.exception(
                "Unhandled Glue job exception",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise


def run_glue_job(
        orchestrator_class: Type[PipelineOrchestrator],
        default_job_name: str,
) -> None:
    GlueJobApplication(
        orchestrator_class=orchestrator_class,
        default_job_name=default_job_name,
    ).run()
