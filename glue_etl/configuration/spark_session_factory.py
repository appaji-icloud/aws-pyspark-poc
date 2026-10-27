
from typing import Tuple

from pyspark.sql import SparkSession

from common.pipeline_logger import PipelineLogger
from constants.aws_constants import AWSConstants
from exceptions.pipeline_exceptions import ConfigurationException


class SparkSessionFactory:
    """Create a Spark session from the AWS Glue-managed runtime."""

    def __init__(self, logger: PipelineLogger) -> None:
        self.logger = logger

    @staticmethod
    def _configure_spark(spark: SparkSession) -> None:
        """Apply common Spark SQL settings."""

        for key, value in AWSConstants.SPARK_SETTINGS.items():
            spark.conf.set(key, value)

    def create(self) -> Tuple[SparkSession, object]:
        """Return the Spark session and Glue context for this job."""

        try:
            from awsglue.context import GlueContext

        except ImportError as exc:
            raise ConfigurationException(
                "This entry point requires the AWS Glue runtime."
            ) from exc

        try:
            spark_context = SparkSession.builder.getOrCreate().sparkContext
            glue_context = GlueContext(spark_context)
            spark = glue_context.spark_session
            self._configure_spark(spark)

            self.logger.info(
                "Spark runtime initialized",
                app_id=spark.sparkContext.applicationId,
                spark_version=spark.version,
            )

            return spark, glue_context

        except ConfigurationException:
            raise

        except Exception as exc:
            raise ConfigurationException(
                f"Unable to initialize Spark runtime: {exc}"
            ) from exc
