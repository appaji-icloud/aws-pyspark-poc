from common.pipeline_logger import PipelineLogger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from exceptions.pipeline_exceptions import DataQualityException


class DataQualityAuditor:
    """Runs referential-integrity and metric checks."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def check_dimension_keys(
            self,
            fact_df: DataFrame,
            column: str,
            dimension_df: DataFrame,
            dimension_key: str,
    ) -> None:
        missing = (
            fact_df
            .filter(F.col(column).isNull())
            .count()
        )

        if missing:
            self.logger.warning(
                "Fact contains missing dimension keys",
                fact_column=column,
                missing=missing,
                dimension_key=dimension_key,
            )

    def check_metrics(self, fact_df: DataFrame) -> None:
        negative_watch = (
            fact_df
            .filter(F.col("watch_seconds") < 0)
            .count()
        )

        invalid_completion = (
            fact_df
            .filter(
                (F.col("completion_percentage") < 0)
                | (F.col("completion_percentage") > 100)
            )
            .count()
        )

        if negative_watch:
            raise DataQualityException(
                f"Negative watch_seconds detected: {negative_watch}"
            )

        if invalid_completion:
            raise DataQualityException(
                "completion_percentage outside 0-100 range: "
                f"{invalid_completion}"
            )

        self.logger.info(
            "Fact metric quality checks passed",
            negative_watch=negative_watch,
            invalid_completion=invalid_completion,
        )

    def audit(
            self,
            fact_df: DataFrame,
            user_df: DataFrame,
            video_df: DataFrame,
            channel_df: DataFrame,
            device_df: DataFrame,
            date_df: DataFrame,
    ) -> None:
        self.check_dimension_keys(
            fact_df, "user_key", user_df, "user_key"
        )
        self.check_dimension_keys(
            fact_df, "video_key", video_df, "video_key"
        )
        self.check_dimension_keys(
            fact_df, "channel_key", channel_df, "channel_key"
        )
        self.check_dimension_keys(
            fact_df, "device_key", device_df, "device_key"
        )
        self.check_dimension_keys(
            fact_df, "date_key", date_df, "date_key"
        )
        self.check_metrics(fact_df)

        self.logger.info("Data quality audit completed")

class ReferentialIntegrityService:
    """Performs explicit dimension-to-fact integrity checks."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def check(
            self,
            fact_df: DataFrame,
            dimension_df: DataFrame,
            fact_key: str,
            dimension_key: str,
            name: str,
    ) -> None:
        dimension_keys = dimension_df.select(
            F.col(dimension_key).alias("_dimension_key")
        ).dropDuplicates()

        orphan_count = (
            fact_df
            .select(F.col(fact_key).alias("_fact_key"))
            .filter(F.col("_fact_key").isNotNull())
            .join(
                dimension_keys,
                F.col("_fact_key") == F.col("_dimension_key"),
                "left_anti",
                )
            .count()
        )

        if orphan_count:
            self.logger.warning(
                "Orphan fact keys detected",
                dimension=name,
                orphan_count=orphan_count,
            )
        else:
            self.logger.info(
                "Referential integrity passed",
                dimension=name,
            )
