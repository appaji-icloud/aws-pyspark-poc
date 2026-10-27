from common.pipeline_logger import PipelineLogger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from exceptions.pipeline_exceptions import TransformationException


class DataStandardizer:
    """Normalizes types, whitespace and business fields."""

    STRING_COLUMNS = [
        "event_id",
        "user_id",
        "user_name",
        "country",
        "age_group",
        "gender",
        "language",
        "video_id",
        "video_title",
        "category",
        "channel_id",
        "channel_name",
        "device_type",
        "platform",
        "traffic_source",
        "quality",
        "session_id",
        "event_type",
    ]

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def trim_strings(self, df: DataFrame) -> DataFrame:
        result = df
        for column in self.STRING_COLUMNS:
            if column in result.columns:
                result = result.withColumn(
                    column,
                    F.trim(F.col(column)),
                )
        return result

    def normalize_boolean_columns(self, df: DataFrame) -> DataFrame:
        result = df
        for column in [
            "liked",
            "commented",
            "shared",
            "subscribed",
            "is_premium",
        ]:
            result = result.withColumn(
                column,
                F.when(
                    F.lower(F.col(column).cast("string")).isin(
                        "true", "1", "yes", "y"
                    ),
                    F.lit(True),
                )
                .when(
                    F.lower(F.col(column).cast("string")).isin(
                        "false", "0", "no", "n"
                    ),
                    F.lit(False),
                )
                .otherwise(F.col(column).cast("boolean")),
                )
        return result

    def normalize_timestamps(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn(
                "event_timestamp",
                F.to_timestamp(
                    F.col("event_timestamp"),
                    "yyyy-MM-dd HH:mm:ss",
                ),
            )
            .withColumn(
                "event_date",
                F.to_date(F.col("event_timestamp")),
            )
        )

    def normalize_numeric_columns(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn(
                "watch_seconds",
                F.col("watch_seconds").cast("int"),
            )
            .withColumn(
                "video_duration_seconds",
                F.col("video_duration_seconds").cast("int"),
            )
            .withColumn(
                "completion_percentage",
                F.col("completion_percentage").cast("double"),
            )
        )

    def apply(self, df: DataFrame) -> DataFrame:
        try:
            self.logger.info("Starting standardization")

            result = self.trim_strings(df)
            result = self.normalize_boolean_columns(result)
            result = self.normalize_timestamps(result)
            result = self.normalize_numeric_columns(result)

            result = result.withColumn(
                "ingestion_timestamp",
                F.current_timestamp(),
            )

            self.logger.info("Standardization completed")
            return result

        except Exception as exc:
            raise TransformationException(
                f"Standardization failed: {exc}"
            ) from exc