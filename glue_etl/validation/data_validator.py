from common.pipeline_logger import PipelineLogger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from exceptions.pipeline_exceptions import DataQualityException, EmptyDataException
from typing import Tuple

class DataValidator:
    """Performs row-level and batch-level validation."""

    REQUIRED_COLUMNS = [
        "event_id",
        "event_timestamp",
        "event_date",
        "user_id",
        "video_id",
        "channel_id",
        "watch_seconds",
        "video_duration_seconds",
    ]

    def __init__(
            self,
            logger: PipelineLogger,
            threshold: float,
    ):
        self.logger = logger
        self.threshold = threshold

    def validate_schema(self, df: DataFrame) -> None:
        missing = [
            c for c in self.REQUIRED_COLUMNS
            if c not in df.columns
        ]

        if missing:
            raise DataQualityException(
                f"Missing required columns: {missing}"
            )

        self.logger.info(
            "Schema validation passed",
            required_columns=len(self.REQUIRED_COLUMNS),
        )

    def split_valid_invalid(
            self,
            df: DataFrame,
    ) -> Tuple[DataFrame, DataFrame]:
        self.validate_schema(df)

        invalid_condition = (
                F.col("event_id").isNull()
                | (F.length(F.trim(F.col("event_id"))) == 0)
                | F.col("event_timestamp").isNull()
                | F.col("event_date").isNull()
                | F.col("user_id").isNull()
                | F.col("video_id").isNull()
                | F.col("channel_id").isNull()
                | F.col("watch_seconds").isNull()
                | (F.col("watch_seconds") < 0)
                | F.col("video_duration_seconds").isNull()
                | (F.col("video_duration_seconds") <= 0)
        )

        invalid_df = (
            df
            .filter(invalid_condition)
            .withColumn(
                "dq_reason",
                F.when(F.col("event_id").isNull(), "MISSING_EVENT_ID")
                .when(F.col("event_timestamp").isNull(), "INVALID_TIMESTAMP")
                .when(F.col("user_id").isNull(), "MISSING_USER_ID")
                .when(F.col("video_id").isNull(), "MISSING_VIDEO_ID")
                .when(F.col("channel_id").isNull(), "MISSING_CHANNEL_ID")
                .when(F.col("watch_seconds").isNull(), "MISSING_WATCH_SECONDS")
                .when(F.col("watch_seconds") < 0, "NEGATIVE_WATCH_SECONDS")
                .when(
                    F.col("video_duration_seconds").isNull(),
                    "MISSING_VIDEO_DURATION",
                )
                .when(
                    F.col("video_duration_seconds") <= 0,
                    "INVALID_VIDEO_DURATION",
                    )
                .otherwise("UNKNOWN")
            )
        )

        valid_df = df.filter(~invalid_condition)

        total = df.count()
        invalid = invalid_df.count()
        valid = valid_df.count()

        ratio = valid / total if total else 0

        self.logger.info(
            "Data quality validation completed",
            total=total,
            valid=valid,
            invalid=invalid,
            valid_ratio=round(ratio, 6),
            threshold=self.threshold,
        )

        if total == 0:
            raise EmptyDataException("Input contains zero records")

        if ratio < self.threshold:
            raise DataQualityException(
                f"Valid ratio {ratio:.4f} is below threshold "
                f"{self.threshold:.4f}"
            )

        return valid_df, invalid_df