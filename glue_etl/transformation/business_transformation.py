from common.pipeline_logger import PipelineLogger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from exceptions.pipeline_exceptions import TransformationException


class BusinessTransformationService:
    """Adds business metrics used by analytics."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def calculate_metrics(self, df: DataFrame) -> DataFrame:
        try:
            result = (
                df
                .withColumn(
                    "watch_minutes",
                    F.round(F.col("watch_seconds") / F.lit(60.0), 2),
                )
                .withColumn(
                    "completion_percentage",
                    F.when(
                        F.col("video_duration_seconds") > 0,
                        F.round(
                            (
                                    F.col("watch_seconds")
                                    / F.col("video_duration_seconds")
                            ) * 100,
                            2,
                            ),
                        ).otherwise(F.lit(0.0)),
                )
                .withColumn(
                    "completion_percentage",
                    F.least(
                        F.greatest(
                            F.col("completion_percentage"),
                            F.lit(0.0),
                        ),
                        F.lit(100.0),
                    ),
                )
                .withColumn(
                    "is_completed",
                    F.col("completion_percentage") >= F.lit(90.0),
                    )
                .withColumn(
                    "engagement_count",
                    F.coalesce(F.col("liked").cast("int"), F.lit(0))
                    + F.coalesce(F.col("commented").cast("int"), F.lit(0))
                    + F.coalesce(F.col("shared").cast("int"), F.lit(0))
                    + F.coalesce(F.col("subscribed").cast("int"), F.lit(0)),
                    )
            )

            self.logger.info("Business metrics calculated")
            return result

        except Exception as exc:
            raise TransformationException(
                f"Business metric calculation failed: {exc}"
            ) from exc