from common.pipeline_logger import PipelineLogger
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from exceptions.pipeline_exceptions import TransformationException


class DeduplicationService:
    """Removes duplicate business events."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def deduplicate(self, df: DataFrame) -> DataFrame:
        try:
            before = df.count()

            window = (
                Window
                .partitionBy("event_id")
                .orderBy(
                    F.col("event_timestamp").desc_nulls_last(),
                    F.when(
                        F.col("source_format") == "xlsx", 3
                    ).when(
                        F.col("source_format") == "json", 2
                    ).otherwise(1).desc(),
                )
            )

            result = (
                df
                .withColumn("_rn", F.row_number().over(window))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            after = result.count()

            self.logger.info(
                "Deduplication completed",
                before=before,
                after=after,
                duplicates_removed=before - after,
            )

            return result

        except Exception as exc:
            raise TransformationException(
                f"Deduplication failed: {exc}"
            ) from exc
