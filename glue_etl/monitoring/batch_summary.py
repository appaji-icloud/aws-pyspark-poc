from common.pipeline_logger import PipelineLogger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class BatchSummaryService:
    """Logs useful business summaries for every batch."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def summarize(self, df: DataFrame) -> None:
        summary = (
            df.agg(
                F.count("*").alias("events"),
                F.sum("watch_seconds").alias("watch_seconds"),
                F.avg("completion_percentage").alias(
                    "avg_completion_percentage"
                ),
                F.sum("engagement_count").alias(
                    "engagement_count"
                ),
            )
            .collect()[0]
        )

        self.logger.info(
            "Business batch summary",
            events=summary["events"],
            watch_seconds=summary["watch_seconds"],
            avg_completion_percentage=summary[
                "avg_completion_percentage"
            ],
            engagement_count=summary["engagement_count"],
        )