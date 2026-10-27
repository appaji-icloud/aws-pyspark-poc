from common.pipeline_logger import PipelineLogger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class BatchProfiler:
    """Produces a lightweight profile for CloudWatch logs."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def profile(self, df: DataFrame, label: str) -> None:
        try:
            row_count = df.count()
            nulls = {}

            for column in df.columns:
                null_count = (
                    df.filter(F.col(column).isNull()).count()
                )
                if null_count:
                    nulls[column] = null_count

            self.logger.info(
                "Dataset profile",
                label=label,
                row_count=row_count,
                column_count=len(df.columns),
                null_columns=nulls,
            )
        except Exception as exc:
            self.logger.warning(
                "Dataset profiling failed",
                label=label,
                error=str(exc),
            )