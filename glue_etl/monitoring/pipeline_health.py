from common.pipeline_logger import PipelineLogger
from exceptions.pipeline_exceptions import EmptyDataException
from pyspark.sql import DataFrame

class PipelineHealthService:
    """Central health gate used before successful completion."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def assert_non_empty(
            self,
            df: DataFrame,
            dataset_name: str,
    ) -> None:
        count = df.limit(1).count()
        if count == 0:
            raise EmptyDataException(
                f"Required dataset is empty: {dataset_name}"
            )

        self.logger.info(
            "Dataset health check passed",
            dataset=dataset_name,
        )
