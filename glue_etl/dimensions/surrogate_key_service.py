from common.pipeline_logger import PipelineLogger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List
class SurrogateKeyService:
    """Creates deterministic surrogate keys using xxhash64."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def add_event_key(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "event_key",
            F.xxhash64(
                F.col("event_id"),
            ),
        )

    def add_dimension_key(
            self,
            df: DataFrame,
            source_column: str,
            target_column: str,
    ) -> DataFrame:
        return df.withColumn(
            target_column,
            F.xxhash64(F.col(source_column)),
        )

    def add_composite_key(
            self,
            df: DataFrame,
            columns: List[str],
            target_column: str,
    ) -> DataFrame:
        return df.withColumn(
            target_column,
            F.xxhash64(*[F.col(c) for c in columns]),
        )