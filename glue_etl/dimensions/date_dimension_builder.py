from pyspark.sql import functions as F
from common.pipeline_logger import PipelineLogger
from pyspark.sql import DataFrame

from exceptions.pipeline_exceptions import DimensionBuildException


class DateDimensionBuilder:
    """Builds dim_date for the dates present in the current batch."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def build(self, df: DataFrame) -> DataFrame:
        try:
            result = (
                df.select(
                    F.col("event_date").alias("date"),
                )
                .dropDuplicates(["date"])
                .withColumn(
                    "date_key",
                    F.date_format(F.col("date"), "yyyyMMdd").cast("int"),
                )
                .withColumn(
                    "year",
                    F.year("date"),
                )
                .withColumn(
                    "month",
                    F.month("date"),
                )
                .withColumn(
                    "day",
                    F.dayofmonth("date"),
                )
                .withColumn(
                    "quarter",
                    F.quarter("date"),
                )
                .withColumn(
                    "day_of_week",
                    F.dayofweek("date"),
                )
                .withColumn(
                    "week_of_year",
                    F.weekofyear("date"),
                )
            )

            result = result.select(
                "date_key",
                "date",
                "year",
                "month",
                "day",
                "quarter",
                "day_of_week",
                "week_of_year",
            )

            self.logger.info(
                "dim_date built",
                count=result.count(),
            )
            return result

        except Exception as exc:
            raise DimensionBuildException(
                f"dim_date failed: {exc}"
            ) from exc