import boto3, re
from typing import Optional, List
from common.pipeline_logger import PipelineLogger
from exceptions.pipeline_exceptions import WriteException
from glue_etl.configuration.glue_config import PipelineConfig
from pyspark.sql import SparkSession, DataFrame


class DataWriter:
    """Writes processed and curated datasets to S3."""

    def __init__(
            self,
            config: PipelineConfig,
            logger: PipelineLogger,
    ):
        self.config = config
        self.logger = logger
        self.s3 = boto3.client("s3", region_name=config.aws_region)
        self.configured_spark = None

    def set_spark(self, spark: SparkSession) -> None:
        self.configured_spark = spark

    def _path_exists(self, path: str) -> bool:
        match = re.match(r"s3a?://([^/]+)/(.+)", path)
        if not match:
            return False

        bucket, prefix = match.groups()
        response = self.s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix.rstrip("/") + "/",
            MaxKeys=1,
        )
        return bool(response.get("Contents"))

    def write_parquet(
            self,
            df: DataFrame,
            path: str,
            partition_columns: Optional[List[str]] = None,
            mode: Optional[str] = None,
    ) -> None:
        selected_mode = mode or self.config.write_mode

        try:
            writer = (
                df.write
                .mode(selected_mode)
                .format("parquet")
                .option("compression", "snappy")
            )

            if partition_columns:
                writer = writer.partitionBy(*partition_columns)

            writer.save(path)

            self.logger.info(
                "Parquet write completed",
                path=path,
                mode=selected_mode,
                partitions=partition_columns or [],
            )

        except Exception as exc:
            raise WriteException(
                f"Failed writing Parquet to {path}: {exc}"
            ) from exc

    def write_invalid(self, df: DataFrame, path: str) -> None:
        if df.limit(1).count() == 0:
            self.logger.info("No invalid records to write")
            return

        self.write_parquet(
            df,
            path,
            partition_columns=["event_date"],
            mode="append",
        )

    def write_processed(self, df: DataFrame, path: str) -> None:
        # Processed is date-scoped, so a rerun replaces only this date.
        self.write_parquet(
            df,
            path,
            partition_columns=["event_date"],
            mode="overwrite",
        )

    def write_dimension(
            self,
            incoming_df: DataFrame,
            name: str,
            path: str,
            business_key: str,
    ) -> None:
        """
        Upsert-style dimension publishing for development.

        Existing dimension data is read, merged with the incoming batch,
        de-duplicated on the business key, then rewritten. Dimensions are
        normally much smaller than the event fact, making this practical for
        a portfolio development environment.
        """
        try:
            if self._path_exists(path):
                if self.configured_spark is None:
                    raise WriteException(
                        "Spark session must be configured before dimension upsert"
                    )
                existing_df = self.configured_spark.read.parquet(path)
                combined = existing_df.unionByName(
                    incoming_df,
                    allowMissingColumns=True,
                )
                merged = (
                    combined
                    .dropDuplicates([business_key])
                )
            else:
                merged = incoming_df.dropDuplicates([business_key])

            (
                merged.write
                .mode("overwrite")
                .format("parquet")
                .option("compression", "snappy")
                .save(path)
            )

            self.logger.info(
                "Dimension upsert completed",
                dimension=name,
                path=path,
                business_key=business_key,
            )

        except Exception as exc:
            raise WriteException(
                f"Dimension write failed for {name}: {exc}"
            ) from exc

    def write_main(self, df: DataFrame, path: str) -> None:
        # Dynamic partition overwrite protects historical dates.
        self.write_parquet(
            df,
            path,
            partition_columns=["event_date"],
            mode="overwrite",
        )

    def write_fact(self, df: DataFrame, path: str) -> None:
        # Date-key partition is overwritten on rerun, preventing duplicate
        # fact rows for the same processing date.
        self.write_parquet(
            df,
            path,
            partition_columns=["date_key"],
            mode="overwrite",
        )