from pyspark.sql import SparkSession, DataFrame
from common.pipeline_logger import PipelineLogger
from constants.aws_constants import AWSConstants
from exceptions.pipeline_exceptions import InputDataException, EmptyDataException
from glue_etl.configuration.glue_config import PipelineConfig
from glue_etl.configuration.schema_manager import SchemaManager
from typing import Optional, List, Dict
import uuid, boto3, os
from pyspark.sql import functions as F
class DataReader:
    """Reads CSV, JSONL and XLSX source data."""

    def __init__(
            self,
            spark: SparkSession,
            config: PipelineConfig,
            logger: PipelineLogger,
    ):
        self.spark = spark
        self.config = config
        self.logger = logger

    def read_csv(self, path: str) -> DataFrame:
        try:
            self.logger.info("Reading CSV", path=path)
            return (
                self.spark.read
                .option("header", "true")
                .option("mode", "PERMISSIVE")
                .schema(SchemaManager.SOURCE_SCHEMA)
                .csv(path)
                .withColumn("source_format", F.lit("csv"))
            )
        except Exception as exc:
            raise InputDataException(
                f"CSV read failed: {path}: {exc}"
            ) from exc

    def read_json(self, path: str) -> DataFrame:
        try:
            self.logger.info("Reading JSONL", path=path)
            return (
                self.spark.read
                .option("mode", "PERMISSIVE")
                .schema(SchemaManager.SOURCE_SCHEMA)
                .json(path)
                .withColumn("source_format", F.lit("json"))
            )
        except Exception as exc:
            raise InputDataException(
                f"JSON read failed: {path}: {exc}"
            ) from exc

    def read_xlsx(self, keys: List[str]) -> Optional[DataFrame]:
        if not self.config.enable_xlsx or not keys:
            self.logger.info(
                "XLSX ingestion disabled or no XLSX files",
                enabled=self.config.enable_xlsx,
                count=len(keys),
            )
            return None

        try:
            import pandas as pd
        except ImportError as exc:
            raise InputDataException(
                "pandas is required for XLSX development ingestion. "
                "Add pandas/openpyxl as Glue additional modules."
            ) from exc

        frames: List[DataFrame] = []

        for key in keys:
            local_file = f"/tmp/{uuid.uuid4().hex}.xlsx"
            try:
                self.logger.info("Downloading XLSX", key=key)

                boto3.client(
                    "s3",
                    region_name=self.config.aws_region,
                ).download_file(
                    self.config.raw_bucket,
                    key,
                    local_file,
                )

                pdf = pd.read_excel(local_file, engine="openpyxl")

                if pdf.empty:
                    self.logger.warning(
                        "XLSX file is empty",
                        key=key,
                    )
                    continue

                # Normalize column names.
                pdf.columns = [
                    str(c).strip().lower()
                    for c in pdf.columns
                ]

                # Spark inference is used only after the workbook is loaded.
                frame = self.spark.createDataFrame(pdf)

                for field in SchemaManager.SOURCE_SCHEMA.fields:
                    if field.name not in frame.columns:
                        frame = frame.withColumn(
                            field.name,
                            F.lit(None).cast(field.dataType),
                        )
                    else:
                        frame = frame.withColumn(
                            field.name,
                            F.col(field.name).cast(field.dataType),
                        )

                frame = (
                    frame
                    .select(
                        [field.name for field in SchemaManager.SOURCE_SCHEMA.fields]
                    )
                    .withColumn("source_format", F.lit("xlsx"))
                )

                frames.append(frame)

            except Exception as exc:
                raise InputDataException(
                    f"XLSX read failed: s3://{self.config.raw_bucket}/{key}: {exc}"
                ) from exc
            finally:
                try:
                    if os.path.exists(local_file):
                        os.remove(local_file)
                except OSError:
                    pass

        if not frames:
            return None

        result = frames[0]
        for frame in frames[1:]:
            result = result.unionByName(frame)

        return result

    def read_all(self, files: Dict[str, List[str]]) -> DataFrame:
        csv_df = None
        json_df = None
        xlsx_df = None

        if files["csv"]:
            csv_df = self.read_csv(
                f"s3a://{self.config.raw_bucket}/"
                f"{AWSConstants.RAW_PREFIX}/"
                f"{self.config.input_date}/csv/"
            )

        if files["json"]:
            json_df = self.read_json(
                f"s3a://{self.config.raw_bucket}/"
                f"{AWSConstants.RAW_PREFIX}/"
                f"{self.config.input_date}/json/"
            )

        if files["xlsx"]:
            xlsx_df = self.read_xlsx(files["xlsx"])

        datasets = [
            df for df in (csv_df, json_df, xlsx_df)
            if df is not None
        ]

        if not datasets:
            raise EmptyDataException("All source datasets are empty")

        combined = datasets[0]
        for df in datasets[1:]:
            combined = combined.unionByName(df)

        self.logger.info(
            "All source formats combined",
            format_count=len(datasets),
            columns=len(combined.columns),
        )

        return combined