import json
import traceback
from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from common.pipeline_logger import PipelineLogger
from glue_etl.configuration.glue_config import PipelineConfig
from glue_etl.dimensions.channel_dimension_builder import (
    ChannelDimensionBuilder,
)
from glue_etl.dimensions.date_dimension_builder import DateDimensionBuilder
from glue_etl.dimensions.device_dimension_builder import (
    DeviceDimensionBuilder,
)
from glue_etl.dimensions.surrogate_key_service import SurrogateKeyService
from glue_etl.dimensions.user_dimension_builder import UserDimensionBuilder
from glue_etl.dimensions.video_dimension_builder import VideoDimensionBuilder
from glue_etl.facts.fact_table_builder import FactTableBuilder
from glue_etl.facts.main_table_builder import MainTableBuilder
from glue_etl.ingestion.data_reader import DataReader
from glue_etl.ingestion.source_file_discovery import SourceFileDiscovery
from glue_etl.ingestion.source_readiness import SourceReadinessService
from glue_etl.monitoring.batch_summary import BatchSummaryService
from glue_etl.monitoring.pipeline_health import PipelineHealthService
from glue_etl.monitoring.pipeline_metrics import PipelineMetrics
from glue_etl.monitoring.sns_notifier import SNSNotifier
from glue_etl.output.data_writer import DataWriter
from glue_etl.transformation.business_transformation import (
    BusinessTransformationService,
)
from glue_etl.transformation.data_standardizer import DataStandardizer
from glue_etl.transformation.deduplication_service import (
    DeduplicationService,
)
from glue_etl.validation.data_quality_auditor import DataQualityAuditor
from glue_etl.validation.data_validator import DataValidator
from glue_etl.validation.s3_path_manager import S3PathManager
from glue_etl.validation.schema_contract import SchemaContractService


class PipelineOrchestrator:
    """Shared services and lifecycle for one independently runnable layer job."""

    stage_name = "pipeline"

    def __init__(
            self,
            spark: SparkSession,
            config: PipelineConfig,
            logger: PipelineLogger,
    ):
        self.spark = spark
        self.config = config
        self.logger = logger

        self.paths = S3PathManager(config, logger)
        self.keys = SurrogateKeyService(logger)

        self.main_builder = MainTableBuilder(logger, self.keys)
        self.user_builder = UserDimensionBuilder(logger, self.keys)
        self.video_builder = VideoDimensionBuilder(logger, self.keys)
        self.channel_builder = ChannelDimensionBuilder(logger, self.keys)
        self.device_builder = DeviceDimensionBuilder(logger, self.keys)
        self.date_builder = DateDimensionBuilder(logger)
        self.fact_builder = FactTableBuilder(logger)

        self.audit = DataQualityAuditor(logger)
        self.health = PipelineHealthService(logger)
        self.writer = DataWriter(config, logger)
        self.writer.set_spark(spark)
        self.notifier = SNSNotifier(config, logger)
        self.metrics = PipelineMetrics()

    def _cache(self, frame: DataFrame) -> DataFrame:
        return frame.persist(StorageLevel.MEMORY_AND_DISK)

    def _unpersist(self, frames: list) -> None:
        for frame in frames:
            if frame is not None:
                try:
                    frame.unpersist()
                except Exception as exc:
                    self.logger.warning(
                        "Unable to release cached DataFrame",
                        error_message=str(exc),
                    )

    def _read_nonempty(self, path: str, name: str) -> DataFrame:
        frame = self.spark.read.parquet(path)
        self.health.assert_non_empty(frame, name)
        return frame

    def _notify_failure(self, exc: Exception) -> None:
        try:
            self.notifier.notify(
                subject=f"[FAILED] {self.config.job_name} {self.config.input_date}",
                message=json.dumps(
                    {
                        "stage": self.stage_name,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "metrics": self.metrics.as_dict(),
                        "traceback": traceback.format_exc(),
                    },
                    indent=2,
                ),
            )
        except Exception:
            self.logger.exception("Unable to send failure notification")

    def run(self) -> PipelineMetrics:
        self.logger.info(
            "Glue layer job started",
            stage=self.stage_name,
            job_name=self.config.job_name,
            input_date=self.config.input_date,
        )
        try:
            self.execute_stage()
            self.metrics.finish()
            summary = self.metrics.as_dict()
            self.logger.info(
                "Glue layer job completed",
                stage=self.stage_name,
                **summary,
            )
            self.notifier.notify(
                subject=f"[SUCCESS] {self.config.job_name} {self.config.input_date}",
                message=json.dumps(
                    {"stage": self.stage_name, **summary},
                    indent=2,
                ),
            )
            return self.metrics
        except Exception as exc:
            self.metrics.finish()
            self.logger.exception(
                "Glue layer job failed",
                stage=self.stage_name,
                error_type=type(exc).__name__,
                error_message=str(exc),
                **self.metrics.as_dict(),
            )
            self._notify_failure(exc)
            raise

    def execute_stage(self) -> None:
        raise NotImplementedError


class RawToProcessedOrchestrator(PipelineOrchestrator):
    """Reads raw files and publishes validated, deduplicated processed events."""

    stage_name = "raw-to-processed"

    def __init__(
            self,
            spark: SparkSession,
            config: PipelineConfig,
            logger: PipelineLogger,
    ):
        super().__init__(spark, config, logger)
        self.discovery = SourceFileDiscovery(config, logger)
        self.reader = DataReader(spark, config, logger)
        self.standardizer = DataStandardizer(logger)
        self.validator = DataValidator(
            logger,
            config.data_quality_threshold,
        )
        self.deduplicator = DeduplicationService(logger)
        self.business = BusinessTransformationService(logger)

    def execute_stage(self) -> None:
        frames = []
        try:
            SourceReadinessService(self.config, self.logger).assert_ready()
            files = self.discovery.discover()
            source_df = self._cache(self.reader.read_all(files))
            frames.append(source_df)
            self.health.assert_non_empty(source_df, "raw source")
            self.metrics.source_rows = source_df.count()
            self.logger.info(
                "Raw source loaded",
                rows=self.metrics.source_rows,
                schema=source_df.schema.simpleString(),
            )

            SchemaContractService(self.logger).validate_contract(source_df)
            standardized_df = self._cache(
                self.standardizer.apply(source_df)
            )
            frames.append(standardized_df)

            valid_df, invalid_df = self.validator.split_valid_invalid(
                standardized_df
            )
            valid_df = self._cache(valid_df)
            invalid_df = self._cache(invalid_df)
            frames.extend([valid_df, invalid_df])

            self.metrics.valid_rows = valid_df.count()
            self.metrics.invalid_rows = invalid_df.count()
            self.writer.write_invalid(
                invalid_df,
                self.paths.invalid_path(),
            )

            deduped_df = self._cache(
                self.deduplicator.deduplicate(valid_df)
            )
            frames.append(deduped_df)
            self.metrics.deduplicated_rows = deduped_df.count()

            transformed_df = self.business.calculate_metrics(deduped_df)
            main_df = self._cache(self.main_builder.build(transformed_df))
            frames.append(main_df)
            self.metrics.main_rows = main_df.count()

            self.writer.write_processed(
                main_df,
                self.paths.processed_path(),
            )
        finally:
            self._unpersist(frames)


class ProcessedToCuratedOrchestrator(PipelineOrchestrator):
    """Builds and publishes curated main events and dimensions."""

    stage_name = "processed-to-curated"

    def execute_stage(self) -> None:
        main_df = self._read_nonempty(
            self.paths.processed_path(),
            "processed events",
        )
        self.metrics.source_rows = main_df.count()
        self.metrics.main_rows = self.metrics.source_rows

        dimensions = {
            "dim_user": (
                self.user_builder.build(main_df),
                "user_id",
            ),
            "dim_video": (
                self.video_builder.build(main_df),
                "video_id",
            ),
            "dim_channel": (
                self.channel_builder.build(main_df),
                "channel_id",
            ),
            "dim_device": (
                self.device_builder.build(main_df),
                "device_key",
            ),
            "dim_date": (
                self.date_builder.build(main_df),
                "date_key",
            ),
        }
        for name, (dimension_df, business_key) in dimensions.items():
            self.writer.write_dimension(
                dimension_df,
                name,
                self.paths.dimension_path(name),
                business_key,
            )

        self.writer.write_main(main_df, self.paths.main_path())


class CuratedFactOrchestrator(PipelineOrchestrator):
    """Builds, audits, and publishes the curated video-events fact table."""

    stage_name = "curated-to-fact"

    def execute_stage(self) -> None:
        main_df = self._read_nonempty(
            self.paths.main_path(),
            "curated main events",
        )
        user_df = self._read_nonempty(
            self.paths.dimension_path("dim_user"),
            "dim_user",
        )
        video_df = self._read_nonempty(
            self.paths.dimension_path("dim_video"),
            "dim_video",
        )
        channel_df = self._read_nonempty(
            self.paths.dimension_path("dim_channel"),
            "dim_channel",
        )
        device_df = self._read_nonempty(
            self.paths.dimension_path("dim_device"),
            "dim_device",
        )
        date_df = self._read_nonempty(
            self.paths.dimension_path("dim_date"),
            "dim_date",
        )

        fact_df = self.fact_builder.build(
            main_df,
            user_df,
            video_df,
            channel_df,
            device_df,
            date_df,
        )
        self.metrics.source_rows = main_df.count()
        self.metrics.main_rows = self.metrics.source_rows
        self.metrics.fact_rows = fact_df.count()

        self.audit.audit(
            fact_df,
            user_df,
            video_df,
            channel_df,
            device_df,
            date_df,
        )
        BatchSummaryService(self.logger).summarize(main_df)
        self.writer.write_fact(fact_df, self.paths.fact_path())
