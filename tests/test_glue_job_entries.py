import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from constants.aws_constants import AWSConstants
from glue_etl.monitoring.pipeline_metrics import PipelineMetrics
from glue_etl.pipeline_orchestrator import (
    CuratedFactOrchestrator,
    ProcessedToCuratedOrchestrator,
    RawToProcessedOrchestrator,
)


class GlueLayerEntryTests(unittest.TestCase):
    def test_raw_entry_runs_raw_to_processed_layer(self):
        from glue_etl.jobs import raw_to_processed
        with patch("glue_etl.jobs.raw_to_processed.run_glue_job") as runner:
            raw_to_processed.main()

        runner.assert_called_once_with(
            RawToProcessedOrchestrator,
            AWSConstants.RAW_TO_PROCESSED_JOB_NAME,
        )

    def test_processed_entry_runs_processed_to_curated_layer(self):
        from glue_etl.jobs import processed_to_curated
        with patch(
            "glue_etl.jobs.processed_to_curated.run_glue_job"
        ) as runner:
            processed_to_curated.main()

        runner.assert_called_once_with(
            ProcessedToCuratedOrchestrator,
            AWSConstants.PROCESSED_TO_CURATED_JOB_NAME,
        )

    def test_fact_entry_runs_curated_fact_layer(self):
        from glue_etl.jobs import curated_fact
        with patch("glue_etl.jobs.curated_fact.run_glue_job") as runner:
            curated_fact.main()

        runner.assert_called_once_with(
            CuratedFactOrchestrator,
            AWSConstants.CURATED_FACT_JOB_NAME,
        )


class GlueLayerOrchestrationTests(unittest.TestCase):
    def test_processed_to_curated_reads_processed_and_writes_curated(self):
        orchestrator = ProcessedToCuratedOrchestrator.__new__(
            ProcessedToCuratedOrchestrator
        )
        main = Mock(name="processed-events")
        dimensions = {
            "dim_user": Mock(name="user-dimension"),
            "dim_video": Mock(name="video-dimension"),
            "dim_channel": Mock(name="channel-dimension"),
            "dim_device": Mock(name="device-dimension"),
            "dim_date": Mock(name="date-dimension"),
        }
        orchestrator.spark = Mock()
        orchestrator.spark.read.parquet.return_value = main
        orchestrator.paths = Mock()
        orchestrator.paths.processed_path.return_value = "processed/events"
        orchestrator.paths.dimension_path.side_effect = (
            lambda name: f"curated/{name}"
        )
        orchestrator.paths.main_path.return_value = "curated/main_video_events"
        orchestrator.health = SimpleNamespace(
            assert_non_empty=Mock(),
        )
        orchestrator.metrics = PipelineMetrics()
        orchestrator.logger = Mock()
        orchestrator.user_builder = Mock()
        orchestrator.video_builder = Mock()
        orchestrator.channel_builder = Mock()
        orchestrator.device_builder = Mock()
        orchestrator.date_builder = Mock()
        for name, frame in dimensions.items():
            builder = getattr(
                orchestrator,
                {
                    "dim_user": "user_builder",
                    "dim_video": "video_builder",
                    "dim_channel": "channel_builder",
                    "dim_device": "device_builder",
                    "dim_date": "date_builder",
                }[name],
            )
            builder.build.return_value = frame
        orchestrator.writer = Mock()

        orchestrator.execute_stage()

        orchestrator.spark.read.parquet.assert_called_once_with(
            "processed/events"
        )
        orchestrator.writer.write_main.assert_called_once_with(
            main,
            "curated/main_video_events",
        )
        self.assertEqual(orchestrator.writer.write_dimension.call_count, 5)
        orchestrator.writer.write_fact.assert_not_called()

    def test_curated_fact_reads_curated_inputs_and_writes_fact(self):
        orchestrator = CuratedFactOrchestrator.__new__(
            CuratedFactOrchestrator
        )
        main = Mock(name="curated-main")
        dimensions = [
            Mock(name="user-dimension"),
            Mock(name="video-dimension"),
            Mock(name="channel-dimension"),
            Mock(name="device-dimension"),
            Mock(name="date-dimension"),
        ]
        fact = Mock(name="fact")
        orchestrator.spark = Mock()
        orchestrator.spark.read.parquet.side_effect = [main, *dimensions]
        orchestrator.paths = Mock()
        orchestrator.paths.main_path.return_value = "curated/main_video_events"
        orchestrator.paths.dimension_path.side_effect = (
            lambda name: f"curated/{name}"
        )
        orchestrator.paths.fact_path.return_value = "curated/fact_video_events"
        orchestrator.health = SimpleNamespace(
            assert_non_empty=Mock(),
        )
        orchestrator.metrics = PipelineMetrics()
        orchestrator.logger = Mock()
        orchestrator.fact_builder = Mock()
        orchestrator.fact_builder.build.return_value = fact
        orchestrator.audit = Mock()
        orchestrator.writer = Mock()

        with patch(
            "glue_etl.pipeline_orchestrator.BatchSummaryService"
        ) as summary:
            orchestrator.execute_stage()

        orchestrator.fact_builder.build.assert_called_once_with(
            main,
            *dimensions,
        )
        orchestrator.audit.audit.assert_called_once_with(
            fact,
            *dimensions,
        )
        orchestrator.writer.write_fact.assert_called_once_with(
            fact,
            "curated/fact_video_events",
        )
        summary.return_value.summarize.assert_called_once_with(main)


if __name__ == "__main__":
    unittest.main()
