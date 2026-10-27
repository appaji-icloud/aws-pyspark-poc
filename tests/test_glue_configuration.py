import unittest
from unittest.mock import Mock, patch

from constants.aws_constants import AWSConstants
from exceptions.pipeline_exceptions import ConfigurationException
from glue_etl.configuration.glue_config import ConfigurationLoader
from glue_etl.lambda_orchestration.lambda_function import (
    GlueJobTrigger,
    LambdaConfiguration,
    S3ReadyEventException,
    S3ReadyEventParser,
)
from glue_etl.output.data_writer import DataWriter


class GlueConfigurationTests(unittest.TestCase):
    def test_glue_arguments_override_defaults(self):
        argv = [
            "glue_job.py",
            "--INPUT_DATE",
            "2026-10-06",
            "--JOB_NAME=YoutubeAnalytics",
            "--RAW_BUCKET",
            "raw-test-bucket",
            "--PROCESSED_BUCKET=processed-test-bucket",
            "--CURATED_BUCKET",
            "curated-test-bucket",
            "--AWS_REGION",
            "ap-south-1",
            "--ENVIRONMENT",
            "test",
            "--DATA_QUALITY_THRESHOLD",
            "0.8",
            "--ENABLE_XLSX=true",
            "--WRITE_MODE=overwrite",
            "--SNS_TOPIC_ARN",
            "arn:aws:sns:ap-south-1:123456789012:etl-test",
        ]
        with (
            patch("glue_etl.configuration.glue_config.sys.argv", argv),
        ):
            config = ConfigurationLoader(Mock()).load()

        self.assertEqual(config.job_name, "YoutubeAnalytics")
        self.assertEqual(config.raw_bucket, "raw-test-bucket")
        self.assertEqual(config.processed_bucket, "processed-test-bucket")
        self.assertEqual(config.curated_bucket, "curated-test-bucket")
        self.assertEqual(config.aws_region, "ap-south-1")
        self.assertEqual(config.environment, "test")
        self.assertEqual(config.data_quality_threshold, 0.8)
        self.assertTrue(config.enable_xlsx)
        self.assertEqual(config.write_mode, "overwrite")
        self.assertEqual(
            config.sns_topic_arn,
            "arn:aws:sns:ap-south-1:123456789012:etl-test",
        )

    def test_glue_uses_defaults_for_omitted_optional_arguments(self):
        argv = ["glue_job.py", "--INPUT_DATE=2026-10-06"]
        with (
            patch("glue_etl.configuration.glue_config.sys.argv", argv),
        ):
            config = ConfigurationLoader(Mock()).load()

        self.assertEqual(config.raw_bucket, AWSConstants.RAW_BUCKET)
        self.assertEqual(config.aws_region, AWSConstants.REGION)
        self.assertEqual(
            config.data_quality_threshold,
            AWSConstants.DEFAULT_DATA_QUALITY_THRESHOLD,
        )
        self.assertEqual(config.enable_xlsx, AWSConstants.DEFAULT_ENABLE_XLSX)

    def test_input_date_uses_glue_argument_resolver_before_workflow_fallback(self):
        argv = ["glue_job.py", "--INPUT_DATE", "2026-10-06"]
        with (
            patch("glue_etl.configuration.glue_config.sys.argv", argv),
            patch(
                "glue_etl.configuration.glue_config.ConfigurationLoader."
                "_read_cli_arguments",
                return_value={},
            ),
            patch(
                "glue_etl.configuration.glue_config.getResolvedOptions",
                return_value={"INPUT_DATE": "2026-10-06"},
            ) as resolve_options,
        ):
            config = ConfigurationLoader(Mock()).load()

        self.assertEqual(config.input_date, "2026-10-06")
        resolve_options.assert_called_once_with(argv, ["INPUT_DATE"])

    def test_invalid_optional_argument_fails_configuration(self):
        argv = [
            "glue_job.py",
            "--INPUT_DATE=2026-10-06",
            "--ENABLE_XLSX=maybe",
        ]
        with (
            patch("glue_etl.configuration.glue_config.sys.argv", argv),
            self.assertRaises(ConfigurationException),
        ):
            ConfigurationLoader(Mock()).load()

    def test_input_date_is_read_from_glue_workflow_properties(self):
        argv = [
            "glue_job.py",
            "--INPUT_DATE=2020-01-01",
            "--WORKFLOW_NAME=YoutubeETLWorkflow",
            "--WORKFLOW_RUN_ID=run-123",
        ]
        glue_client = Mock()
        glue_client.get_workflow_run_properties.return_value = {
            "RunProperties": {"INPUT_DATE": "2026-10-06"}
        }
        with (
            patch("glue_etl.configuration.glue_config.sys.argv", argv),
            patch(
                "glue_etl.configuration.glue_config.boto3.client",
                return_value=glue_client,
            ) as client_factory,
        ):
            config = ConfigurationLoader(Mock()).load()

        self.assertEqual(config.input_date, "2026-10-06")
        self.assertEqual(config.raw_bucket, AWSConstants.RAW_BUCKET)
        client_factory.assert_called_once_with(
            "glue",
            region_name=AWSConstants.REGION,
        )
        glue_client.get_workflow_run_properties.assert_called_once_with(
            Name="YoutubeETLWorkflow",
            RunId="run-123",
        )


class GlueIntegrationAlignmentTests(unittest.TestCase):
    @staticmethod
    def _event(key):
        return {
            "detail": {
                "bucket": {"name": "raw-test-bucket"},
                "object": {"key": key},
            }
        }

    def test_ready_event_uses_configured_raw_prefix(self):
        parser = S3ReadyEventParser("raw-test-bucket")
        self.assertEqual(
            parser.extract_input_date(
                self._event("source/2026-10-06/_READY")
            ),
            "2026-10-06",
        )

    def test_ready_event_rejects_old_incompatible_prefix(self):
        parser = S3ReadyEventParser("raw-test-bucket")
        with self.assertRaises(S3ReadyEventException):
            parser.extract_input_date(
                self._event("raw/2026-10-06/_READY")
            )

    def test_lambda_forwards_glue_runtime_overrides(self):
        config = LambdaConfiguration(
            glue_job_name="YouTubeDataETL",
            raw_bucket="raw-test-bucket",
            processed_bucket="processed-test-bucket",
            curated_bucket="curated-test-bucket",
            aws_region="ap-south-1",
            environment="test",
            sns_topic_arn="arn:aws:sns:ap-south-1:123456789012:etl-test",
            data_quality_threshold="0.8",
            enable_xlsx="true",
            write_mode="overwrite",
        )

        arguments = GlueJobTrigger(config, glue_client=Mock()).build_job_arguments(
            "2026-10-06"
        )

        self.assertEqual(arguments["--INPUT_DATE"], "2026-10-06")
        self.assertEqual(arguments["--RAW_BUCKET"], "raw-test-bucket")
        self.assertEqual(arguments["--AWS_REGION"], "ap-south-1")
        self.assertEqual(arguments["--DATA_QUALITY_THRESHOLD"], "0.8")
        self.assertEqual(arguments["--ENABLE_XLSX"], "true")
        self.assertEqual(arguments["--WRITE_MODE"], "overwrite")

    def test_lambda_starts_workflow_with_batch_date(self):
        config = LambdaConfiguration(
            glue_job_name="",
            raw_bucket="raw-test-bucket",
            processed_bucket="processed-test-bucket",
            curated_bucket="curated-test-bucket",
            aws_region="ap-south-1",
            environment="test",
            data_quality_threshold="0.8",
            enable_xlsx="true",
            write_mode="overwrite",
            glue_workflow_name="YoutubeETLWorkflow",
        )
        glue_client = Mock()
        glue_client.start_workflow_run.return_value = {"RunId": "run-123"}
        trigger = GlueJobTrigger(config, glue_client=glue_client)

        result = trigger.trigger_glue_job("2026-10-06")

        self.assertEqual(result["workflow_name"], "YoutubeETLWorkflow")
        self.assertEqual(result["workflow_run_id"], "run-123")
        self.assertEqual(result["input_date"], "2026-10-06")
        glue_client.start_workflow_run.assert_called_once_with(
            Name="YoutubeETLWorkflow",
            RunProperties={
                "INPUT_DATE": "2026-10-06",
                "RAW_BUCKET": "raw-test-bucket",
                "PROCESSED_BUCKET": "processed-test-bucket",
                "CURATED_BUCKET": "curated-test-bucket",
                "AWS_REGION": "ap-south-1",
                "ENVIRONMENT": "test",
                "DATA_QUALITY_THRESHOLD": "0.8",
                "ENABLE_XLSX": "true",
                "WRITE_MODE": "overwrite",
            },
        )
        glue_client.start_job_run.assert_not_called()

    def test_dimension_path_existence_supports_s3a_scheme(self):
        writer = DataWriter.__new__(DataWriter)
        writer.s3 = Mock()
        writer.s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "curated/date/dim_user/part-000.parquet"}]
        }

        exists = writer._path_exists(
            "s3a://curated-test-bucket/curated/date/dim_user"
        )

        self.assertTrue(exists)
        writer.s3.list_objects_v2.assert_called_once_with(
            Bucket="curated-test-bucket",
            Prefix="curated/date/dim_user/",
            MaxKeys=1,
        )


if __name__ == "__main__":
    unittest.main()
