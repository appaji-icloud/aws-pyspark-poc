
from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional
import sys

import boto3

from common.pipeline_logger import PipelineLogger
from constants.aws_constants import AWSConstants
from exceptions.pipeline_exceptions import ConfigurationException

try:
    from awsglue.utils import GlueArgumentError, getResolvedOptions
except ImportError:
    GlueArgumentError = None
    getResolvedOptions = None


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration for the AWS Glue ETL job."""

    job_name: str
    input_date: str
    raw_bucket: str
    processed_bucket: str
    curated_bucket: str
    aws_region: str
    sns_topic_arn: Optional[str]
    environment: str
    data_quality_threshold: float
    enable_xlsx: bool
    write_mode: str

    @property
    def raw_base(self) -> str:
        return (
            f"s3a://{self.raw_bucket}/"
            f"{AWSConstants.RAW_PREFIX}/{self.input_date}"
        )

    @property
    def processed_base(self) -> str:
        return (
            f"s3a://{self.processed_bucket}/"
            f"{AWSConstants.PROCESSED_PREFIX}/{self.input_date}"
        )

    @property
    def curated_base(self) -> str:
        return (
            f"s3a://{self.curated_bucket}/"
            f"{AWSConstants.CURATED_PREFIX}/{self.input_date}"
        )

    @property
    def main_table_path(self) -> str:
        return (
            f"{self.curated_base}/"
            f"{AWSConstants.MAIN_TABLE_NAME}"
        )

    @property
    def fact_table_path(self) -> str:
        return (
            f"{self.curated_base}/"
            f"{AWSConstants.FACT_TABLE_NAME}"
        )

    def validate(self) -> None:
        """Validate the configuration before starting the ETL."""

        try:
            parsed_date = date.fromisoformat(self.input_date)

            if parsed_date.isoformat() != self.input_date:
                raise ValueError

        except (ValueError, TypeError) as exc:
            raise ConfigurationException(
                "INPUT_DATE must use YYYY-MM-DD format; "
                f"received {self.input_date!r}"
            ) from exc

        if self.environment not in {"dev", "test", "prod"}:
            raise ConfigurationException(
                f"Unsupported environment: {self.environment}"
            )

        for name, value in (
                ("job_name", self.job_name),
                ("aws_region", self.aws_region),
                ("raw_bucket", self.raw_bucket),
                ("processed_bucket", self.processed_bucket),
                ("curated_bucket", self.curated_bucket),
        ):
            if not value or not value.strip():
                raise ConfigurationException(
                    f"{name} is required"
                )

        if self.write_mode not in {"append", "overwrite"}:
            raise ConfigurationException(
                f"Unsupported write mode: {self.write_mode}"
            )

        if not 0 <= self.data_quality_threshold <= 1:
            raise ConfigurationException(
                "Data quality threshold must be between 0 and 1"
            )


class ConfigurationLoader:
    """Load Glue job arguments, falling back to centralized defaults."""

    RUNTIME_ARGUMENTS = {
        "INPUT_DATE",
        "WORKFLOW_NAME",
        "WORKFLOW_RUN_ID",
        "JOB_NAME",
        "RAW_BUCKET",
        "PROCESSED_BUCKET",
        "CURATED_BUCKET",
        "AWS_REGION",
        "SNS_TOPIC_ARN",
        "ENVIRONMENT",
        "DATA_QUALITY_THRESHOLD",
        "ENABLE_XLSX",
        "WRITE_MODE",
    }

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    @classmethod
    def _read_cli_arguments(cls) -> Dict[str, str]:
        """Extract recognized ``--NAME value`` and ``--NAME=value`` options."""
        arguments: Dict[str, str] = {}
        index = 1

        while index < len(sys.argv):
            token = sys.argv[index]
            if not token.startswith("--"):
                index += 1
                continue

            name, separator, value = token[2:].partition("=")
            if name not in cls.RUNTIME_ARGUMENTS:
                index += 1
                continue

            if not separator:
                if index + 1 >= len(sys.argv) or sys.argv[index + 1].startswith("--"):
                    raise ConfigurationException(
                        f"Missing value for runtime parameter --{name}"
                    )
                index += 1
                value = sys.argv[index]

            arguments[name] = value.strip()
            index += 1

        return arguments

    @classmethod
    def _read_workflow_properties(
            cls,
            arguments: Dict[str, str],
    ) -> Dict[str, str]:
        """Read ETL settings passed through the active Glue Workflow run."""
        workflow_name = arguments.get("WORKFLOW_NAME")
        workflow_run_id = arguments.get("WORKFLOW_RUN_ID")
        if not workflow_name or not workflow_run_id:
            if getResolvedOptions is None:
                raise ConfigurationException(
                    "This entry point requires the AWS Glue runtime."
                )
            try:
                workflow_args = getResolvedOptions(
                    sys.argv,
                    ["WORKFLOW_NAME", "WORKFLOW_RUN_ID"],
                )
                workflow_name = workflow_args["WORKFLOW_NAME"]
                workflow_run_id = workflow_args["WORKFLOW_RUN_ID"]
            except Exception as exc:
                raise ConfigurationException(
                    "Missing INPUT_DATE and Glue Workflow run context."
                ) from exc

        try:
            response = boto3.client(
                "glue",
                region_name=AWSConstants.REGION,
            ).get_workflow_run_properties(
                Name=workflow_name,
                RunId=workflow_run_id,
            )
            properties = response.get("RunProperties", {})
            return {
                name: str(value)
                for name, value in properties.items()
                if name in cls.RUNTIME_ARGUMENTS
            }
        except ConfigurationException:
            raise
        except Exception as exc:
            raise ConfigurationException(
                "Unable to read settings from the Glue Workflow run."
            ) from exc

    @staticmethod
    def _parse_bool(name: str, value: str) -> bool:
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
        raise ConfigurationException(
            f"{name} must be true or false; received {value!r}"
        )

    @staticmethod
    def _parse_float(name: str, value: str) -> float:
        try:
            return float(value)
        except ValueError as exc:
            raise ConfigurationException(
                f"{name} must be a number; received {value!r}"
            ) from exc

    def load(
            self,
            default_job_name: Optional[str] = None,
    ) -> PipelineConfig:
        """Create and validate the runtime configuration."""

        arguments = self._read_cli_arguments()
        if (
                not arguments.get("INPUT_DATE")
                and getResolvedOptions is not None
        ):
            try:
                arguments.update(
                    getResolvedOptions(sys.argv, ["INPUT_DATE"])
                )
            except GlueArgumentError:
                pass
        if (
                arguments.get("WORKFLOW_NAME")
                and arguments.get("WORKFLOW_RUN_ID")
        ) or not arguments.get("INPUT_DATE"):
            arguments.update(self._read_workflow_properties(arguments))
        if not arguments.get("INPUT_DATE"):
            raise ConfigurationException(
                "Glue job requires --INPUT_DATE or an INPUT_DATE "
                "Glue Workflow run property."
            )

        sns_topic_arn = arguments.get(
            "SNS_TOPIC_ARN",
            AWSConstants.SNS_TOPIC_ARN,
        )
        config = PipelineConfig(
            job_name=arguments.get(
                "JOB_NAME",
                default_job_name or AWSConstants.DEFAULT_JOB_NAME,
            ),
            input_date=arguments["INPUT_DATE"],
            raw_bucket=arguments.get("RAW_BUCKET", AWSConstants.RAW_BUCKET),
            processed_bucket=arguments.get(
                "PROCESSED_BUCKET",
                AWSConstants.PROCESSED_BUCKET,
            ),
            curated_bucket=arguments.get(
                "CURATED_BUCKET",
                AWSConstants.CURATED_BUCKET,
            ),
            aws_region=arguments.get("AWS_REGION", AWSConstants.REGION),
            sns_topic_arn=sns_topic_arn or None,
            environment=arguments.get(
                "ENVIRONMENT",
                AWSConstants.DEFAULT_ENVIRONMENT,
            ),
            data_quality_threshold=self._parse_float(
                "DATA_QUALITY_THRESHOLD",
                arguments.get(
                    "DATA_QUALITY_THRESHOLD",
                    str(AWSConstants.DEFAULT_DATA_QUALITY_THRESHOLD),
                ),
            ),
            enable_xlsx=self._parse_bool(
                "ENABLE_XLSX",
                arguments.get(
                    "ENABLE_XLSX",
                    str(AWSConstants.DEFAULT_ENABLE_XLSX),
                ),
            ),
            write_mode=arguments.get(
                "WRITE_MODE",
                AWSConstants.DEFAULT_WRITE_MODE,
            ).lower(),
        )

        config.validate()

        self.logger.info(
            "Configuration loaded",
            job_name=config.job_name,
            input_date=config.input_date,
            aws_region=config.aws_region,
            raw_bucket=config.raw_bucket,
            processed_bucket=config.processed_bucket,
            curated_bucket=config.curated_bucket,
            environment=config.environment,
            enable_xlsx=config.enable_xlsx,
            write_mode=config.write_mode,
        )

        return config
