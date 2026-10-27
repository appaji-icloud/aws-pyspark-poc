from common.pipeline_logger import PipelineLogger
from constants.aws_constants import AWSConstants
from exceptions.pipeline_exceptions import InputDataException
from glue_etl.configuration.glue_config import PipelineConfig
import boto3
from typing import List, Dict

class SourceFileDiscovery:
    """Discovers source files through S3 API."""

    def __init__(
            self,
            config: PipelineConfig,
            logger: PipelineLogger,
    ):
        self.config = config
        self.logger = logger
        self.s3 = boto3.client("s3", region_name=config.aws_region)

    def list_objects(self, prefix: str) -> List[str]:
        keys: List[str] = []
        paginator = self.s3.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(
                    Bucket=self.config.raw_bucket,
                    Prefix=prefix,
            ):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith("/"):
                        keys.append(key)

            self.logger.info(
                "S3 objects discovered",
                prefix=prefix,
                count=len(keys),
            )
            return keys
        except Exception as exc:
            raise InputDataException(
                f"Unable to list S3 objects under {prefix}: {exc}"
            ) from exc
    def discover(self) -> Dict[str, List[str]]:
        base_prefix = (
            f"{AWSConstants.RAW_PREFIX}/{self.config.input_date}"
        )

        csv_keys = self.list_objects(f"{base_prefix}/csv/")
        json_keys = self.list_objects(f"{base_prefix}/json/")
        xlsx_keys = self.list_objects(f"{base_prefix}/xlsx/")

        csv_keys = [k for k in csv_keys if k.lower().endswith(".csv")]
        json_keys = [
            k for k in json_keys
            if k.lower().endswith((".json", ".jsonl"))
        ]
        xlsx_keys = [
            k for k in xlsx_keys
            if k.lower().endswith((".xlsx", ".xls"))
        ]

        if not csv_keys and not json_keys and not xlsx_keys:
            raise InputDataException(
                f"No supported source files found for {self.config.input_date}"
            )

        result = {
            "csv": csv_keys,
            "json": json_keys,
            "xlsx": xlsx_keys,
        }

        self.logger.info(
            "Source discovery completed",
            csv=len(csv_keys),
            json=len(json_keys),
            xlsx=len(xlsx_keys),
        )
        return result