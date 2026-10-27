from typing import Optional

from common.pipeline_logger import PipelineLogger
from exceptions.pipeline_exceptions import ConfigurationException
from glue_etl.configuration.glue_config import PipelineConfig


class S3PathManager:
    """Builds and validates S3 locations."""

    def __init__(self, config: PipelineConfig, logger: PipelineLogger):
        self.config = config
        self.logger = logger

    def path(self, layer: str, entity: Optional[str] = None) -> str:
        if layer == "source":
            base = self.config.raw_base
        elif layer == "processed":
            base = self.config.processed_base
        elif layer == "curated":
            base = self.config.curated_base
        else:
            raise ConfigurationException(f"Unknown S3 layer: {layer}")

        return f"{base}/{entity}" if entity else base

    def source_csv(self) -> str:
        return self.path("raw", "csv")

    def source_json(self) -> str:
        return self.path("raw", "json")

    def source_xlsx(self) -> str:
        return self.path("raw", "xlsx")

    def invalid_path(self) -> str:
        return self.path("processed", "invalid")

    def processed_path(self) -> str:
        return self.path("processed", "events")

    def dimension_path(self, name: str) -> str:
        return self.path("curated", name)

    def main_path(self) -> str:
        return self.config.main_table_path

    def fact_path(self) -> str:
        return self.config.fact_table_path