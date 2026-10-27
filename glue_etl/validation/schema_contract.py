from pyspark.sql import DataFrame

from common.pipeline_logger import PipelineLogger
from exceptions.pipeline_exceptions import DataQualityException
from glue_etl.configuration.schema_manager import SchemaManager


class SchemaContractService:
    """Checks source schema names against the canonical contract."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def validate_contract(self, df: DataFrame) -> None:
        expected = {
            field.name
            for field in SchemaManager.SOURCE_SCHEMA.fields
        }

        actual = set(df.columns)

        missing = expected - actual
        unexpected = actual - expected - {"source_format"}

        if missing:
            raise DataQualityException(
                f"Schema contract missing columns: {sorted(missing)}"
            )

        if unexpected:
            self.logger.warning(
                "Additional source columns detected",
                columns=sorted(unexpected),
            )

        self.logger.info("Schema contract validation passed")