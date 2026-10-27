from common.pipeline_logger import PipelineLogger
from exceptions.pipeline_exceptions import TransformationException
from glue_etl.configuration.schema_manager import SchemaManager
from glue_etl.dimensions.surrogate_key_service import SurrogateKeyService
from pyspark.sql import DataFrame

class MainTableBuilder:
    """
    Builds the canonical event table.

    This is the cleaned, deduplicated, business-enriched table from which
    dimensions and fact are derived.
    """

    def __init__(
            self,
            logger: PipelineLogger,
            key_service: SurrogateKeyService,
    ):
        self.logger = logger
        self.key_service = key_service

    def build(self, df: DataFrame) -> DataFrame:
        try:
            result = self.key_service.add_event_key(df)

            result = result.select(
                *SchemaManager.main_columns(),
            )

            self.logger.info(
                "Main table built",
                columns=len(result.columns),
            )
            return result

        except Exception as exc:
            raise TransformationException(
                f"Main table build failed: {exc}"
            ) from exc