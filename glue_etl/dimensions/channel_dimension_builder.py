from common.pipeline_logger import  PipelineLogger
from exceptions.pipeline_exceptions import DimensionBuildException
from glue_etl.dimensions.surrogate_key_service import SurrogateKeyService
from pyspark.sql import DataFrame

class ChannelDimensionBuilder:
    """Builds dim_channel."""

    def __init__(
            self,
            logger: PipelineLogger,
            key_service: SurrogateKeyService,
    ):
        self.logger = logger
        self.key_service = key_service

    def build(self, df: DataFrame) -> DataFrame:
        try:
            result = (
                df.select(
                    "channel_id",
                    "channel_name",
                    "category",
                )
                .dropDuplicates(["channel_id"])
            )

            result = self.key_service.add_dimension_key(
                result,
                "channel_id",
                "channel_key",
            )

            result = result.select(
                "channel_key",
                "channel_id",
                "channel_name",
                "category",
            )

            self.logger.info(
                "dim_channel built",
                count=result.count(),
            )
            return result

        except Exception as exc:
            raise DimensionBuildException(
                f"dim_channel failed: {exc}"
            ) from exc