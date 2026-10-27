from common.pipeline_logger import PipelineLogger
from exceptions.pipeline_exceptions import DimensionBuildException
from glue_etl.dimensions.surrogate_key_service import SurrogateKeyService
from pyspark.sql import DataFrame

class VideoDimensionBuilder:
    """Builds dim_video."""

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
                    "video_id",
                    "video_title",
                    "category",
                    "channel_id",
                    "channel_name",
                )
                .dropDuplicates(["video_id"])
            )

            result = self.key_service.add_dimension_key(
                result,
                "video_id",
                "video_key",
            )

            result = result.select(
                "video_key",
                "video_id",
                "video_title",
                "category",
                "channel_id",
                "channel_name",
            )

            self.logger.info(
                "dim_video built",
                count=result.count(),
            )
            return result

        except Exception as exc:
            raise DimensionBuildException(
                f"dim_video failed: {exc}"
            ) from exc