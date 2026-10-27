from common.pipeline_logger import PipelineLogger
from exceptions.pipeline_exceptions import DimensionBuildException
from glue_etl.dimensions.surrogate_key_service import SurrogateKeyService
from pyspark.sql import DataFrame

class DeviceDimensionBuilder:
    """Builds dim_device."""

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
                    "device_type",
                    "platform",
                )
                .dropDuplicates(
                    ["device_type", "platform"]
                )
            )

            result = self.key_service.add_composite_key(
                result,
                ["device_type", "platform"],
                "device_key",
            )

            result = result.select(
                "device_key",
                "device_type",
                "platform",
            )

            self.logger.info(
                "dim_device built",
                count=result.count(),
            )
            return result

        except Exception as exc:
            raise DimensionBuildException(
                f"dim_device failed: {exc}"
            ) from exc