from common.pipeline_logger import PipelineLogger
from exceptions.pipeline_exceptions import DimensionBuildException
from glue_etl.dimensions.surrogate_key_service import SurrogateKeyService
from pyspark.sql import DataFrame

class UserDimensionBuilder:
    """Builds dim_user."""

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
                    "user_id",
                    "user_name",
                    "country",
                    "age_group",
                    "gender",
                    "language",
                )
                .dropDuplicates(["user_id"])
            )

            result = self.key_service.add_dimension_key(
                result,
                "user_id",
                "user_key",
            )

            result = result.select(
                "user_key",
                "user_id",
                "user_name",
                "country",
                "age_group",
                "gender",
                "language",
            )

            self.logger.info(
                "dim_user built",
                count=result.count(),
            )
            return result

        except Exception as exc:
            raise DimensionBuildException(
                f"dim_user failed: {exc}"
            ) from exc