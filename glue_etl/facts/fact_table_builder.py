from common.pipeline_logger import PipelineLogger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from exceptions.pipeline_exceptions import FactBuildException


class FactTableBuilder:
    """Builds fact_video_events by joining canonical events to dimensions."""

    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def build(
            self,
            main_df: DataFrame,
            user_df: DataFrame,
            video_df: DataFrame,
            channel_df: DataFrame,
            device_df: DataFrame,
            date_df: DataFrame,
    ) -> DataFrame:
        try:
            result = (
                main_df.alias("m")
                .join(
                    user_df.alias("u"),
                    F.col("m.user_id") == F.col("u.user_id"),
                    "left",
                    )
                .join(
                    video_df.alias("v"),
                    F.col("m.video_id") == F.col("v.video_id"),
                    "left",
                    )
                .join(
                    channel_df.alias("c"),
                    F.col("m.channel_id") == F.col("c.channel_id"),
                    "left",
                    )
                .join(
                    device_df.alias("d"),
                    (
                            (F.col("m.device_type") == F.col("d.device_type"))
                            & (F.col("m.platform") == F.col("d.platform"))
                    ),
                    "left",
                )
                .join(
                    date_df.alias("dt"),
                    F.col("m.event_date") == F.col("dt.date"),
                    "left",
                    )
            )

            result = result.select(
                F.col("m.event_key"),
                F.col("m.event_id"),
                F.col("dt.date_key"),
                F.col("u.user_key"),
                F.col("v.video_key"),
                F.col("c.channel_key"),
                F.col("d.device_key"),
                F.col("m.event_timestamp"),
                F.col("m.event_date"),
                F.col("m.session_id"),
                F.col("m.event_type"),
                F.col("m.watch_seconds"),
                F.col("m.watch_minutes"),
                F.col("m.video_duration_seconds"),
                F.col("m.completion_percentage"),
                F.col("m.is_completed"),
                F.col("m.engagement_count"),
                F.col("m.liked"),
                F.col("m.commented"),
                F.col("m.shared"),
                F.col("m.subscribed"),
                F.col("m.is_premium"),
                F.col("m.traffic_source"),
                F.col("m.quality"),
            )

            self.logger.info(
                "fact_video_events built",
                count=result.count(),
                columns=len(result.columns),
            )

            return result

        except Exception as exc:
            raise FactBuildException(
                f"fact_video_events failed: {exc}"
            ) from exc