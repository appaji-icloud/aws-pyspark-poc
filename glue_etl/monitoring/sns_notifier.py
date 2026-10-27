from common.pipeline_logger import PipelineLogger
from exceptions.pipeline_exceptions import NotificationException
from glue_etl.configuration.glue_config import PipelineConfig
import boto3

class SNSNotifier:
    """Sends pipeline status notifications."""

    def __init__(
            self,
            config: PipelineConfig,
            logger: PipelineLogger,
    ):
        self.config = config
        self.logger = logger

        self.client = None
        if config.sns_topic_arn:
            self.client = boto3.client(
                "sns",
                region_name=config.aws_region,
            )

    def notify(
            self,
            subject: str,
            message: str,
    ) -> None:
        if not self.config.sns_topic_arn:
            self.logger.info(
                "SNS notification skipped because SNS_TOPIC_ARN is not configured"
            )
            return

        try:
            self.client.publish(
                TopicArn=self.config.sns_topic_arn,
                Subject=subject[:100],
                Message=message,
            )
            self.logger.info(
                "SNS notification sent",
                subject=subject,
            )
        except Exception as exc:
            raise NotificationException(
                f"SNS notification failed: {exc}"
            ) from exc