
class AWSConstants:
    """Centralized AWS and YouTube ETL configuration."""

    # Glue job
    DEFAULT_JOB_NAME = "YouTubeDataETL"
    RAW_TO_PROCESSED_JOB_NAME = "YouTubeRawToProcessed"
    PROCESSED_TO_CURATED_JOB_NAME = "YouTubeProcessedToCurated"
    CURATED_FACT_JOB_NAME = "YouTubeCuratedFact"

    # AWS configuration
    REGION = "ap-south-2"

    # S3 buckets
    RAW_BUCKET = "yt-raw-data-upload"
    PROCESSED_BUCKET = "poc-yt-cleaned-data"
    CURATED_BUCKET = "poc-yt-curated-data"

    # S3 prefixes
    RAW_PREFIX = "source"
    PROCESSED_PREFIX = "processed"
    CURATED_PREFIX = "curated"
    READY_FILE = "_READY"

    # Curated table names
    MAIN_TABLE_NAME = "main_video_events"
    FACT_TABLE_NAME = "fact_video_events"

    # ETL defaults
    DEFAULT_ENVIRONMENT = "dev"
    DEFAULT_DATA_QUALITY_THRESHOLD = 0.95
    DEFAULT_ENABLE_XLSX = False
    DEFAULT_WRITE_MODE = "append"
    # Optional integrations
    SNS_TOPIC_ARN = None

    # Spark configuration
    SPARK_SETTINGS = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
    }
