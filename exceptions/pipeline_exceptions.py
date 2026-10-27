class YouTubePipelineException(Exception):
    """Base exception for the YouTube data pipeline."""


class ConfigurationException(YouTubePipelineException):
    """Raised when required runtime configuration is missing or invalid."""


class InputDataException(YouTubePipelineException):
    """Raised when source data is missing, unreadable, or malformed."""


class DataQualityException(YouTubePipelineException):
    """Raised when a mandatory data quality gate fails."""


class TransformationException(YouTubePipelineException):
    """Raised when a business transformation cannot be completed."""


class DimensionBuildException(YouTubePipelineException):
    """Raised when a dimension cannot be created."""


class FactBuildException(YouTubePipelineException):
    """Raised when the fact table cannot be created."""


class WriteException(YouTubePipelineException):
    """Raised when an output write fails."""


class NotificationException(YouTubePipelineException):
    """Raised when SNS notification fails."""


class EmptyDataException(YouTubePipelineException):
    """Raised when a required dataset contains zero rows."""