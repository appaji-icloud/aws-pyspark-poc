import logging
import sys, os, uuid, json
from typing import Dict, Any


class PipelineLogger:
    """Centralized structured logging wrapper."""

    def __init__(self, name: str = "youtube-video-etl"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                fmt="%(asctime)s | %(levelname)s | %(name)s | "
                    "%(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.run_id = str(uuid.uuid4())

    def info(self, message: str, **context: Any) -> None:
        self._log(logging.INFO, message, context)

    def warning(self, message: str, **context: Any) -> None:
        self._log(logging.WARNING, message, context)

    def error(self, message: str, **context: Any) -> None:
        self._log(logging.ERROR, message, context)

    def exception(self, message: str, **context: Any) -> None:
        payload = self._format_context(context)
        self.logger.exception(
            "%s | run_id=%s%s",
            message,
            self.run_id,
            payload,
        )

    def _log(self, level: int, message: str, context: Dict[str, Any]) -> None:
        payload = self._format_context(context)
        self.logger.log(
            level,
            "%s | run_id=%s%s",
            message,
            self.run_id,
            payload,
        )

    @staticmethod
    def _format_context(context: Dict[str, Any]) -> str:
        if not context:
            return ""
        safe = {}
        for key, value in context.items():
            # Avoid logging secrets accidentally.
            if any(token in key.lower() for token in
                   ("password", "secret", "token", "access_key")):
                safe[key] = "***REDACTED***"
            else:
                safe[key] = value
        return " | " + json.dumps(safe, default=str, sort_keys=True)