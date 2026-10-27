from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, Dict, Any

@dataclass
class PipelineMetrics:
    """Holds run-level metrics."""

    started_at: datetime = field(default_factory=datetime.utcnow)
    ended_at: Optional[datetime] = None
    source_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    deduplicated_rows: int = 0
    main_rows: int = 0
    fact_rows: int = 0

    def finish(self) -> None:
        self.ended_at = datetime.utcnow()

    @property
    def duration_seconds(self) -> Optional[float]:
        if not self.ended_at:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    def as_dict(self) -> Dict[str, Any]:
        return {
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "source_rows": self.source_rows,
            "valid_rows": self.valid_rows,
            "invalid_rows": self.invalid_rows,
            "deduplicated_rows": self.deduplicated_rows,
            "main_rows": self.main_rows,
            "fact_rows": self.fact_rows,
        }