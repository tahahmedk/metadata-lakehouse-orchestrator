"""Read facade; only AuditStore.finish may advance durable watermarks."""

from .audit import AuditStore


class WatermarkStore:
    def __init__(self, audit: AuditStore):
        self.audit = audit

    def get(self, pipeline: str) -> int:
        return self.audit.watermark(pipeline)
