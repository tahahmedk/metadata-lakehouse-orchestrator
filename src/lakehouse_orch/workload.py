def classify(kind: str, estimated_minutes: float, interactive: bool = False) -> str:
    if interactive:
        return "interactive"
    if kind in {"full-load", "backfill"} or estimated_minutes >= 30:
        return "etl-heavy"
    if kind in {"ad-hoc", "exploration"}:
        return "ad-hoc"
    return "etl-standard"
