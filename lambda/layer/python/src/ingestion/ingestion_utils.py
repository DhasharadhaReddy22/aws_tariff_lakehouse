from datetime import datetime, timezone

def build_raw_key(
    domain: str,
    source: str,
    dataset: str,
    ingestion_date: str,
    filename: str,
) -> str:
    """
    Build a Hive-style S3 key for raw ingestion data.

    Example:
    raw/macroeconomics/source=imf/dataset=indicators/date=2025-01-02/data.jsonl
    """

    if not ingestion_date:
        ingestion_date = datetime.now(timezone.utc).isoformat()

    return f"bronze/{domain}/source={source}/dataset={dataset}/ingestion_date={ingestion_date}/{filename}"

def normalize_utc_datetime(dt_str: str) -> str:
    """
    Convert 'YYYY-MM-DD HH:MM:SS' → ISO-8601 UTC string
    """
    return (
        datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        .replace(tzinfo=timezone.utc)
        .isoformat()
    )

def create_filename(prefix: str, iso_ts: str, ext: str = ".jsonl") -> str:
    """
    Convert ISO-8601 timestamp to a filesystem/S3-safe filename.

    Example:
    iso_ts = "2025-12-16T18:18:30.123456+00:00"
    → prefix_20251216_181830.jsonl
    """
    ts = (
        iso_ts.replace(":", "")
              .replace("-", "")
              .replace("T", "_")
              .split(".")[0]  # drop subseconds & timezone
    )
    return f"{prefix}_{ts}{ext}"