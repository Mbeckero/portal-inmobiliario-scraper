"""Export listings to CSV, JSON, and optionally Parquet."""
from __future__ import annotations

import json
from datetime import datetime, date
from pathlib import Path
from typing import Any, Sequence

from src.database import ListingRow, row_to_dict
from src.logging_config import get_logger

logger = get_logger(__name__)


def _serialize(obj: Any) -> Any:
    """JSON serializer for datetime and date objects."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Not serializable: {type(obj)}")


def rows_to_records(rows: Sequence[ListingRow]) -> list[dict[str, Any]]:
    return [row_to_dict(r) for r in rows]


def export_json(
    rows: Sequence[ListingRow],
    output_path: Path,
    indent: int = 2,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    records = rows_to_records(rows)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, ensure_ascii=False, indent=indent, default=_serialize)
    logger.info("export_json", path=str(output_path), records=len(records))
    return output_path


def export_csv(
    rows: Sequence[ListingRow],
    output_path: Path,
) -> Path:
    import pandas as pd

    output_path.parent.mkdir(parents=True, exist_ok=True)
    records = rows_to_records(rows)
    if not records:
        logger.warning("export_csv_empty", path=str(output_path))
        output_path.write_text("", encoding="utf-8")
        return output_path

    df = pd.DataFrame(records)

    # Convert list columns stored as JSON strings back to readable form
    for col in ("features", "image_urls"):
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: "|".join(v) if isinstance(v, list) else v
            )

    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("export_csv", path=str(output_path), rows=len(df))
    return output_path


def export_parquet(
    rows: Sequence[ListingRow],
    output_path: Path,
) -> Path:
    try:
        import pandas as pd
    except ImportError:
        raise RuntimeError("pandas is required for parquet export")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    records = rows_to_records(rows)
    df = pd.DataFrame(records)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    logger.info("export_parquet", path=str(output_path), rows=len(df))
    return output_path


def export_new_listings(
    rows: Sequence[ListingRow],
    output_dir: Path,
    run_date: str | None = None,
) -> dict[str, Path]:
    """Export a 'new listings today' report in both CSV and JSON."""
    date_str = run_date or datetime.utcnow().strftime("%Y-%m-%d")
    paths: dict[str, Path] = {}

    csv_path = output_dir / f"new_listings_{date_str}.csv"
    json_path = output_dir / f"new_listings_{date_str}.json"

    paths["csv"] = export_csv(rows, csv_path)
    paths["json"] = export_json(rows, json_path)
    return paths


def export_changed_listings(
    changes: list[dict[str, Any]],
    output_dir: Path,
    run_date: str | None = None,
) -> Path:
    """Export a 'changed listings' report as JSON."""
    date_str = run_date or datetime.utcnow().strftime("%Y-%m-%d")
    path = output_dir / f"changed_listings_{date_str}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(changes, fh, ensure_ascii=False, indent=2, default=_serialize)
    logger.info("export_changes", path=str(path), count=len(changes))
    return path
