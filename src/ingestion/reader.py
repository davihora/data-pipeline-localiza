"""
Ingestion module — byte-stream partition reader.

The source CSVs were split at the byte level (not line level), so:
  - part_001.csv  → contains the header + data, possibly ending mid-line
  - part_002..N   → start with the remainder of the previous cut line + data

Strategy: open all partition files as binary streams, concatenate into a
single in-memory bytes buffer, then decode once and parse as a regular CSV.
This naturally reassembles every fragmented line without any custom logic.
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.csv as pac
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = [
    "timestamp",
    "sending_address",
    "receiving_address",
    "amount",
    "transaction_type",
    "location_region",
    "ip_prefix",
    "login_frequency",
    "session_duration",
    "purchase_pattern",
    "age_group",
    "risk_score",
    "anomaly",
]


def _sorted_partition_paths(raw_dir: str | Path) -> list[Path]:
    """Return all part_*.csv files sorted lexicographically."""
    raw_path = Path(raw_dir)
    parts = sorted(raw_path.glob("part_*.csv"))
    if not parts:
        raise FileNotFoundError(f"No part_*.csv files found in {raw_dir}")
    logger.info("Found %d partition(s): %s", len(parts), [p.name for p in parts])
    return parts


def _concat_partitions_to_buffer(partition_paths: list[Path]) -> io.BytesIO:
    """
    Concatenate all partition files into a single bytes buffer.

    Because the split was done at the byte level, the first file
    contains the CSV header.  Subsequent files may start with a
    fragment of the last line of the previous file — the binary
    concatenation reassembles those fragments automatically.
    """
    buffer = io.BytesIO()
    total_bytes = 0
    for path in partition_paths:
        chunk = path.read_bytes()
        buffer.write(chunk)
        total_bytes += len(chunk)
        logger.debug("Read %s (%d bytes)", path.name, len(chunk))

    logger.info("Total bytes read: %d", total_bytes)
    buffer.seek(0)
    return buffer


def read_partitions_to_parquet(raw_dir: str | Path, output_parquet: str | Path) -> dict:
    """
    Read all partitions, reassemble via binary concat, and write to Parquet.

    Returns a dict with ingestion metadata (row_count, file_count, bytes_read).
    """
    partition_paths = _sorted_partition_paths(raw_dir)
    buffer = _concat_partitions_to_buffer(partition_paths)

    # Parse with PyArrow CSV reader (robust, handles any line endings)
    parse_options = pac.ParseOptions(newlines_in_values=False)
    # Treat common null representations as nulls before type conversion
    null_values = ["", "none", "None", "null", "NULL", "N/A", "n/a", "NaN", "nan"]
    convert_options = pac.ConvertOptions(
        column_types={
            "timestamp": pa.int64(),
            "amount": pa.float64(),
            "ip_prefix": pa.string(),
            "login_frequency": pa.int64(),
            "session_duration": pa.int64(),
            "risk_score": pa.float64(),
        },
        null_values=null_values,
    )
    table = pac.read_csv(buffer, parse_options=parse_options, convert_options=convert_options)

    # Validate schema
    missing = [c for c in EXPECTED_COLUMNS if c not in table.schema.names]
    if missing:
        raise ValueError(f"Missing expected columns after ingestion: {missing}")

    # Write to Parquet
    output_path = Path(output_parquet)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="snappy")

    metadata = {
        "row_count": table.num_rows,
        "file_count": len(partition_paths),
        "columns": table.schema.names,
    }
    logger.info("Ingestion complete → %s | rows=%d", output_path, metadata["row_count"])
    return metadata


def validate_raw_inputs(raw_dir: str | Path) -> dict:
    """
    Pre-flight check: verify partition files exist and are non-empty.
    Returns a summary dict for logging.
    """
    partition_paths = _sorted_partition_paths(raw_dir)
    summary: list[dict] = []
    errors: list[str] = []

    for p in partition_paths:
        size = p.stat().st_size
        if size == 0:
            errors.append(f"{p.name} is empty")
        summary.append({"file": p.name, "size_bytes": size})

    if errors:
        raise ValueError(f"Raw input validation failed: {errors}")

    logger.info("Raw input validation passed for %d file(s)", len(partition_paths))
    return {"files": summary, "total_files": len(partition_paths)}
