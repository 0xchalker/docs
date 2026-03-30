"""
Parquet storage backend using pandas and pyarrow.
Partitioned by date for efficient time-range queries.
"""
import os
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from core.models import TradeJournal, FeatureSnapshot
from utils.logging_utils import get_logger

logger = get_logger(__name__)

_DEFAULT_BASE_PATH = "data/parquet"


class ParquetStore:
    """
    Append-only Parquet store partitioned by date.
    Directory layout:
      data/parquet/trades/date=YYYY-MM-DD/part-*.parquet
      data/parquet/feature_snapshots/date=YYYY-MM-DD/part-*.parquet
    """

    def __init__(self, base_path: str = _DEFAULT_BASE_PATH) -> None:
        self._base = Path(base_path)
        self._trades_path = self._base / "trades"
        self._snaps_path = self._base / "feature_snapshots"
        self._trades_path.mkdir(parents=True, exist_ok=True)
        self._snaps_path.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------------
    # Trades
    # -------------------------------------------------------------------------

    def append_trades(self, df: pd.DataFrame) -> None:
        """Append a DataFrame of trade records, partitioned by entry date."""
        if df.empty:
            return
        if "entry_time" not in df.columns:
            logger.warning("append_trades: missing entry_time column")
            return

        df = df.copy()
        df["entry_time"] = pd.to_datetime(df["entry_time"], utc=True)
        df["date"] = df["entry_time"].dt.date.astype(str)

        for trade_date, group_df in df.groupby("date"):
            partition_dir = self._trades_path / f"date={trade_date}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
            out_path = partition_dir / f"part-{ts}.parquet"
            table = pa.Table.from_pandas(group_df.drop(columns=["date"]))
            pq.write_table(table, str(out_path), compression="snappy")
            logger.debug("Appended trades parquet", path=str(out_path), rows=len(group_df))

    def read_trades(
        self,
        symbol: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Read trade records filtered by symbol and date range."""
        if not self._trades_path.exists():
            return pd.DataFrame()

        # Collect matching partition directories
        partition_dirs = []
        for child in sorted(self._trades_path.iterdir()):
            if not child.name.startswith("date="):
                continue
            part_date = child.name.replace("date=", "")
            try:
                part_dt = date.fromisoformat(part_date)
            except ValueError:
                continue
            if start and part_dt < start.date():
                continue
            if end and part_dt > end.date():
                continue
            partition_dirs.append(str(child))

        if not partition_dirs:
            return pd.DataFrame()

        frames = []
        for pdir in partition_dirs:
            for pq_file in Path(pdir).glob("*.parquet"):
                try:
                    df = pq.read_table(str(pq_file)).to_pandas()
                    frames.append(df)
                except Exception as exc:
                    logger.warning("Failed to read parquet", path=str(pq_file), error=str(exc))

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)

        if "entry_time" in result.columns:
            result["entry_time"] = pd.to_datetime(result["entry_time"], utc=True)
            if start:
                result = result[result["entry_time"] >= pd.Timestamp(start, tz="UTC")]
            if end:
                result = result[result["entry_time"] <= pd.Timestamp(end, tz="UTC")]

        if symbol and "symbol" in result.columns:
            result = result[result["symbol"] == symbol]

        return result.reset_index(drop=True)

    def append_trades_from_journal(self, trades: List[TradeJournal]) -> None:
        """Convenience: convert TradeJournal objects to DataFrame and append."""
        if not trades:
            return
        import dataclasses
        rows = []
        for t in trades:
            row = dataclasses.asdict(t)
            # Convert datetime fields to ISO strings
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()
            rows.append(row)
        df = pd.DataFrame(rows)
        self.append_trades(df)

    # -------------------------------------------------------------------------
    # Feature snapshots
    # -------------------------------------------------------------------------

    def append_feature_snapshots(self, df: pd.DataFrame) -> None:
        """Append a DataFrame of feature snapshots, partitioned by date."""
        if df.empty:
            return
        if "timestamp" not in df.columns:
            logger.warning("append_feature_snapshots: missing timestamp column")
            return

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["date"] = df["timestamp"].dt.date.astype(str)

        for snap_date, group_df in df.groupby("date"):
            partition_dir = self._snaps_path / f"date={snap_date}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
            out_path = partition_dir / f"part-{ts}.parquet"
            table = pa.Table.from_pandas(group_df.drop(columns=["date"]))
            pq.write_table(table, str(out_path), compression="snappy")
            logger.debug("Appended snapshot parquet", path=str(out_path), rows=len(group_df))

    def read_feature_snapshots(
        self,
        symbol: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Read feature snapshots for a given symbol and date range."""
        if not self._snaps_path.exists():
            return pd.DataFrame()

        frames = []
        for child in sorted(self._snaps_path.iterdir()):
            if not child.name.startswith("date="):
                continue
            part_date = child.name.replace("date=", "")
            try:
                part_dt = date.fromisoformat(part_date)
            except ValueError:
                continue
            if start and part_dt < start.date():
                continue
            if end and part_dt > end.date():
                continue
            for pq_file in child.glob("*.parquet"):
                try:
                    df = pq.read_table(str(pq_file)).to_pandas()
                    frames.append(df)
                except Exception as exc:
                    logger.warning("Failed to read snapshot parquet", path=str(pq_file), error=str(exc))

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)
        if "timestamp" in result.columns:
            result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True)
            if start:
                result = result[result["timestamp"] >= pd.Timestamp(start, tz="UTC")]
            if end:
                result = result[result["timestamp"] <= pd.Timestamp(end, tz="UTC")]

        if symbol and "symbol" in result.columns:
            result = result[result["symbol"] == symbol]

        return result.reset_index(drop=True)
