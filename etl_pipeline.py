"""
=============================================================
 Multi-Source ETL Pipeline with Orchestration & Health Monitor
 Author  : Prince Kumar Gupta
 Role    : Data Analyst
 Tools   : Python, Pandas, SQLite/Snowflake, Logging, Schedule
=============================================================
"""

import pandas as pd
import numpy as np
import sqlite3
import logging
import json
import os
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | [%(name)s] %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler()
    ]
)

# ── Enums ─────────────────────────────────────────────────────────
class PipelineStatus(Enum):
    PENDING   = "PENDING"
    RUNNING   = "RUNNING"
    SUCCESS   = "SUCCESS"
    FAILED    = "FAILED"
    SKIPPED   = "SKIPPED"

class DataSource(Enum):
    CSV_FILE     = "csv_file"
    JSON_API     = "json_api"
    SQL_DATABASE = "sql_database"
    EXCEL_FILE   = "excel_file"
    FLAT_FILE    = "flat_file"

# ── Data Classes ─────────────────────────────────────────────────
@dataclass
class PipelineMetric:
    source_name   : str
    source_type   : str
    records_in    : int = 0
    records_out   : int = 0
    records_dropped: int = 0
    start_time    : Optional[datetime] = None
    end_time      : Optional[datetime] = None
    status        : str = PipelineStatus.PENDING.value
    error_message : str = ""

    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def drop_rate(self) -> float:
        if self.records_in > 0:
            return round(self.records_dropped / self.records_in * 100, 2)
        return 0.0

@dataclass
class PipelineRun:
    run_id       : str = field(default_factory=lambda: f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    start_time   : datetime = field(default_factory=datetime.now)
    end_time     : Optional[datetime] = None
    metrics      : List[PipelineMetric] = field(default_factory=list)
    total_records: int = 0
    status       : str = PipelineStatus.RUNNING.value

    @property
    def uptime_pct(self) -> float:
        success = len([m for m in self.metrics if m.status == PipelineStatus.SUCCESS.value])
        total   = len(self.metrics)
        return round(success / total * 100, 1) if total > 0 else 0.0

# ── Data Source Simulators ────────────────────────────────────────
class DataSourceFactory:
    """Simulates 6 different data sources."""

    @staticmethod
    def source_sales_csv(n: int = 1000) -> pd.DataFrame:
        """Source 1: Sales flat file (CSV)"""
        np.random.seed(1)
        return pd.DataFrame({
            "sale_id"  : [f"S{i:05d}" for i in range(n)],
            "date"     : [(datetime.now() - timedelta(days=i % 90)).strftime("%Y-%m-%d") for i in range(n)],
            "region"   : np.random.choice(["North","South","East","West"], n),
            "product"  : np.random.choice(["A","B","C","D"], n),
            "revenue"  : np.round(np.random.uniform(1000, 100000, n), 2),
            "units"    : np.random.randint(1, 100, n),
            "source"   : "CSV_SALES",
        })

    @staticmethod
    def source_customer_json(n: int = 800) -> pd.DataFrame:
        """Source 2: Customer API response (JSON)"""
        np.random.seed(2)
        return pd.DataFrame({
            "customer_id" : [f"C{i:05d}" for i in range(n)],
            "name"        : [f"Customer_{i}" for i in range(n)],
            "email"       : [f"c{i}@mail.com" if np.random.rand() > 0.05 else None for i in range(n)],
            "segment"     : np.random.choice(["Premium","Standard","Basic"], n),
            "tenure_days" : np.random.randint(1, 1000, n),
            "churn_risk"  : np.round(np.random.uniform(0, 1, n), 3),
            "source"      : "JSON_API",
        })

    @staticmethod
    def source_finance_db(n: int = 600) -> pd.DataFrame:
        """Source 3: Finance SQL database"""
        np.random.seed(3)
        return pd.DataFrame({
            "txn_id"    : [f"TXN{i:06d}" for i in range(n)],
            "account"   : [f"ACC{np.random.randint(1000,9999)}" for _ in range(n)],
            "debit"     : np.round(np.random.uniform(0, 50000, n), 2),
            "credit"    : np.round(np.random.uniform(0, 50000, n), 2),
            "balance"   : np.round(np.random.uniform(10000, 500000, n), 2),
            "txn_date"  : [(datetime.now()-timedelta(days=i%60)).strftime("%Y-%m-%d") for i in range(n)],
            "source"    : "SQL_FINANCE",
        })

    @staticmethod
    def source_inventory_excel(n: int = 400) -> pd.DataFrame:
        """Source 4: Inventory Excel export"""
        np.random.seed(4)
        return pd.DataFrame({
            "sku"       : [f"SKU-{i:04d}" for i in range(n)],
            "product"   : [f"Product_{chr(65 + i % 26)}" for i in range(n)],
            "quantity"  : np.random.randint(0, 5000, n),
            "warehouse" : np.random.choice(["WH-North","WH-South","WH-East"], n),
            "reorder_pt": np.random.randint(50, 500, n),
            "unit_cost" : np.round(np.random.uniform(10, 1000, n), 2),
            "source"    : "EXCEL_INVENTORY",
        })

    @staticmethod
    def source_hr_flat_file(n: int = 300) -> pd.DataFrame:
        """Source 5: HR flat file"""
        np.random.seed(5)
        depts = ["Analytics","Engineering","Finance","Sales","Operations"]
        return pd.DataFrame({
            "emp_id"    : [f"EMP{i:04d}" for i in range(n)],
            "department": np.random.choice(depts, n),
            "salary"    : np.round(np.random.uniform(25000, 200000, n), 0),
            "join_date" : [(datetime.now()-timedelta(days=np.random.randint(30,3650))).strftime("%Y-%m-%d") for _ in range(n)],
            "rating"    : np.random.choice([1,2,3,4,5], n),
            "active"    : np.random.choice([True, False], n, p=[0.9, 0.1]),
            "source"    : "FLAT_FILE_HR",
        })

    @staticmethod
    def source_web_logs(n: int = 2000) -> pd.DataFrame:
        """Source 6: Web/app event logs"""
        np.random.seed(6)
        events = ["page_view","click","purchase","login","logout","search"]
        return pd.DataFrame({
            "event_id"  : [f"EVT{i:07d}" for i in range(n)],
            "user_id"   : [f"U{np.random.randint(1000,9999)}" for _ in range(n)],
            "event_type": np.random.choice(events, n),
            "timestamp" : [(datetime.now()-timedelta(seconds=i*30)).strftime("%Y-%m-%d %H:%M:%S") for i in range(n)],
            "session_id": [f"SES{np.random.randint(10000,99999)}" for _ in range(n)],
            "device"    : np.random.choice(["mobile","desktop","tablet"], n, p=[0.55,0.35,0.10]),
            "source"    : "WEB_LOGS",
        })

# ── Transformation Engine ─────────────────────────────────────────
class TransformationEngine:
    """Applies standardised transformations to each source."""

    @staticmethod
    def clean_common(df: pd.DataFrame, metric: PipelineMetric) -> pd.DataFrame:
        """Common cleaning applied to all sources."""
        before = len(df)
        df = df.drop_duplicates()
        df = df.dropna(subset=[c for c in df.columns if c not in ["email"]])
        after = len(df)
        metric.records_dropped = before - after
        return df

    @staticmethod
    def transform_sales(df: pd.DataFrame) -> pd.DataFrame:
        df["revenue"]     = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
        df["units"]       = pd.to_numeric(df["units"],   errors="coerce").fillna(0)
        df["revenue_per_unit"] = (df["revenue"] / df["units"].replace(0, np.nan)).round(2)
        df["date"]        = pd.to_datetime(df["date"], errors="coerce")
        df["month"]       = df["date"].dt.to_period("M").astype(str)
        df["high_value"]  = df["revenue"] > df["revenue"].quantile(0.9)
        return df

    @staticmethod
    def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
        df["email"]       = df["email"].fillna("unknown@na.com")
        df["churn_label"] = pd.cut(df["churn_risk"],
                                   bins=[0, 0.3, 0.6, 1.0],
                                   labels=["Low","Medium","High"])
        df["tenure_yrs"]  = (df["tenure_days"] / 365).round(1)
        return df

    @staticmethod
    def transform_finance(df: pd.DataFrame) -> pd.DataFrame:
        df["net_flow"]    = df["credit"] - df["debit"]
        df["txn_date"]    = pd.to_datetime(df["txn_date"], errors="coerce")
        df["is_deficit"]  = df["net_flow"] < 0
        return df

    @staticmethod
    def apply(source_name: str, df: pd.DataFrame) -> pd.DataFrame:
        transforms = {
            "CSV_SALES"      : TransformationEngine.transform_sales,
            "JSON_API"       : TransformationEngine.transform_customers,
            "SQL_FINANCE"    : TransformationEngine.transform_finance,
        }
        fn = transforms.get(source_name)
        return fn(df) if fn else df

# ── Loader ───────────────────────────────────────────────────────
class DataLoader:
    def __init__(self, db_path: str = "data_warehouse.db"):
        self.db_path = db_path
        self.conn    = sqlite3.connect(db_path)
        self.log     = logging.getLogger("Loader")

    def load(self, df: pd.DataFrame, table_name: str, if_exists: str = "append"):
        df.to_sql(table_name, self.conn, if_exists=if_exists, index=False)
        self.log.info(f"✅ Loaded {len(df):,} rows → table '{table_name}'")

    def get_table_count(self, table: str) -> int:
        try:
            return pd.read_sql_query(f"SELECT COUNT(*) AS n FROM {table}", self.conn).iloc[0,0]
        except:
            return 0

    def close(self):
        self.conn.close()

# ── Pipeline Orchestrator ─────────────────────────────────────────
class ETLOrchestrator:
    def __init__(self):
        self.log     = logging.getLogger("Orchestrator")
        self.loader  = DataLoader()
        self.run     = PipelineRun()
        self.factory = DataSourceFactory()
        self.engine  = TransformationEngine()

    def _run_source(self, name: str, source_fn, table: str, transform_name: str = "") -> bool:
        metric = PipelineMetric(source_name=name, source_type=table)
        metric.start_time = datetime.now()
        metric.status     = PipelineStatus.RUNNING.value
        self.log.info(f"▶ Extracting from: {name}")

        try:
            df = source_fn()
            metric.records_in = len(df)
            df = TransformationEngine.clean_common(df, metric)

            t_name = transform_name or name
            df = TransformationEngine.apply(t_name, df)

            # Convert non-SQL-safe columns
            for col in df.select_dtypes(include=["datetime64", "period"]).columns:
                df[col] = df[col].astype(str)
            for col in df.select_dtypes(include=["bool"]).columns:
                df[col] = df[col].astype(int)

            self.loader.load(df, table, if_exists="replace")
            metric.records_out  = len(df)
            metric.status       = PipelineStatus.SUCCESS.value
            metric.end_time     = datetime.now()
            self.run.total_records += len(df)
            self.log.info(f"✅ {name}: {metric.records_in:,} in → {metric.records_out:,} out "
                          f"({metric.records_dropped} dropped) [{metric.duration_seconds:.1f}s]")
            self.run.metrics.append(metric)
            return True

        except Exception as e:
            metric.status        = PipelineStatus.FAILED.value
            metric.error_message = str(e)
            metric.end_time      = datetime.now()
            self.log.error(f"❌ {name} FAILED: {e}")
            self.run.metrics.append(metric)
            return False

    def run_pipeline(self):
        self.log.info("=" * 65)
        self.log.info(f"🚀 ETL PIPELINE STARTED | Run ID: {self.run.run_id}")
        self.log.info("=" * 65)

        sources = [
            ("Source 1: Sales CSV",         self.factory.source_sales_csv,     "fact_sales",     "CSV_SALES"),
            ("Source 2: Customer JSON API",  self.factory.source_customer_json, "dim_customers",  "JSON_API"),
            ("Source 3: Finance SQL DB",     self.factory.source_finance_db,    "fact_finance",   "SQL_FINANCE"),
            ("Source 4: Inventory Excel",    self.factory.source_inventory_excel,"dim_inventory", ""),
            ("Source 5: HR Flat File",       self.factory.source_hr_flat_file,  "dim_employees",  ""),
            ("Source 6: Web Event Logs",     self.factory.source_web_logs,      "fact_web_events",""),
        ]

        for name, fn, table, t_name in sources:
            self._run_source(name, fn, table, t_name)
            time.sleep(0.1)  # Simulate I/O latency

        self.run.end_time = datetime.now()
        self.run.status   = PipelineStatus.SUCCESS.value
        self._save_health_metrics()
        self._print_run_summary()

    def _save_health_metrics(self):
        """Persist pipeline health metrics to monitoring table."""
        records = []
        for m in self.run.metrics:
            records.append({
                "run_id"       : self.run.run_id,
                "source_name"  : m.source_name,
                "status"       : m.status,
                "records_in"   : m.records_in,
                "records_out"  : m.records_out,
                "drop_rate_pct": m.drop_rate,
                "duration_sec" : m.duration_seconds,
                "run_time"     : self.run.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        pd.DataFrame(records).to_sql("pipeline_health", self.loader.conn,
                                      if_exists="append", index=False)
        self.log.info("✅ Health metrics saved to monitoring table.")

    def _print_run_summary(self):
        total_dur = (self.run.end_time - self.run.start_time).total_seconds()
        print("\n" + "="*65)
        print(f"  ETL PIPELINE RUN SUMMARY — {self.run.run_id}")
        print("="*65)
        print(f"  Duration       : {total_dur:.1f} seconds")
        print(f"  Total Records  : {self.run.total_records:,}")
        print(f"  Uptime Score   : {self.run.uptime_pct}%")
        print(f"  Sources Run    : {len(self.run.metrics)}")
        print("-"*65)
        for m in self.run.metrics:
            icon = "✅" if m.status == "SUCCESS" else "❌"
            print(f"  {icon} {m.source_name:<35} | {m.records_out:>6,} rows | {m.duration_seconds:.1f}s")
        print("="*65 + "\n")

    def close(self):
        self.loader.close()

# ── Main ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    orchestrator = ETLOrchestrator()
    try:
        orchestrator.run_pipeline()
    finally:
        orchestrator.close()
