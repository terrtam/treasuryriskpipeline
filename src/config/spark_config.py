from __future__ import annotations

from dataclasses import dataclass
import os

from .env import load_env_file


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    master: str
    shuffle_partitions: int
    warehouse_dir: str | None = None


def load_spark_config() -> SparkConfig:
    load_env_file()
    warehouse_dir = os.getenv("SPARK_WAREHOUSE_DIR")
    return SparkConfig(
        app_name=os.getenv("SPARK_APP_NAME", "TreasuryPipeline"),
        master=os.getenv("SPARK_MASTER", "local[*]"),
        shuffle_partitions=int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")),
        warehouse_dir=warehouse_dir if warehouse_dir else None,
    )
