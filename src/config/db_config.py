from __future__ import annotations

from dataclasses import dataclass
import os

from .env import load_env_file


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str = "prefer"

    def to_dsn(self) -> str:
        return (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.database} "
            f"user={self.user} "
            f"password={self.password} "
            f"sslmode={self.sslmode}"
        )


@dataclass(frozen=True)
class ElasticsearchConfig:
    base_url: str
    index_name: str = "treasury_audit_logs"


def load_elasticsearch_config() -> ElasticsearchConfig:
    load_env_file()
    return ElasticsearchConfig(
        base_url=os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
        index_name=os.getenv("ELASTICSEARCH_INDEX", "treasury_audit_logs"),
    )


def load_database_config() -> DatabaseConfig:
    load_env_file()
    return DatabaseConfig(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "treasury"),
        user=os.getenv("POSTGRES_USER", "treasury"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
    )
