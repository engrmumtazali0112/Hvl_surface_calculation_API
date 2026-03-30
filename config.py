"""
config.py — Configuration and environment helpers for HVL Surface API.
"""
import os
from dataclasses import dataclass, field
from typing import List


# ---------------------------------------------------------------------------
# Environment → Login DB mapping
# ---------------------------------------------------------------------------
LOGIN_DB_MAP = {
    "prod": "Login",
    "dev":  "Login-dev",
    "uat":  "Login-uat",
    "test": "Login-test",
}


@dataclass
class Config:
    db_engine:          str   = "sqlserver"
    db_server:          str   = "host8728.shserver.it"
    db_port:            int   = 1438
    db_user:            str   = "sa"
    db_password:        str   = ""
    connection_timeout: int   = 60
    login_dbs:          List[str] = field(default_factory=lambda: list(LOGIN_DB_MAP.values()))

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            db_engine          = os.getenv("DB_ENGINE", "sqlserver"),
            db_server          = os.getenv("DB_SERVER", "host8728.shserver.it"),
            db_port            = int(os.getenv("DB_PORT", "1438")),
            db_user            = os.getenv("DB_USER", "sa"),
            db_password        = os.getenv("DB_PASSWORD", "NmlILt68qnWCQT7Eog"),
            connection_timeout = int(os.getenv("CONNECTION_TIMEOUT", "60")),
        )

    def login_db_for_env(self, environment: str) -> str:
        env = environment.lower().strip()
        if env not in LOGIN_DB_MAP:
            raise ValueError(
                f"Unknown environment {environment!r}. "
                f"Valid options: {list(LOGIN_DB_MAP.keys())}"
            )
        return LOGIN_DB_MAP[env]

    def print_config(self):
        import pyodbc
        drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
        driver  = drivers[0] if drivers else "NOT FOUND"
        print("=" * 60)
        print("HVL Surface API Configuration")
        print("=" * 60)
        print(f"Database Engine:  {self.db_engine}")
        print(f"Connection Type:  SQL Server Authentication")
        print(f"Server:           {self.db_server}")
        print(f"Port:             {self.db_port}")
        print(f"User:             {self.db_user}")
        print(f"Driver:           {driver}")
        print(f"Login DBs:        {self.login_dbs}")
        print(f"Timeout:          {self.connection_timeout}s")
        print("=" * 60)


# Singleton
cfg = Config.from_env()