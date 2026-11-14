"""Configuración externalizada: config/pipeline.toml + variables de entorno.

Precedencia: variable de entorno > config/astra.env > pipeline.toml.
"""
from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(os.environ.get("CPA_ROOT", Path(__file__).resolve().parents[2]))


def _load_env_file(path: Path) -> dict[str, str]:
    """Parser mínimo de KEY=VALUE (sin dependencias). Ignora comentarios y líneas vacías."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


@dataclass(frozen=True)
class Config:
    raw: dict
    root: Path

    # --- rutas ---------------------------------------------------------------
    def _path(self, value: str) -> Path:
        p = Path(value)
        return p if p.is_absolute() else self.root / p

    @property
    def landing(self) -> Path:
        return self._path(os.environ.get("CPA_LANDING", self.raw["paths"]["landing"]))

    @property
    def lake(self) -> Path:
        return self._path(os.environ.get("CPA_LAKE", self.raw["paths"]["lake"]))

    @property
    def evidence(self) -> Path:
        return self._path(os.environ.get("CPA_EVIDENCE", self.raw["paths"]["evidence"]))

    def zone(self, zone: str, table: str = "") -> str:
        """Ruta de una tabla del lake como string (lo que esperan las APIs de Spark)."""
        p = self.lake / zone
        return str(p / table) if table else str(p)

    def checkpoint(self, query: str) -> str:
        return str(self.lake / "_checkpoints" / query)

    @property
    def meta(self) -> Path:
        return self.lake / "_meta"

    # --- secciones -----------------------------------------------------------
    def section(self, name: str) -> dict:
        return self.raw.get(name, {})

    @property
    def serving(self) -> dict:
        """Serving con overrides de entorno y de config/astra.env (credenciales, nunca versionadas)."""
        s = dict(self.raw["serving"])
        env = {**_load_env_file(self.root / "config" / "astra.env"), **os.environ}
        s["target"] = env.get("CPA_SERVING_TARGET", s["target"])
        s["cassandra_host"] = env.get("CASSANDRA_HOST", s["cassandra_host"])
        s["cassandra_port"] = int(env.get("CASSANDRA_PORT", s["cassandra_port"]))
        s["keyspace"] = env.get("ASTRA_DB_KEYSPACE", s["keyspace"]) if s["target"] == "astra" else s["keyspace"]
        s["astra_token"] = env.get("ASTRA_DB_APPLICATION_TOKEN")
        bundle = env.get("ASTRA_SECURE_CONNECT_BUNDLE")
        s["astra_bundle"] = str(self._path(bundle)) if bundle else None
        return s


def load_config(path: str | os.PathLike | None = None) -> Config:
    path = Path(path or os.environ.get("CPA_CONFIG", REPO_ROOT / "config" / "pipeline.toml"))
    with open(path, "rb") as fh:
        raw = tomllib.load(fh)
    return Config(raw=raw, root=REPO_ROOT)
