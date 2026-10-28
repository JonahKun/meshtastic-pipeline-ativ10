"""Configuracao via variaveis de ambiente (.env). Credenciais nunca ficam no codigo."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    mongo_uri: str = ""
    mongo_db: str = ""
    mongo_collection: str = "pacotes_brutos"
    fonte_formato: str = "auto"  # auto | pacotes_brutos | legado
    mariadb_url: str = ""
    redis_url: str = ""
    prefixo: str = "jonah"
    poll_segundos: float = 10.0
    lote: int = 500
    sobreposicao_segundos: int = 120
    intervalo_esperado_min: int = 30  # minimo de telemetria do firmware no canal publico

    @property
    def limite_lacuna_s(self) -> int:
        """Lacuna = intervalo maior que 2x o intervalo esperado."""
        return 2 * self.intervalo_esperado_min * 60


def carregar(usar_dotenv: bool = True) -> Config:
    if usar_dotenv:
        try:
            from dotenv import load_dotenv

            load_dotenv()
        except ImportError:  # python-dotenv e opcional em runtime
            pass
    env = os.environ
    return Config(
        mongo_uri=env.get("MONGO_URI", ""),
        mongo_db=env.get("MONGO_DB", ""),
        mongo_collection=env.get("MONGO_COLLECTION", "pacotes_brutos"),
        fonte_formato=env.get("FONTE_FORMATO", "auto").strip().lower(),
        mariadb_url=env.get("MARIADB_URL", ""),
        redis_url=env.get("REDIS_URL", ""),
        prefixo=env.get("PREFIXO", "jonah"),
        poll_segundos=float(env.get("POLL_SEGUNDOS", "10")),
        lote=int(env.get("LOTE", "500")),
        sobreposicao_segundos=int(env.get("SOBREPOSICAO_SEGUNDOS", "120")),
        intervalo_esperado_min=int(env.get("INTERVALO_ESPERADO_MIN", "30")),
    )


def validar(cfg: Config) -> list[str]:
    erros = []
    for campo in ("mongo_uri", "mongo_db", "mariadb_url", "redis_url"):
        if not getattr(cfg, campo):
            erros.append(f"{campo.upper()} nao configurado (veja .env.example)")
    if not cfg.prefixo.isidentifier():
        erros.append("PREFIXO invalido")
    if cfg.fonte_formato not in ("auto", "pacotes_brutos", "legado"):
        erros.append("FONTE_FORMATO deve ser auto, pacotes_brutos ou legado")
    if cfg.lote < 1:
        erros.append("LOTE deve ser >= 1")
    return erros
