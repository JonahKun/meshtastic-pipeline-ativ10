"""Utilitarios de teste: documentos sinteticos e montagem do pipeline sem servicos reais."""
from __future__ import annotations

from datetime import datetime, timedelta

import fakeredis
import mongomock
from bson import ObjectId

from pipeline.config import Config
from pipeline.destinos import Destinos, criar_engine
from pipeline.fonte import Fonte
from pipeline.main import Pipeline

T0 = datetime(2026, 10, 20, 12, 0, 0)  # instante base de recebimento


def ts(dt: datetime) -> int:
    """datetime ingenuo UTC -> epoch em segundos (como o campo `timestamp` do Meshtastic)."""
    return int((dt - datetime(1970, 1, 1)).total_seconds())


def telemetria(no=0xAABBCCDD, pid=1, recebido=T0, tensao=3.9, bateria=80, canal=5.0, air=1.0,
               uptime=1000, sender="!11111111", timestamp="auto", extra=None, tipo="telemetry",
               payload=None):
    msg = {
        "from": no, "to": 0xFFFFFFFF, "id": pid, "sender": sender, "type": tipo,
        "timestamp": ts(recebido) - 3 if timestamp == "auto" else timestamp,
        "hops_away": 1, "rssi": -90, "snr": 6.5, "channel": 0,
        "payload": payload if payload is not None else {
            "battery_level": bateria, "voltage": tensao, "channel_utilization": canal,
            "air_util_tx": air, "uptime_seconds": uptime,
        },
    }
    if extra:
        msg.update(extra)
    return {"_id": ObjectId(), "topico": f"msh/BR_902/2/json/LongFast/{sender}", "recebido_em": recebido, "mensagem": msg}


def bruto_invalido(recebido=T0):
    return {"_id": ObjectId(), "topico": "msh/BR_902/2/json/LongFast/!11111111", "recebido_em": recebido,
            "mensagem_raw": "{isto nao e json"}


def cfg(**kw) -> Config:
    base = dict(lote=500, sobreposicao_segundos=120, intervalo_esperado_min=15, poll_segundos=0.01)
    base.update(kw)
    return Config(**base)


class Ambiente:
    """Mongo (mongomock) + MariaDB (SQLite em arquivo) + Redis (fakeredis)."""

    def __init__(self, caminho_db, colecao=None, redis_client=None, **cfg_kw):
        self.url = f"sqlite:///{caminho_db}"
        self.colecao = colecao if colecao is not None else mongomock.MongoClient()["teste"]["pacotes_brutos"]
        self.redis = redis_client if redis_client is not None else fakeredis.FakeRedis(decode_responses=True)
        self.cfg = cfg(**cfg_kw)
        self.novo_processo()

    def novo_processo(self):
        """Simula (re)inicio do processo: novo engine, novo Destinos, novo Pipeline."""
        self.engine = criar_engine(self.url)
        self.destinos = Destinos(self.engine, self.redis, "jonah", self.cfg.limite_lacuna_s)
        self.pipeline = Pipeline(self.cfg, Fonte(self.colecao), self.destinos)
        self.pipeline.iniciar()
        return self.pipeline

    def inserir(self, *docs):
        self.colecao.insert_many(list(docs))

    def consulta(self, sql, **params):
        from sqlalchemy import text
        with self.engine.connect() as c:
            return [tuple(r) for r in c.execute(text(sql), params)]

    def estado(self):
        """Foto comparavel do MariaDB (serie, vistos, contadores, agregados, quarentena)."""
        return {
            "serie": self.consulta("select no,pacote_id,metrica,instante,valor,instante_no_valido from jonah_serie order by 1,2,3"),
            "vistos": self.consulta("select no,pacote_id,bruto_id from jonah_pacotes_vistos order by 1,2"),
            "contadores": self.consulta("select nome,valor from jonah_contadores order by 1"),
            "agg_hora": self.consulta("select * from jonah_agg_hora order by 1,2,3"),
            "agg_dia": self.consulta("select * from jonah_agg_dia order by 1,2,3"),
            "quarentena": self.consulta("select bruto_id,motivo from jonah_quarentena order by 1"),
            "duplicatas": self.consulta("select bruto_id from jonah_duplicatas order by 1"),
        }

    def contadores(self):
        return self.destinos.ler_contadores()


def minutos(n):
    return timedelta(minutes=n)
