"""Fonte: leitura SOMENTE LEITURA da colecao de pacotes brutos no MongoDB.

Este modulo so usa `find`/`find_one`. Nunca grava, atualiza nem apaga na origem.
O formato dos documentos (contrato `pacotes_brutos` ou esquema legado `iot.sensores`)
e tratado pelo adaptador (`pipeline/adaptador.py`).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from . import adaptador


def conectar(uri: str, banco: str, colecao: str):
    from pymongo import MongoClient

    cliente = MongoClient(uri, serverSelectionTimeoutMS=10000, appname="jonah-pipeline")
    return cliente, cliente[banco][colecao]


class Fonte:
    def __init__(self, colecao, formato: str = "auto"):
        if formato not in adaptador.FORMATOS:
            raise ValueError(f"FONTE_FORMATO invalido: {formato!r} (use {adaptador.FORMATOS})")
        self.colecao = colecao
        self.formato = formato
        self._campo: Optional[str] = adaptador.CAMPO_TEMPO.get(formato)

    @property
    def campo_tempo(self) -> Optional[str]:
        """Campo do horario de recebimento; em `auto`, detectado no primeiro documento."""
        if self._campo is None:
            amostra = self.colecao.find_one()
            if amostra is not None:
                self._campo = adaptador.CAMPO_TEMPO[adaptador.formato_efetivo(amostra, "auto")]
        return self._campo

    def normalizar(self, doc: dict) -> dict:
        return adaptador.normalizar(doc, self.formato)

    def tempo(self, doc: dict):
        return adaptador.tempo_do_documento(doc, self.formato)

    def ler_lote(
        self,
        desde: datetime,
        apos_id: Optional[Any] = None,
        limite: int = 500,
        apos_recebido_em: Optional[datetime] = None,
    ) -> list[dict]:
        """Le ate `limite` documentos ordenados por (horario de recebimento, _id).

        Primeira pagina: horario >= desde (inclusive; a releitura e absorvida
        pela deduplicacao). Paginas seguintes: estritamente depois do par
        (apos_recebido_em, apos_id), usando _id como desempate.
        """
        campo = self.campo_tempo
        if campo is None:  # colecao ainda vazia
            return []
        if apos_id is None:
            filtro: dict = {campo: {"$gte": desde}}
        else:
            filtro = {
                "$or": [
                    {campo: {"$gt": apos_recebido_em}},
                    {campo: apos_recebido_em, "_id": {"$gt": apos_id}},
                ]
            }
        return list(self.colecao.find(filtro).sort([(campo, 1), ("_id", 1)]).limit(limite))
