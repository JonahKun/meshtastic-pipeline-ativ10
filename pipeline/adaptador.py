"""Adaptador de formato da fonte: normaliza documentos do Mongo para o contrato interno.

Contrato interno (o mesmo de `pacotes_brutos`, ativ. 9):
    {_id, topico, recebido_em, mensagem: <objeto Meshtastic>}   ou   {..., mensagem_raw: str}

Formatos suportados:
  pacotes_brutos  {topico, recebido_em, mensagem | mensagem_raw}
  legado          esquema do consumidor da turma (iot.sensores):
                  {topic, received_at, payload: "<string JSON>", payload_data: {objeto},
                   data: {objeto} (docs antigos) ou datetime (docs novos)}
                  Objeto Meshtastic: payload_data; senao `data` se for dict; senao
                  parse da string `payload`; JSON invalido vira `mensagem_raw`;
                  sem nada utilizavel, fica sem `mensagem` (vai para a quarentena).
  auto            decide por documento: tem `recebido_em` -> pacotes_brutos;
                  tem `received_at` -> legado.

Modulo puro (sem IO).
"""
from __future__ import annotations

import json
from typing import Any, Optional

FORMATOS = ("auto", "pacotes_brutos", "legado")
CAMPO_TEMPO = {"pacotes_brutos": "recebido_em", "legado": "received_at"}


def detectar_formato(doc: dict) -> Optional[str]:
    if "recebido_em" in doc:
        return "pacotes_brutos"
    if "received_at" in doc:
        return "legado"
    return None


def formato_efetivo(doc: dict, formato: str = "auto") -> str:
    if formato in CAMPO_TEMPO:
        return formato
    return detectar_formato(doc) or "pacotes_brutos"


def tempo_do_documento(doc: dict, formato: str = "auto"):
    """Valor bruto do campo de horario de recebimento (datetime), ou None."""
    return doc.get(CAMPO_TEMPO[formato_efetivo(doc, formato)])


def normalizar(doc: dict, formato: str = "auto") -> dict:
    """Converte um documento da fonte para o contrato interno. Nunca levanta."""
    if not isinstance(doc, dict):
        return doc
    if formato_efetivo(doc, formato) == "pacotes_brutos":
        return doc
    saida: dict[str, Any] = {
        "_id": doc.get("_id"),
        "topico": doc.get("topic"),
        "recebido_em": doc.get("received_at"),
    }
    objeto = doc.get("payload_data")
    if not isinstance(objeto, dict):
        objeto = doc.get("data") if isinstance(doc.get("data"), dict) else None
    if objeto is None:
        payload = doc.get("payload")
        if isinstance(payload, dict):
            objeto = payload
        elif isinstance(payload, (str, bytes)):
            try:
                objeto = json.loads(payload)
            except ValueError:
                saida["mensagem_raw"] = payload if isinstance(payload, str) else payload.decode("utf-8", "replace")
                return saida
    if objeto is not None:
        saida["mensagem"] = objeto
    return saida
