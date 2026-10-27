"""Transformacao pura: documento bruto (Mongo) -> resultado validado.

Este modulo NAO faz IO (sem banco, sem rede, sem relogio do sistema). Toda a
logica de parse, validacao, normalizacao e extracao de metricas fica aqui para
ser testada sem servicos.

Formato de entrada (contrato da ativ. 9):
    {_id, topico, recebido_em: datetime UTC, mensagem: <JSON Meshtastic>}
    {_id, topico, recebido_em, mensagem_raw: str}      (JSON invalido)

Campos usados do JSON Meshtastic (ramo MQTT JSON, firmware ESP32):
    from, to, id, sender, type, timestamp, hops_away, rssi, snr, channel, payload
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

# Relogio do no: antes disso e considerado invalido (RTC sem sincronizar).
INICIO_VALIDO = datetime(2024, 1, 1)
# Relogio do no: alem de recebido_em + 5 min e considerado "no futuro".
TOLERANCIA_FUTURO = timedelta(minutes=5)

# Tipos conhecidos do ramo JSON do Meshtastic. Somente "telemetry" gera metricas
# numericas hoje; os demais sao validados (from/id/tipo), deduplicados e contados
# como "sem_metrica", mas nao vao para a serie.
TIPO_TELEMETRIA = "telemetry"
TIPOS_SEM_METRICAS = frozenset(
    {
        "position",
        "nodeinfo",
        "text",
        "neighborinfo",
        "traceroute",
        "waypoint",
        "mapreport",
        "detection",
        "paxcounter",
        "remotehardware",
        "routing",
    }
)

# metrica canonica -> (nomes aceitos no payload, minimo, maximo)
# Os aliases cobrem snake_case (JSON do firmware) e camelCase (protobuf-JSON de
# outras versoes). battery_level == 101 significa "alimentado por energia externa".
METRICAS: dict[str, tuple[tuple[str, ...], Optional[float], Optional[float]]] = {
    # telemetria de dispositivo
    "battery_level": (("battery_level", "batteryLevel"), 0, 101),
    "voltage": (("voltage",), 0, 60),
    "channel_utilization": (("channel_utilization", "channelUtilization"), 0, 100),
    "air_util_tx": (("air_util_tx", "airUtilTx"), 0, 100),
    "uptime_seconds": (("uptime_seconds", "uptimeSeconds"), 0, None),
    # telemetria ambiental
    "temperature": (("temperature",), -90, 100),
    "relative_humidity": (("relative_humidity", "relativeHumidity"), 0, 100),
    "barometric_pressure": (("barometric_pressure", "barometricPressure"), 300, 1100),
    "gas_resistance": (("gas_resistance", "gasResistance"), 0, None),
    "iaq": (("iaq",), 0, 500),
    "lux": (("lux",), 0, None),
    "soil_moisture": (("soil_moisture", "soilMoisture"), 0, 100),
    "soil_temperature": (("soil_temperature", "soilTemperature"), -90, 100),
}

# Algumas versoes aninham as metricas num sub-objeto do payload.
SUBOBJETOS_PAYLOAD = (
    "device_metrics",
    "deviceMetrics",
    "environment_metrics",
    "environmentMetrics",
)

_RE_NO_HEX = re.compile(r"^![0-9a-fA-F]{1,8}$")


@dataclass
class Resultado:
    bruto_id: str
    recebido_em: Optional[datetime]
    status: str  # "ok" ou "quarentena"
    motivo: Optional[str] = None  # preenchido na quarentena
    detalhe: Optional[str] = None
    no: Optional[str] = None  # "!aabbccdd"
    pacote_id: Optional[int] = None
    gateway: Optional[str] = None  # campo "sender" (quem publicou no MQTT)
    tipo: Optional[str] = None
    instante_no: Optional[datetime] = None
    instante_no_valido: bool = False
    motivo_timestamp: Optional[str] = None  # ausente | antes_2024 | futuro
    hops_away: Optional[int] = None
    hop_start: Optional[int] = None
    rssi: Optional[int] = None
    snr: Optional[float] = None
    medidas: list[tuple[str, float]] = field(default_factory=list)
    info_no: Optional[dict] = None  # nodeinfo: nome_curto, nome_longo, hardware, papel


# ---------------------------------------------------------------- utilitarios


def para_utc_ingenuo(valor: Any) -> Optional[datetime]:
    """datetime (aware ou ingenuo) -> datetime ingenuo em UTC, truncado em ms."""
    if isinstance(valor, str):
        try:
            valor = datetime.fromisoformat(valor.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(valor, datetime):
        return None
    if valor.tzinfo is not None:
        valor = valor.astimezone(timezone.utc).replace(tzinfo=None)
    return valor.replace(microsecond=(valor.microsecond // 1000) * 1000)


def _numero(valor: Any) -> Optional[float]:
    """Numero finito ou None. Booleanos e strings sao rejeitados."""
    if isinstance(valor, bool) or not isinstance(valor, (int, float)):
        return None
    valor = float(valor)
    return valor if math.isfinite(valor) else None


def _inteiro(valor: Any) -> Optional[int]:
    if isinstance(valor, bool):
        return None
    if isinstance(valor, int):
        return valor
    if isinstance(valor, float) and math.isfinite(valor) and valor == int(valor):
        return int(valor)
    if isinstance(valor, str) and valor.strip().lstrip("-").isdigit():
        return int(valor.strip())
    return None


def normalizar_no(valor: Any) -> Optional[str]:
    """`from` pode ser inteiro (uint32, as vezes negativo) ou "!aabbccdd"."""
    if isinstance(valor, str) and _RE_NO_HEX.match(valor):
        return "!" + valor[1:].lower().zfill(8)
    n = _inteiro(valor)
    if n is None:
        return None
    return "!{:08x}".format(n & 0xFFFFFFFF)


def normalizar_id_pacote(valor: Any) -> Optional[int]:
    n = _inteiro(valor)
    if n is None:
        return None
    return n & 0xFFFFFFFF


def avaliar_timestamp_no(ts: Any, recebido_em: datetime) -> tuple[Optional[datetime], bool, Optional[str]]:
    """Avalia o `timestamp` do no. Retorna (instante_no, valido, motivo).

    Invalido se ausente/0/nao numerico, anterior a 2024-01-01 ou posterior a
    recebido_em + 5 min. O valor convertido (quando possivel) e preservado como
    atributo mesmo se invalido, para diagnostico; ele nunca e usado como tempo
    de referencia.
    """
    n = _numero(ts)
    if n is None or n == 0:
        return None, False, "ausente"
    if n > 1e11:  # algumas versoes enviam milissegundos
        n = n / 1000.0
    try:
        instante = datetime(1970, 1, 1) + timedelta(seconds=n)
    except (OverflowError, ValueError):
        return None, False, "futuro" if n > 0 else "antes_2024"
    instante = instante.replace(microsecond=(instante.microsecond // 1000) * 1000)
    if instante < INICIO_VALIDO:
        return instante, False, "antes_2024"
    if instante > recebido_em + TOLERANCIA_FUTURO:
        return instante, False, "futuro"
    return instante, True, None


def _achatar_payload(payload: dict) -> dict:
    achatado = dict(payload)
    for chave in SUBOBJETOS_PAYLOAD:
        sub = payload.get(chave)
        if isinstance(sub, dict):
            for k, v in sub.items():
                achatado.setdefault(k, v)
    return achatado


def extrair_metricas(payload: Any) -> tuple[list[tuple[str, float]], Optional[tuple[str, str]]]:
    """Extrai metricas conhecidas do payload de telemetria.

    Retorna (medidas, erro). erro = (motivo, detalhe) ou None. Se QUALQUER campo
    conhecido vier invalido, o pacote inteiro e rejeitado: nao gravamos metade
    de um pacote.
    """
    if not isinstance(payload, dict):
        return [], ("payload_invalido", "payload ausente ou nao e objeto")
    plano = _achatar_payload(payload)
    medidas: list[tuple[str, float]] = []
    for metrica, (aliases, minimo, maximo) in METRICAS.items():
        for alias in aliases:
            if alias not in plano:
                continue
            bruto = plano[alias]
            valor = _numero(bruto)
            if valor is None:
                return [], ("valor_invalido", f"{alias}={bruto!r} nao e numero finito")
            if (minimo is not None and valor < minimo) or (maximo is not None and valor > maximo):
                return [], ("valor_fora_da_faixa", f"{alias}={valor} fora de [{minimo}, {maximo}]")
            medidas.append((metrica, valor))
            break  # primeiro alias presente vence
    if not medidas:
        return [], ("telemetria_sem_metrica_conhecida", "payload sem nenhum campo de telemetria reconhecido")
    return medidas, None


def _hops(msg: dict) -> Optional[int]:
    h = _inteiro(msg.get("hops_away"))
    if h is None:
        inicio, limite = _inteiro(msg.get("hop_start")), _inteiro(msg.get("hop_limit"))
        if inicio is not None and limite is not None:
            h = inicio - limite
    return h if h is not None and 0 <= h <= 7 else None


def _hop_start(msg: dict) -> Optional[int]:
    h = _inteiro(msg.get("hop_start"))
    return h if h is not None and 0 <= h <= 7 else None


def _info_no(payload: Any) -> Optional[dict]:
    """Extrai nome curto/longo, hardware e papel de um pacote nodeinfo."""
    if not isinstance(payload, dict):
        return None

    def texto(*chaves):
        for k in chaves:
            v = payload.get(k)
            if isinstance(v, str) and v:
                return v[:64]
        return None

    info = {
        "nome_curto": texto("shortname", "shortName"),
        "nome_longo": texto("longname", "longName"),
        "hardware": _inteiro(payload.get("hardware", payload.get("hwModel"))),
        "papel": _inteiro(payload.get("role")),
    }
    return info if any(v is not None for v in info.values()) else None


# ------------------------------------------------------------ funcao principal


def processar_documento(doc: dict) -> Resultado:
    """Valida e normaliza um documento de `pacotes_brutos`. Nunca levanta."""
    bruto_id = str(doc.get("_id")) if isinstance(doc, dict) else "?"
    if not isinstance(doc, dict):
        return Resultado(bruto_id, None, "quarentena", "documento_invalido", "documento nao e objeto")

    recebido_em = para_utc_ingenuo(doc.get("recebido_em"))
    if recebido_em is None:
        return Resultado(bruto_id, None, "quarentena", "recebido_em_invalido", repr(doc.get("recebido_em")))

    def quarentena(motivo: str, detalhe: str = "", **extra: Any) -> Resultado:
        return Resultado(bruto_id, recebido_em, "quarentena", motivo, detalhe or None, **extra)

    if "mensagem" not in doc:
        if "mensagem_raw" in doc:
            return quarentena("json_invalido", "mensagem_raw: o consumidor nao conseguiu decodificar o JSON")
        return quarentena("mensagem_ausente", "documento sem `mensagem` nem `mensagem_raw`")
    msg = doc["mensagem"]
    if isinstance(msg, str):  # tolera JSON entregue como texto
        try:
            msg = json.loads(msg)
        except ValueError:
            return quarentena("json_invalido", "mensagem e texto que nao e JSON")
    if not isinstance(msg, dict):
        return quarentena("mensagem_nao_objeto", f"tipo {type(msg).__name__}")

    no = normalizar_no(msg.get("from"))
    if no is None:
        return quarentena("campo_faltando_from", f"from={msg.get('from')!r}")
    pacote_id = normalizar_id_pacote(msg.get("id"))
    if pacote_id is None:
        return quarentena("campo_faltando_id", f"id={msg.get('id')!r}", no=no)
    tipo_bruto = msg.get("type")
    if not isinstance(tipo_bruto, str) or not tipo_bruto.strip():
        return quarentena("campo_faltando_type", f"type={tipo_bruto!r}", no=no, pacote_id=pacote_id)
    tipo = tipo_bruto.strip().lower()
    if tipo != TIPO_TELEMETRIA and tipo not in TIPOS_SEM_METRICAS:
        return quarentena("tipo_desconhecido", f"type={tipo_bruto!r}", no=no, pacote_id=pacote_id)

    medidas: list[tuple[str, float]] = []
    if tipo == TIPO_TELEMETRIA:
        medidas, erro = extrair_metricas(msg.get("payload"))
        if erro:
            return quarentena(erro[0], erro[1], no=no, pacote_id=pacote_id, tipo=tipo)

    instante_no, valido, motivo_ts = avaliar_timestamp_no(msg.get("timestamp"), recebido_em)
    rssi = _numero(msg.get("rssi"))
    snr = _numero(msg.get("snr"))
    sender = msg.get("sender")
    return Resultado(
        bruto_id=bruto_id,
        recebido_em=recebido_em,
        status="ok",
        no=no,
        pacote_id=pacote_id,
        gateway=sender if isinstance(sender, str) and sender else None,
        tipo=tipo,
        instante_no=instante_no,
        instante_no_valido=valido,
        motivo_timestamp=motivo_ts,
        hops_away=_hops(msg),
        hop_start=_hop_start(msg),
        info_no=_info_no(msg.get("payload")) if tipo == "nodeinfo" else None,
        rssi=int(rssi) if rssi is not None else None,
        snr=snr,
        medidas=medidas,
    )
