from datetime import datetime, timedelta

import pytest

from pipeline.transformar import (
    avaliar_timestamp_no,
    normalizar_no,
    processar_documento,
)

from .helpers import T0, bruto_invalido, telemetria, ts


def test_telemetria_de_dispositivo_vira_metricas():
    r = processar_documento(telemetria(pid=7))
    assert r.status == "ok"
    assert r.no == "!aabbccdd" and r.pacote_id == 7 and r.gateway == "!11111111"
    assert dict(r.medidas) == {
        "battery_level": 80.0, "voltage": 3.9, "channel_utilization": 5.0,
        "air_util_tx": 1.0, "uptime_seconds": 1000.0,
    }
    assert (r.hops_away, r.rssi, r.snr) == (1, -90, 6.5)
    assert r.instante_no_valido and r.recebido_em == T0


def test_variacoes_de_nomes_entre_versoes():
    camel = telemetria(payload={"batteryLevel": 50, "channelUtilization": 3.2, "airUtilTx": 0.4, "uptimeSeconds": 9})
    assert dict(processar_documento(camel).medidas) == {
        "battery_level": 50.0, "channel_utilization": 3.2, "air_util_tx": 0.4, "uptime_seconds": 9.0}
    aninhado = telemetria(payload={"device_metrics": {"voltage": 4.1}, "environment_metrics": {"temperature": 22.5, "relativeHumidity": 61}})
    assert dict(processar_documento(aninhado).medidas) == {"voltage": 4.1, "temperature": 22.5, "relative_humidity": 61.0}


def test_telemetria_ambiental():
    r = processar_documento(telemetria(payload={"temperature": 21.3, "relative_humidity": 55.0, "barometric_pressure": 1012.0}))
    assert r.status == "ok" and len(r.medidas) == 3


@pytest.mark.parametrize("bruto,esperado", [
    (0xAABBCCDD, "!aabbccdd"), ("!AABBCCDD", "!aabbccdd"), ("!abc", "!00000abc"),
    (-1, "!ffffffff"), ("2864434397", "!aabbccdd"), (True, None), (None, None), ("xyz", None),
])
def test_normalizar_no(bruto, esperado):
    assert normalizar_no(bruto) == esperado


def test_hops_a_partir_de_hop_start():
    d = telemetria()
    d["mensagem"].pop("hops_away")
    d["mensagem"].update(hop_start=3, hop_limit=1)
    assert processar_documento(d).hops_away == 2


# ---- relogio do no -------------------------------------------------------


@pytest.mark.parametrize("valor,motivo", [
    (None, "ausente"), (0, "ausente"), ("abc", "ausente"),
    (ts(datetime(2023, 12, 31)), "antes_2024"),
    (ts(T0 + timedelta(minutes=6)), "futuro"),
    (ts(T0 + timedelta(days=400)), "futuro"),
    (10**15, "futuro"),
])
def test_timestamp_invalido(valor, motivo):
    _, valido, m = avaliar_timestamp_no(valor, T0)
    assert not valido and m == motivo


def test_timestamp_valido_e_limites():
    assert avaliar_timestamp_no(ts(T0 - timedelta(hours=2)), T0)[1]
    assert avaliar_timestamp_no(ts(T0 + timedelta(minutes=5)), T0)[1]  # exatamente 5 min ainda vale
    inst, valido, _ = avaliar_timestamp_no(ts(T0) * 1000, T0)  # milissegundos
    assert valido and inst == T0


def test_tempo_de_referencia_e_recebido_em_mesmo_com_relogio_errado():
    r = processar_documento(telemetria(timestamp=0))
    assert r.status == "ok" and not r.instante_no_valido and r.motivo_timestamp == "ausente"
    assert r.recebido_em == T0


# ---- quarentena -----------------------------------------------------------


def test_json_invalido():
    r = processar_documento(bruto_invalido())
    assert (r.status, r.motivo) == ("quarentena", "json_invalido")


def test_documentos_sem_mensagem_e_nao_objeto():
    d = telemetria()
    del d["mensagem"]
    assert processar_documento(d).motivo == "mensagem_ausente"
    d["mensagem"] = [1, 2]
    assert processar_documento(d).motivo == "mensagem_nao_objeto"


def test_tipo_desconhecido_e_faltando():
    assert processar_documento(telemetria(tipo="foguete")).motivo == "tipo_desconhecido"
    d = telemetria()
    del d["mensagem"]["type"]
    assert processar_documento(d).motivo == "campo_faltando_type"


def test_campos_faltando():
    d = telemetria()
    del d["mensagem"]["from"]
    assert processar_documento(d).motivo == "campo_faltando_from"
    d = telemetria()
    del d["mensagem"]["id"]
    assert processar_documento(d).motivo == "campo_faltando_id"


def test_payload_e_valores_invalidos():
    d = telemetria()
    del d["mensagem"]["payload"]
    assert processar_documento(d).motivo == "payload_invalido"
    assert processar_documento(telemetria(payload={"voltage": "3.9"})).motivo == "valor_invalido"
    assert processar_documento(telemetria(payload={"voltage": float("nan")})).motivo == "valor_invalido"
    assert processar_documento(telemetria(payload={"voltage": 3.9, "battery_level": 250})).motivo == "valor_fora_da_faixa"
    assert processar_documento(telemetria(payload={"foo": 1})).motivo == "telemetria_sem_metrica_conhecida"


def test_tipos_sem_metricas_sao_validos():
    r = processar_documento(telemetria(tipo="position", payload={"latitude_i": -224000000}))
    assert r.status == "ok" and r.medidas == []


def test_nunca_levanta_excecao():
    assert processar_documento({"_id": 1}).status == "quarentena"
    assert processar_documento({"_id": 1, "recebido_em": "lixo"}).motivo == "recebido_em_invalido"
