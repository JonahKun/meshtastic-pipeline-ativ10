"""Esquema legado do servidor da disciplina (iot.sensores), com documentos no formato real observado."""
import json
from datetime import datetime, timedelta

import mongomock
import pytest
from bson import ObjectId

from pipeline.adaptador import detectar_formato, normalizar
from pipeline.fonte import Fonte
from pipeline.transformar import processar_documento

from .helpers import Ambiente

R0 = datetime(2026, 10, 25, 14, 0, 0)
TOPICO = "sensores/tempUmid/2/json/mqtt_teste/!8fa724e8"

TEXTO = {"channel": 0, "from": 2410095848, "hop_start": 3, "hops_away": 0, "id": 866148401,
         "payload": {"text": ""}, "sender": "!8fa724e8", "timestamp": 0, "to": 4294967295, "type": "text"}
NODEINFO = {"channel": 0, "from": 2410095848, "hop_start": 3, "hops_away": 0, "id": 12345,
            "payload": {"hardware": 110, "id": "!8fa724e8", "longname": "Meshtastic 24e8", "role": 0,
                        "shortname": "24e8"},
            "sender": "!8fa724e8", "timestamp": 0, "to": 4294967295, "type": "nodeinfo"}


def novo(obj, t=R0, topic=TOPICO):
    """Doc 'novo': payload (string), payload_data (objeto), received_at, data = datetime."""
    return {"_id": ObjectId(), "topic": topic, "payload": json.dumps(obj), "payload_data": obj,
            "received_at": t, "qos": 0, "retain": False, "data": t}


def antigo(obj, t=R0):
    """Doc 'antigo': `data` e o proprio objeto Meshtastic."""
    return {"_id": ObjectId(), "topic": TOPICO, "payload": json.dumps(obj), "data": obj,
            "received_at": t, "qos": 0, "retain": False}


def test_deteccao_de_formato():
    assert detectar_formato(novo(TEXTO)) == "legado"
    assert detectar_formato({"recebido_em": R0, "mensagem": {}}) == "pacotes_brutos"


def test_doc_novo_usa_payload_data():
    n = normalizar(novo(TEXTO))
    assert n["mensagem"] == TEXTO and n["recebido_em"] == R0 and n["topico"] == TOPICO
    r = processar_documento(n)
    assert r.status == "ok" and r.no == "!8fa724e8" and r.pacote_id == 866148401 and r.tipo == "text"
    assert r.hop_start == 3 and r.hops_away == 0 and r.rssi is None and r.snr is None
    assert not r.instante_no_valido and r.motivo_timestamp == "ausente"  # timestamp=0 real


def test_doc_antigo_usa_data_dict():
    n = normalizar(antigo(TEXTO))
    assert n["mensagem"] == TEXTO
    assert processar_documento(n).status == "ok"


def test_fallback_para_parse_da_string_payload():
    d = {"_id": 1, "topic": TOPICO, "payload": json.dumps(TEXTO), "received_at": R0}
    assert normalizar(d)["mensagem"] == TEXTO


def test_payload_string_invalida_vai_para_quarentena():
    d = {"_id": 1, "topic": TOPICO, "payload": "{quebrado", "received_at": R0}
    r = processar_documento(normalizar(d))
    assert (r.status, r.motivo) == ("quarentena", "json_invalido")
    r = processar_documento(normalizar({"_id": 2, "topic": TOPICO, "received_at": R0}))
    assert r.motivo == "mensagem_ausente"


def test_serial_sem_topico_e_sem_type():
    serial = {"_id": ObjectId(), "topic": None, "payload": "x", "payload_data": {"type": "serial", "from": 1, "id": 2, "payload": {}}, "received_at": R0}
    assert processar_documento(normalizar(serial)).motivo == "tipo_desconhecido"
    sem_type = novo({k: v for k, v in TEXTO.items() if k != "type"})
    assert processar_documento(normalizar(sem_type)).motivo == "campo_faltando_type"


def test_fonte_auto_detecta_campo_e_le_legado():
    col = mongomock.MongoClient()["iot"]["sensores"]
    f = Fonte(col, "auto")
    assert f.ler_lote(datetime(1970, 1, 1)) == []  # colecao vazia: sem erro
    col.insert_many([novo(TEXTO, R0), antigo(NODEINFO, R0 + timedelta(seconds=1))])
    docs = f.ler_lote(datetime(1970, 1, 1))
    assert f.campo_tempo == "received_at" and len(docs) == 2
    with pytest.raises(ValueError):
        Fonte(col, "xml")


def test_pipeline_ponta_a_ponta_com_dados_reais(tmp_path):
    col = mongomock.MongoClient()["iot"]["sensores"]
    amb = Ambiente(tmp_path / "l.db", colecao=col)
    # 3 copias do mesmo pacote text (3 gateways/rotas), um nodeinfo, lixo diversificado
    col.insert_many([
        novo(TEXTO, R0), novo(TEXTO, R0 + timedelta(seconds=2)), antigo(TEXTO, R0 + timedelta(seconds=3)),
        novo(NODEINFO, R0 + timedelta(minutes=1)),
        {"_id": ObjectId(), "topic": None, "payload": "s", "payload_data": {"type": "serial", "from": 1, "id": 2}, "received_at": R0 + timedelta(minutes=2)},
        novo({k: v for k, v in TEXTO.items() if k != "type"}, R0 + timedelta(minutes=3)),
        {"_id": ObjectId(), "topic": TOPICO, "payload": "{quebrado", "received_at": R0 + timedelta(minutes=4)},
    ])
    amb.pipeline.ciclo()
    c = amb.contadores()
    assert c["validos"] == 2 and c["duplicatas"] == 2 and c["quarentena"] == 3
    assert c["quarentena:tipo_desconhecido"] == 1 and c["quarentena:campo_faltando_type"] == 1
    assert c["quarentena:json_invalido"] == 1
    assert c["timestamp_invalido:ausente"] == 2 and c["sem_metrica"] == 2
    assert c["processados"] == 7
    # documento ORIGINAL (legado) preservado na quarentena
    doc = json.loads(amb.consulta("select documento from jonah_quarentena where motivo='json_invalido'")[0][0])
    assert doc["payload"] == "{quebrado"
    # checkpoint usa received_at
    assert amb.destinos.ler_checkpoint()[0] == R0 + timedelta(minutes=4)
    # cadastro do no pelo nodeinfo
    assert amb.consulta("select no, nome_curto, nome_longo, hardware, papel from jonah_nos") == [
        ("!8fa724e8", "24e8", "Meshtastic 24e8", 110, 0)]
    h = amb.redis.hgetall("jonah:no:!8fa724e8")
    assert h["nome_longo"] == "Meshtastic 24e8" and h["hardware"] == "110"
    assert "!8fa724e8" in amb.redis.smembers("jonah:nos")
    # releituras nao inflam
    antes = amb.contadores()
    amb.novo_processo()
    amb.pipeline.ciclo()
    assert amb.contadores() == antes


def test_hop_start_salvo_na_serie(tmp_path):
    from .helpers import telemetria
    amb = Ambiente(tmp_path / "h.db")
    amb.inserir(telemetria(pid=1, extra={"hop_start": 3, "hops_away": 1}))
    amb.pipeline.ciclo()
    assert amb.consulta("select distinct hops_away, hop_start from jonah_serie") == [(1, 3)]
