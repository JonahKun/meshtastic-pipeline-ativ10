import json
import re
from datetime import timedelta
from pathlib import Path

import pytest

from pipeline.destinos import Destinos, Esquema

from .helpers import T0, bruto_invalido, minutos, telemetria, ts


def rodar(amb):
    return amb.pipeline.ciclo()


# ---- deduplicacao ----------------------------------------------------------


def test_dedup_entre_gateways_por_from_e_id(amb):
    # Mesmo pacote (from, id) recebido por 3 gateways: conteudo de radio (rssi/snr/sender) diferente.
    a = telemetria(pid=10, sender="!aaaaaaaa")
    b = telemetria(pid=10, sender="!bbbbbbbb", recebido=T0 + timedelta(seconds=2), extra={"rssi": -70, "snr": 9})
    c = telemetria(pid=10, sender="!cccccccc", recebido=T0 + timedelta(seconds=5), extra={"hops_away": 3})
    amb.inserir(a, b, c)
    rodar(amb)
    cont = amb.contadores()
    assert cont["duplicatas"] == 2 and cont["validos"] == 1 and cont["processados"] == 3
    assert amb.consulta("select count(*) from jonah_pacotes_vistos") == [(1,)]
    assert amb.consulta("select count(*) from jonah_serie") == [(5,)]  # 5 metricas, 1 so pacote
    # fica a PRIMEIRA copia recebida
    assert amb.consulta("select bruto_id from jonah_pacotes_vistos") == [(str(a["_id"]),)]


def test_nunca_deduplica_por_conteudo(amb):
    # Mesmos valores, ids diferentes: sao leituras distintas.
    amb.inserir(*[telemetria(pid=i, recebido=T0 + minutos(i), tensao=3.9) for i in range(1, 4)])
    rodar(amb)
    assert amb.contadores().get("duplicatas", 0) == 0
    assert amb.consulta("select count(*) from jonah_pacotes_vistos") == [(3,)]
    # mesmo id, nos diferentes: tambem sao pacotes distintos
    amb.inserir(telemetria(no=0x11, pid=1, recebido=T0 + minutos(4)), telemetria(no=0x22, pid=1, recebido=T0 + minutos(4)))
    rodar(amb)
    assert amb.consulta("select count(*) from jonah_pacotes_vistos") == [(5,)]


# ---- relogio do no ----------------------------------------------------------


def test_timestamp_invalido_conta_e_usa_recebido_em(amb):
    amb.inserir(
        telemetria(pid=1, timestamp=0),
        telemetria(pid=2, recebido=T0 + minutos(1), timestamp=ts(T0 + minutos(1)) + 3600),  # futuro
        telemetria(pid=3, recebido=T0 + minutos(2), timestamp=ts(T0) - 10**8),  # ~3 anos antes: 2023
        telemetria(pid=4, recebido=T0 + minutos(3)),  # valido
    )
    rodar(amb)
    cont = amb.contadores()
    assert cont["timestamp_invalido"] == 3
    assert cont["timestamp_invalido:ausente"] == 1 and cont["timestamp_invalido:futuro"] == 1
    assert cont["timestamp_invalido:antes_2024"] == 1
    linhas = amb.consulta("select pacote_id, instante, instante_no_valido from jonah_serie where metrica='voltage' order by 1")
    assert [bool(l[2]) for l in linhas] == [False, False, False, True]
    # o tempo de referencia e sempre recebido_em, mesmo com relogio errado
    assert [str(l[1])[:19] for l in linhas] == [str(T0 + minutos(m)) for m in (0, 1, 2, 3)]


def test_timestamp_antes_de_2024(amb):
    amb.inserir(telemetria(pid=1, timestamp=1_000_000))
    rodar(amb)
    assert amb.contadores()["timestamp_invalido:antes_2024"] == 1
    assert amb.contadores()["timestamp_invalido"] == 1
    assert amb.consulta("select instante_no_valido from jonah_serie limit 1") == [(0,)]


# ---- quarentena -------------------------------------------------------------


def test_quarentena_com_motivo_e_documento(amb):
    sem_from = telemetria(pid=2)
    del sem_from["mensagem"]["from"]
    amb.inserir(
        bruto_invalido(),
        telemetria(pid=1, tipo="foguete"),
        sem_from,
        telemetria(pid=3, payload={"voltage": "alto"}),
        telemetria(pid=4),  # valido
    )
    rodar(amb)
    cont = amb.contadores()
    assert cont["quarentena"] == 4 and cont["validos"] == 1
    assert cont["quarentena:json_invalido"] == 1 and cont["quarentena:tipo_desconhecido"] == 1
    assert cont["quarentena:campo_faltando_from"] == 1 and cont["quarentena:valor_invalido"] == 1
    linhas = amb.consulta("select motivo, documento from jonah_quarentena where motivo='json_invalido'")
    assert json.loads(linhas[0][1])["mensagem_raw"] == "{isto nao e json"
    # nada da quarentena entra na serie
    assert amb.consulta("select count(distinct pacote_id) from jonah_serie") == [(1,)]


def test_tipo_sem_metrica_e_contado_mas_nao_vai_para_serie(amb):
    amb.inserir(telemetria(pid=1, tipo="position", payload={"latitude_i": 1}))
    rodar(amb)
    cont = amb.contadores()
    assert cont["sem_metrica"] == 1 and cont["validos"] == 1
    assert amb.consulta("select count(*) from jonah_serie") == [(0,)]


# ---- reinicio ---------------------------------------------------------------


def _carga(n=14):
    docs = []
    for i in range(n):
        docs.append(telemetria(pid=i, no=0x10 + (i % 3), recebido=T0 + minutos(i * 7), tensao=3.5 + i / 20))
    docs.append(telemetria(pid=3, no=0x10 + (3 % 3), recebido=T0 + minutos(3 * 7) + timedelta(seconds=1)))  # duplicata
    docs.append(bruto_invalido(T0 + minutos(40)))
    docs.append(telemetria(pid=99, recebido=T0 + minutos(41), timestamp=0))
    return docs


def test_reinicio_apos_lotes_nao_duplica_nem_perde(fabrica):
    docs = _carga()
    ref = fabrica(lote=4)
    ref.inserir(*docs)
    rodar(ref)

    amb = fabrica(lote=4)
    amb.inserir(*docs)

    # processo 1 le so o primeiro lote e "morre" (a fonte falha no segundo)
    original = amb.pipeline.fonte.ler_lote
    chamadas = {"n": 0}

    def falha_no_segundo(*a, **k):
        chamadas["n"] += 1
        if chamadas["n"] == 2:
            raise ConnectionError("processo morto")
        return original(*a, **k)

    amb.pipeline.fonte.ler_lote = falha_no_segundo
    with pytest.raises(ConnectionError):
        rodar(amb)
    parcial = amb.consulta("select count(*) from jonah_pacotes_vistos")[0][0]
    assert 0 < parcial < ref.consulta("select count(*) from jonah_pacotes_vistos")[0][0]

    # processo 2 (novo engine, novo Pipeline) retoma do checkpoint com sobreposicao
    amb.novo_processo()
    rodar(amb)
    assert amb.estado() == ref.estado()
    # e um terceiro reinicio sem dados novos nao muda nada
    antes = amb.estado()
    amb.novo_processo()
    rodar(amb)
    assert amb.estado() == antes


def test_falha_no_meio_do_lote_reverte_tudo_e_depois_completa(fabrica, monkeypatch):
    docs = _carga()
    ref = fabrica(lote=100)
    ref.inserir(*docs)
    rodar(ref)

    amb = fabrica(lote=100)
    amb.inserir(*docs)
    real = Destinos._recalcular_lacunas

    def explode(self, *a, **k):
        raise RuntimeError("queda no meio da transacao")

    monkeypatch.setattr(Destinos, "_recalcular_lacunas", explode)
    with pytest.raises(RuntimeError):
        rodar(amb)
    # rollback total: nem vistos, nem serie, nem contadores, nem checkpoint
    assert amb.consulta("select count(*) from jonah_pacotes_vistos") == [(0,)]
    assert amb.consulta("select count(*) from jonah_serie") == [(0,)]
    assert amb.consulta("select count(*) from jonah_contadores") == [(0,)]
    assert amb.consulta("select count(*) from jonah_checkpoint") == [(0,)]
    assert amb.consulta("select count(*) from jonah_quarentena") == [(0,)]

    monkeypatch.setattr(Destinos, "_recalcular_lacunas", real)
    amb.novo_processo()
    rodar(amb)
    assert amb.estado() == ref.estado()


def test_sobreposicao_nao_infla_contadores_e_pega_atrasados(amb):
    amb.inserir(*[telemetria(pid=i, recebido=T0 + minutos(i)) for i in range(5)])
    rodar(amb)
    antes = amb.contadores()
    for _ in range(3):
        rodar(amb)  # relê a janela de sobreposicao a cada ciclo
    assert amb.contadores() == antes

    # documento que chegou ao Mongo "atrasado" (recebido_em dentro da janela de sobreposicao)
    amb.inserir(telemetria(pid=100, recebido=T0 + minutos(4) - timedelta(seconds=30)))
    rodar(amb)
    assert amb.consulta("select count(*) from jonah_pacotes_vistos") == [(6,)]
    assert amb.contadores()["validos"] == 6


def test_checkpoint_avanca_e_nunca_regride(amb):
    amb.inserir(*[telemetria(pid=i, recebido=T0 + minutos(i)) for i in range(3)])
    rodar(amb)
    cp1 = amb.destinos.ler_checkpoint()
    assert cp1[0] == T0 + minutos(2)
    rodar(amb)
    assert amb.destinos.ler_checkpoint() == cp1


def test_fonte_e_somente_leitura(amb):
    amb.inserir(*[telemetria(pid=i, recebido=T0 + minutos(i)) for i in range(3)])
    antes = list(amb.colecao.find())
    rodar(amb)
    assert list(amb.colecao.find()) == antes


def test_desempate_por_id_quando_recebido_em_igual(fabrica):
    amb = fabrica(lote=2, sobreposicao_segundos=0)
    amb.inserir(*[telemetria(pid=i, recebido=T0) for i in range(7)])  # todos com o mesmo recebido_em
    rodar(amb)
    assert amb.consulta("select count(*) from jonah_pacotes_vistos") == [(7,)]


# ---- Redis ------------------------------------------------------------------


def test_redis_ultimo_valor_e_nao_regride(amb):
    amb.inserir(telemetria(pid=1, recebido=T0 + minutos(10), tensao=4.0))
    rodar(amb)
    chave = "jonah:ultimo:!aabbccdd:voltage"
    h = amb.redis.hgetall(chave)
    assert float(h["valor"]) == 4.0 and h["instante_iso"].startswith("2026-10-20T12:10")
    assert "!aabbccdd" in amb.redis.smembers("jonah:nos")

    # leitura mais ANTIGA chegando depois (dentro da sobreposicao): Redis nao regride
    amb.inserir(telemetria(pid=2, recebido=T0 + minutos(9), tensao=3.1))
    rodar(amb)
    assert float(amb.redis.hget(chave, "valor")) == 4.0
    # e um mais novo atualiza
    amb.inserir(telemetria(pid=3, recebido=T0 + minutos(11), tensao=4.2))
    rodar(amb)
    assert float(amb.redis.hget(chave, "valor")) == 4.2


def test_redis_reconstruido_apos_perda(amb):
    amb.inserir(*[telemetria(pid=i, recebido=T0 + minutos(i), tensao=3.0 + i / 10) for i in range(4)])
    rodar(amb)
    amb.redis.flushall()  # simula Redis reiniciado sem persistencia (ou queda apos o COMMIT)
    amb.novo_processo()  # iniciar() ressincroniza a partir do MariaDB
    assert float(amb.redis.hget("jonah:ultimo:!aabbccdd:voltage", "valor")) == pytest.approx(3.3)
    assert amb.redis.smembers("jonah:nos") == {"!aabbccdd"}


def test_redis_fora_do_ar_nao_para_a_ingestao(amb):
    amb.inserir(telemetria(pid=1))

    def quebra(*a, **k):
        raise ConnectionError("redis fora")

    amb.destinos.atualizar_redis = quebra
    amb.destinos.reconstruir_redis = quebra
    rodar(amb)
    assert amb.contadores()["validos"] == 1 and amb.pipeline._redis_pendente


# ---- agregados ----------------------------------------------------------------


def test_agregados_hora_e_dia_corretos(amb):
    # 3 leituras na hora 12h, 1 na hora 13h, 1 no dia seguinte; no !aabbccdd
    valores = [(0, 3.0), (10, 4.0), (20, 5.0), (70, 6.0), (24 * 60, 7.0)]
    amb.inserir(*[telemetria(pid=i, recebido=T0 + minutos(m), tensao=v) for i, (m, v) in enumerate(valores)])
    rodar(amb)
    h = amb.consulta("select janela, minimo, maximo, media, n from jonah_agg_hora where metrica='voltage' order by janela")
    assert [(r[1], r[2], r[3], r[4]) for r in h] == [(3.0, 5.0, 4.0, 3), (6.0, 6.0, 6.0, 1), (7.0, 7.0, 7.0, 1)]
    d = amb.consulta("select janela, minimo, maximo, media, n from jonah_agg_dia where metrica='voltage' order by janela")
    assert [(r[1], r[2], r[3], r[4]) for r in d] == [(3.0, 6.0, 4.5, 4), (7.0, 7.0, 7.0, 1)]


def test_agregado_recalculado_com_dado_tardio_e_idempotente(amb):
    amb.inserir(telemetria(pid=1, recebido=T0, tensao=3.0), telemetria(pid=2, recebido=T0 + minutos(30), tensao=5.0))
    rodar(amb)
    amb.inserir(telemetria(pid=3, recebido=T0 + minutos(29), tensao=10.0))  # tardio, mesma hora
    rodar(amb)
    linha = amb.consulta("select minimo, maximo, media, n from jonah_agg_hora where metrica='voltage'")
    assert linha == [(3.0, 10.0, 6.0, 3)]
    rodar(amb)
    rodar(amb)
    assert amb.consulta("select minimo, maximo, media, n from jonah_agg_hora where metrica='voltage'") == linha


# ---- lacunas ------------------------------------------------------------------


def test_lacunas_registradas_e_corrigidas_por_pacote_tardio(amb):
    # esperado 15 min => limite 30 min. Pacotes em 0, 10, 100 min: lacuna 10 -> 100 (90 min)
    amb.inserir(*[telemetria(pid=i, recebido=T0 + minutos(m)) for i, m in enumerate([0, 10, 100])])
    rodar(amb)
    assert amb.consulta("select duracao_s from jonah_lacunas") == [(90 * 60,)]
    # chega pacote aos 60 min: a lacuna de 90 min se divide em 50 + 40, ambas > 30 min
    amb.inserir(telemetria(pid=50, recebido=T0 + minutos(60)))
    amb.pipeline.cfg.sobreposicao_segundos = 3 * 3600
    rodar(amb)
    assert sorted(amb.consulta("select duracao_s from jonah_lacunas")) == [(40 * 60,), (50 * 60,)]
    # sem interpolacao: nenhuma linha de serie nasce nos buracos
    assert amb.consulta("select count(*) from jonah_serie where metrica='voltage'") == [(4,)]


# ---- esquema ------------------------------------------------------------------


def test_ddl_mariadb_bate_com_o_modelo_sqlalchemy():
    sql = (Path(__file__).resolve().parent.parent / "schema" / "mariadb.sql").read_text(encoding="utf-8")
    esquema = Esquema("jonah")
    ddl = dict(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\) ENGINE=InnoDB", sql, flags=re.S))
    assert set(ddl) == {t.name for t in esquema.metadata.sorted_tables}
    for tabela in esquema.metadata.sorted_tables:
        for coluna in tabela.columns:
            assert re.search(rf"^\s*{coluna.name}\s", ddl[tabela.name], flags=re.M), (tabela.name, coluna.name)
    assert all(n.startswith("jonah_") for n in ddl)
    assert ";" not in re.sub(r"\) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", "", re.sub(r"--[^\n]*", "", sql))
