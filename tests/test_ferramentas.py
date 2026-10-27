import importlib.util
from datetime import datetime
from pathlib import Path

import pytest

from ferramentas.semear_mongo_local import gerar_documentos, main as semear_main

from .helpers import Ambiente

RAIZ = Path(__file__).resolve().parent.parent


def _carregar(caminho, nome):
    spec = importlib.util.spec_from_file_location(nome, caminho)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_semeador_recusa_host_remoto():
    assert semear_main(["--uri", "mongodb://servidor.exemplo.com:27017"]) == 2


def test_dados_sinteticos_passam_pelo_pipeline_e_consultas(tmp_path):
    amb = Ambiente(tmp_path / "s.db", lote=200)
    docs = gerar_documentos(datetime(2026, 10, 20, 18, 0), horas=6, nos=3)
    assert all(d["_sintetico"] for d in docs)
    amb.inserir(*docs)
    amb.pipeline.ciclo()
    c = amb.contadores()
    assert c["duplicatas"] > 0 and c["timestamp_invalido"] > 0 and c["quarentena"] > 0
    assert c["processados"] == c["validos"] + c["duplicatas"] + c["quarentena"] == len(docs)

    runner = _carregar(RAIZ / "consultas" / "executar_consultas.py", "runner")
    arquivos = runner.listar_consultas()
    assert len(arquivos) == 3
    # consultas 1 e 2 sao SQL portavel; a 3 usa TIMESTAMPDIFF/UTC_TIMESTAMP (MariaDB) e nao roda no SQLite.
    cols, linhas = runner.executar(amb.engine, arquivos[0])
    assert cols[0] == "no" and linhas and all(3.0 < l[2] < 4.5 for l in linhas)
    cols, linhas = runner.executar(amb.engine, arquivos[1])
    assert "utilizacao_media_pct" in cols and linhas
    texto = runner.formatar_tabela(cols, linhas)
    assert texto.startswith("+-") and "utilizacao_media_pct" in texto

    from relatorio_contadores import montar_relatorio
    rel = montar_relatorio(amb.destinos)
    assert "[OK]" in rel and "Quarentena por motivo" in rel
