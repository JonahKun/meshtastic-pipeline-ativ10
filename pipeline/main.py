"""Loop continuo do pipeline: pacotes_brutos (Mongo) -> MariaDB + Redis.

Uso:
    python -m pipeline.main               # loop continuo (polling)
    python -m pipeline.main --uma-vez     # processa o que existe e sai
"""
from __future__ import annotations

import argparse
import logging
import signal
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

from . import config as cfgmod
from .destinos import Destinos, criar_engine
from .fonte import Fonte, conectar
from .transformar import para_utc_ingenuo, processar_documento

log = logging.getLogger("pipeline")
EPOCA = datetime(1970, 1, 1)


class Pipeline:
    def __init__(self, cfg: cfgmod.Config, fonte: Fonte, destinos: Destinos, parar: Optional[threading.Event] = None):
        self.cfg = cfg
        self.fonte = fonte
        self.destinos = destinos
        self.parar = parar or threading.Event()
        self._redis_pendente = False

    def iniciar(self) -> None:
        self.destinos.criar_esquema()
        self._sincronizar_redis()

    def _sincronizar_redis(self) -> None:
        """Reaplica o ultimo valor do MariaDB no Redis (monotono, idempotente)."""
        try:
            n = self.destinos.reconstruir_redis()
            self._redis_pendente = False
            log.info("Redis sincronizado a partir do MariaDB (%d chaves atualizadas)", n)
        except Exception as exc:  # Redis fora do ar nao pode parar a ingestao
            self._redis_pendente = True
            log.warning("Redis indisponivel, sera ressincronizado depois: %s", exc)

    def ciclo(self) -> int:
        """Le tudo que esta pendente, em lotes. Retorna quantos documentos leu."""
        if self._redis_pendente:
            self._sincronizar_redis()
        cp = self.destinos.ler_checkpoint()
        desde = cp[0] - timedelta(seconds=self.cfg.sobreposicao_segundos) if cp else EPOCA
        apos_id = apos_ts = None
        total = 0
        while not self.parar.is_set():
            docs = self.fonte.ler_lote(desde, apos_id, self.cfg.lote, apos_ts)
            if not docs:
                break
            # o documento ORIGINAL vai para a quarentena; o normalizado vai para a validacao
            itens = [(d, processar_documento(self.fonte.normalizar(d))) for d in docs]
            ultimo = docs[-1]
            tempo_ultimo = self.fonte.tempo(ultimo)
            marca = (para_utc_ingenuo(tempo_ultimo), str(ultimo["_id"]))
            resumo = self.destinos.gravar_lote(itens, marca)  # COMMIT unico do lote
            try:
                self.destinos.atualizar_redis(resumo.ultimos)  # derivado, apos o commit
                self.destinos.atualizar_info_redis(resumo.infos)
            except Exception as exc:
                self._redis_pendente = True
                log.warning("Falha ao atualizar Redis (ressincroniza no proximo ciclo): %s", exc)
            apos_id, apos_ts = ultimo["_id"], tempo_ultimo
            total += len(docs)
            novos = {k: v for k, v in resumo.contagens.items() if v and ":" not in k}
            log.info("lote de %d docs ate %s | %s", len(docs), marca[0], dict(novos))
            if len(docs) < self.cfg.lote:
                break
        return total

    def executar(self) -> None:
        espera_erro = 2.0
        while not self.parar.is_set():
            try:
                self.ciclo()
                espera_erro = 2.0
                self.parar.wait(self.cfg.poll_segundos)
            except Exception:
                # Mongo/MariaDB indisponivel etc.: o lote em andamento foi revertido
                # (ou nem comecou); nada se perde porque o checkpoint nao avancou.
                log.exception("Erro no ciclo; nova tentativa em %.0fs", espera_erro)
                self.parar.wait(espera_erro)
                espera_erro = min(espera_erro * 2, 60.0)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Pipeline Meshtastic: Mongo -> MariaDB + Redis")
    ap.add_argument("--uma-vez", action="store_true", help="processa o pendente e encerra")
    ap.add_argument("--log", default="INFO")
    args = ap.parse_args(argv)
    logging.basicConfig(level=args.log.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = cfgmod.carregar()
    erros = cfgmod.validar(cfg)
    if erros:
        for e in erros:
            log.error(e)
        return 2

    import redis as redislib

    cliente_mongo, colecao = conectar(cfg.mongo_uri, cfg.mongo_db, cfg.mongo_collection)
    engine = criar_engine(cfg.mariadb_url)
    r = redislib.Redis.from_url(cfg.redis_url, decode_responses=True, socket_timeout=10, health_check_interval=30)
    destinos = Destinos(engine, r, cfg.prefixo, cfg.limite_lacuna_s)
    pipe = Pipeline(cfg, Fonte(colecao, cfg.fonte_formato), destinos)

    def encerrar(signum, _frame):
        log.info("Sinal %s recebido: encerrando apos o lote atual", signum)
        pipe.parar.set()

    signal.signal(signal.SIGINT, encerrar)
    signal.signal(signal.SIGTERM, encerrar)

    # Inicializacao com repeticao: servicos podem subir depois do container.
    while not pipe.parar.is_set():
        try:
            pipe.iniciar()
            break
        except Exception:
            log.exception("Falha ao inicializar; nova tentativa em 10s")
            pipe.parar.wait(10)

    log.info("Pipeline iniciado (poll=%ss lote=%s sobreposicao=%ss)", cfg.poll_segundos, cfg.lote, cfg.sobreposicao_segundos)
    t0 = time.time()
    if args.uma_vez:
        pipe.ciclo()
    else:
        pipe.executar()
    cliente_mongo.close()
    engine.dispose()
    log.info("Encerrado limpo apos %.0fs", time.time() - t0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
