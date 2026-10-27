"""Semeia um Mongo LOCAL com pacotes SINTETICOS, apenas para testar o pipeline.

!!! ESTES DADOS SAO FALSOS. NAO SAO EVIDENCIA. !!!
Nao use prints, contadores ou consultas gerados a partir deles na entrega:
a P1 diz que evidencia forjada zera a nota. A evidencia da ativ. 10 e o que o
pipeline processar do MongoDB real da disciplina, em 72h de operacao.

Salvaguardas:
  - so aceita Mongo em localhost/127.0.0.1;
  - grava num banco proprio (padrao: jonah_sintetico_teste), nunca no banco real;
  - todo documento leva o campo extra `_sintetico: true`.

Uso (com um mongod local rodando):
    python ferramentas/semear_mongo_local.py --horas 6 --nos 4
    MONGO_URI=mongodb://localhost:27017 MONGO_DB=jonah_sintetico_teste MONGO_COLLECTION=pacotes_brutos \
    MARIADB_URL=sqlite:///teste_local.db REDIS_URL=redis://localhost:6379/0 python -m pipeline.main --uma-vez
"""
from __future__ import annotations

import argparse
import random
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

BANCO_PADRAO = "jonah_sintetico_teste"
EPOCA = datetime(1970, 1, 1)


def _ts(dt: datetime) -> int:
    return int((dt - EPOCA).total_seconds())


def gerar_documentos(fim: datetime, horas: float = 6, nos: int = 4, semente: int = 42, gateways: int = 2) -> list[dict]:
    """Gera documentos no formato do contrato da ativ. 9. Determinístico para uma semente."""
    rnd = random.Random(semente)
    ids_no = [0xA0000000 + i for i in range(1, nos + 1)]
    gws = [f"!{0xB0000000 + g:08x}" for g in range(gateways)]
    inicio = fim - timedelta(hours=horas)
    docs: list[dict] = []
    contador_id = {n: rnd.randrange(1, 10**6) for n in ids_no}

    def envelope(recebido, msg, gw):
        return {"topico": f"msh/BR_902/2/json/LongFast/{gw}", "recebido_em": recebido, "mensagem": msg, "_sintetico": True}

    for idx, no in enumerate(ids_no):
        t = inicio + timedelta(seconds=rnd.randrange(0, 600))
        silencio = (inicio + timedelta(hours=horas * 0.4), inicio + timedelta(hours=horas * 0.4 + 2)) if idx == 1 else None
        bateria = 100.0
        while t < fim:
            if silencio and silencio[0] <= t < silencio[1]:
                t += timedelta(minutes=15)
                continue
            contador_id[no] += 1
            bateria = max(5.0, bateria - rnd.random() * 0.3)
            sorteio = rnd.random()
            relogio = _ts(t) - rnd.randrange(0, 5)
            if sorteio < 0.05:
                relogio = 0  # RTC sem hora
            elif sorteio < 0.08:
                relogio = 946684800 + rnd.randrange(0, 1000)  # ano 2000
            elif sorteio < 0.10:
                relogio = _ts(t) + 86400 * 30  # no futuro
            msg = {
                "from": no, "to": 4294967295, "id": contador_id[no], "type": "telemetry", "timestamp": relogio,
                "hops_away": rnd.choice([0, 0, 1, 2]), "rssi": rnd.randrange(-120, -60), "snr": round(rnd.uniform(-8, 10), 2),
                "channel": 0,
                "payload": {
                    "battery_level": int(bateria), "voltage": round(3.3 + bateria / 100 * 0.9, 3),
                    "channel_utilization": round(rnd.uniform(0, 25), 2), "air_util_tx": round(rnd.uniform(0, 3), 2),
                    "uptime_seconds": int((t - inicio).total_seconds()),
                },
            }
            ouvintes = rnd.sample(gws, k=rnd.choice([1, 1, min(2, len(gws))]))
            for g, gw in enumerate(ouvintes):
                m = dict(msg, sender=gw, rssi=msg["rssi"] - g * 3)
                docs.append(envelope(t + timedelta(seconds=rnd.randrange(0, 4) + g), m, gw))  # mesma (from, id) por varios gateways
            if rnd.random() < 0.15:  # pacote de posicao (tipo sem metrica)
                contador_id[no] += 1
                docs.append(envelope(t + timedelta(seconds=1), {
                    "from": no, "id": contador_id[no], "type": "position", "timestamp": _ts(t), "sender": gws[0],
                    "payload": {"latitude_i": -224000000, "longitude_i": -499000000, "altitude": 450},
                }, gws[0]))
            t += timedelta(minutes=15, seconds=rnd.randrange(-40, 40))

    # sujeira controlada para a quarentena
    for i in range(max(3, int(horas))):
        quando = inicio + timedelta(minutes=rnd.randrange(1, int(horas * 60)))
        docs.append({"topico": "msh/BR_902/2/json/LongFast/!b0000000", "recebido_em": quando, "mensagem_raw": "{json quebrado", "_sintetico": True})
        docs.append(envelope(quando, {"from": ids_no[0], "id": 9_000_000 + i, "type": "foguete", "payload": {}}, gws[0]))
        docs.append(envelope(quando, {"id": 9_100_000 + i, "type": "telemetry", "payload": {"voltage": 3.9}}, gws[0]))
        docs.append(envelope(quando, {"from": ids_no[0], "id": 9_200_000 + i, "type": "telemetry", "payload": {"voltage": "alto"}}, gws[0]))
    docs.sort(key=lambda d: d["recebido_em"])
    return docs


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--banco", default=BANCO_PADRAO)
    ap.add_argument("--colecao", default="pacotes_brutos")
    ap.add_argument("--horas", type=float, default=6)
    ap.add_argument("--nos", type=int, default=4)
    ap.add_argument("--semente", type=int, default=42)
    ap.add_argument("--limpar", action="store_true", help="apaga a colecao sintetica antes de semear")
    args = ap.parse_args(argv)

    host = urlparse(args.uri).hostname
    if host not in ("localhost", "127.0.0.1", "::1"):
        print(f"RECUSADO: {host!r} nao e local. Este script so semeia Mongo local, para teste.", file=sys.stderr)
        return 2

    from pymongo import MongoClient

    docs = gerar_documentos(datetime.now(timezone.utc).replace(tzinfo=None), args.horas, args.nos, args.semente)
    col = MongoClient(args.uri, serverSelectionTimeoutMS=5000)[args.banco][args.colecao]
    if args.limpar:
        col.delete_many({"_sintetico": True})
    col.insert_many(docs)
    print("=" * 70)
    print("DADOS SINTETICOS (FALSOS) gravados SO para teste local. NAO SAO EVIDENCIA.")
    print(f"{len(docs)} documentos em {args.uri} -> {args.banco}.{args.colecao}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
