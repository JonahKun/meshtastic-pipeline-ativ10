"""Simulacao ACELERADA de 72h de operacao da malha, para testar o pipeline.

!!! OS PACOTES DESTA SIMULACAO SAO GERADOS POR PROGRAMA. NAO SAO OPERACAO REAL. !!!
Use o resultado na entrega SOMENTE identificado como simulacao (secao propria,
titulo "Simulacao", tabelas jonahsim_*). A evidencia de operacao real continua
sendo o que o pipeline processar de iot.pacotes_brutos nas tabelas jonah_*.

O que e real aqui: o codigo do pipeline (transformar, deduplicar, agregar,
lacunas, quarentena, Redis) e o MariaDB/Redis da disciplina. O que e simulado:
a malha. Os pacotes ficam num Mongo EM MEMORIA (mongomock); nada e gravado no
Mongo da disciplina.

Salvaguardas:
  - prefixo obrigatoriamente diferente de "jonah" (padrao: jonahsim);
  - ids de no e de gateway na faixa !51xxxxxx, que nao existe na malha real;
  - todo documento leva `_sintetico: true` (vai junto para a quarentena).

Cenario (72h terminando agora):
  - 6 nos, 2 gateways; nos a 0, 1 e 2 saltos;
  - telemetria de dispositivo a cada ~30 min (o minimo do firmware no canal publico);
  - bateria: no na tomada (101), nos solares (carregam de dia, descarregam a noite)
    e um no so com bateria que vai se esgotando;
  - uso do canal com ciclo diario (pico a noite, vale de madrugada) e rajadas;
  - 2 nos com sensor de temperatura/umidade (topico tempUmid);
  - eventos: no solar desliga de madrugada na 2a noite por bateria baixa, gateway B
    fica 3h fora (nos distantes perdem pacotes), um no novo entra na hora 20,
    perda aleatoria de ~5% dos pacotes;
  - sujeira para a quarentena (JSON quebrado, tipo desconhecido, sem `from`,
    metrica nao numerica) e relogios de no invalidos.

Uso:
    python ferramentas/simular_72h.py --limpar          # recria jonahsim_* e roda
    python ferramentas/simular_72h.py --limpar --semente 7 --horas 72
"""
from __future__ import annotations

import argparse
import math
import random
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

RAIZ = Path(__file__).resolve().parent.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

EPOCA = datetime(1970, 1, 1)
FUSO_LOCAL = timedelta(hours=-3)  # Brasilia, para o ciclo diario
BROADCAST = 4294967295
GATEWAYS = {"A": 0x51A00001, "B": 0x51A00002}
PASTA_RELATORIO = RAIZ / "docs" / "simulacao_72h"


def _ts(dt: datetime) -> int:
    return int((dt - EPOCA).total_seconds())


def _hex(n: int) -> str:
    return f"!{n:08x}"


def _hora_local(t: datetime) -> float:
    lt = t + FUSO_LOCAL
    return lt.hour + lt.minute / 60


@dataclass
class No:
    num: int
    curto: str
    longo: str
    energia: str  # tomada | solar | bateria
    saltos: int  # distancia tipica ate os gateways
    gateways: tuple[str, ...]  # quem costuma ouvir este no
    sensor: bool = False
    entra_na_hora: float = 0.0
    bateria: float = 100.0
    contador: int = field(default=0)
    ligado: bool = True
    uptime_desde: Optional[datetime] = None


def criar_nos(rnd: random.Random) -> list[No]:
    nos = [
        No(0x51B00001, "GWA", "Sim Gateway A (tomada)", "tomada", 0, ("A",)),
        No(0x51B00002, "TETO", "Sim Telhado (solar)", "solar", 0, ("A", "B"), sensor=True),
        No(0x51B00003, "LAB", "Sim Laboratorio", "tomada", 1, ("A", "B"), sensor=True),
        No(0x51B00004, "POST", "Sim Poste (solar)", "solar", 1, ("B",), bateria=55.0),
        No(0x51B00005, "TRIL", "Sim Trilha (bateria)", "bateria", 2, ("B",), bateria=92.0),
        No(0x51B00006, "NOVO", "Sim No Novo", "solar", 2, ("A", "B"), entra_na_hora=20.0, bateria=70.0),
    ]
    for n in nos:
        n.contador = rnd.randrange(1, 10**6)
    return nos


def uso_canal_base(t: datetime) -> float:
    """Ciclo diario do canal: vale ~1,5% de madrugada, pico ~11% por volta das 21h."""
    h = _hora_local(t)
    return 6.0 + 4.5 * math.sin((h - 15) / 24 * 2 * math.pi) + (2.0 if 19 <= h <= 23 else 0.0)


def sol(t: datetime) -> float:
    """Irradiancia relativa 0..1 (nasce 6h, pico 12h, se poe 18h, horario local)."""
    h = _hora_local(t)
    return max(0.0, math.sin((h - 6) / 12 * math.pi)) if 6 <= h <= 18 else 0.0


def gerar_documentos(fim: datetime, horas: float = 72, semente: int = 42) -> list[dict]:
    rnd = random.Random(semente)
    inicio = fim - timedelta(hours=horas)
    nos = criar_nos(rnd)
    docs: list[dict] = []
    # gateway B fora do ar por 3h no meio da simulacao
    fora_b = (inicio + timedelta(hours=horas * 0.55), inicio + timedelta(hours=horas * 0.55 + 3))
    rajadas = [inicio + timedelta(hours=rnd.uniform(0, horas)) for _ in range(5)]

    def envelope(recebido: datetime, msg: dict, gw: str) -> dict:
        return {
            "topico": f"sensores/tempUmid/2/json/LongFast/{_hex(GATEWAYS[gw])}",
            "recebido_em": recebido,
            "mensagem": msg,
            "_sintetico": True,
        }

    def publicar(no: No, t: datetime, tipo: str, payload: dict, relogio: Optional[int] = None) -> None:
        no.contador += 1
        ouvintes = [g for g in no.gateways if not (g == "B" and fora_b[0] <= t < fora_b[1])]
        ouvintes = [g for g in ouvintes if rnd.random() > 0.05 + 0.04 * no.saltos]  # perda por salto
        saltos = no.saltos if rnd.random() > 0.15 else max(0, no.saltos + rnd.choice([-1, 1]))
        base = {
            "from": no.num, "to": BROADCAST, "id": no.contador, "type": tipo, "channel": 0,
            "timestamp": _ts(t) if relogio is None else relogio,
            "hops_away": saltos, "hop_start": 3,
            "rssi": rnd.randrange(-118, -70) - 8 * saltos, "snr": round(rnd.uniform(-9, 9) - 2 * saltos, 2),
            "payload": payload,
        }
        for i, gw in enumerate(ouvintes):  # mais de um gateway -> mesmo (from, id) duplicado
            msg = dict(base, sender=_hex(GATEWAYS[gw]), rssi=base["rssi"] - 3 * i)
            docs.append(envelope(t + timedelta(milliseconds=rnd.randrange(50, 2500) + 900 * i), msg, gw))

    for no in nos:
        t = inicio + timedelta(hours=no.entra_na_hora, seconds=rnd.randrange(0, 1800))
        no.uptime_desde = t
        prox_info = t
        while t < fim:
            passo = timedelta(minutes=30, seconds=rnd.randrange(-45, 45))
            h = _hora_local(t)
            # energia
            if no.energia == "solar":
                no.bateria += sol(t) * rnd.uniform(5.0, 8.0) - rnd.uniform(1.2, 1.8)
            elif no.energia == "bateria":
                no.bateria -= rnd.uniform(0.6, 0.9)
            no.bateria = min(100.0, no.bateria)
            if no.energia != "tomada" and no.bateria < 8:
                no.ligado = False  # desliga por bateria baixa
            if not no.ligado:
                if no.energia == "solar" and sol(t) > 0.3:
                    no.bateria += sol(t) * 6
                    if no.bateria > 15:  # religa com sol: uptime zera
                        no.ligado, no.uptime_desde = True, t
                t += passo
                continue

            if t >= prox_info:  # nodeinfo a cada ~3h
                publicar(no, t + timedelta(seconds=5), "nodeinfo", {
                    "id": _hex(no.num), "longname": no.longo, "shortname": no.curto, "hardware": 110, "role": 0,
                })
                prox_info = t + timedelta(hours=3, minutes=rnd.randrange(-20, 20))

            nivel = 101 if no.energia == "tomada" else max(0, int(no.bateria))
            tensao = 4.45 + rnd.uniform(-0.004, 0.004) if nivel == 101 else 3.3 + 0.9 * no.bateria / 100 + rnd.uniform(-0.02, 0.02)
            canal = uso_canal_base(t) + rnd.gauss(0, 0.8)
            if any(abs((t - r).total_seconds()) < 1800 for r in rajadas):
                canal += rnd.uniform(10, 22)  # rajada (muita gente mandando texto)
            relogio = None
            sorteio = rnd.random()
            if sorteio < 0.02:
                relogio = 0  # RTC sem hora
            elif sorteio < 0.03:
                relogio = 946684800 + rnd.randrange(0, 1000)  # ano 2000
            publicar(no, t, "telemetry", {
                "battery_level": nivel, "voltage": round(tensao, 3),
                "channel_utilization": round(max(0.0, canal), 2),
                "air_util_tx": round(max(0.0, canal * rnd.uniform(0.05, 0.15)), 3),
                "uptime_seconds": int((t - no.uptime_desde).total_seconds()),
            }, relogio)
            if no.sensor:  # telemetria ambiental: temperatura com ciclo diario
                temp = 21 + 6 * math.sin((h - 9) / 24 * 2 * math.pi) + rnd.gauss(0, 0.4)
                publicar(no, t + timedelta(seconds=20), "telemetry", {
                    "temperature": round(temp, 2),
                    "relative_humidity": round(min(100, max(20, 85 - 2.2 * (temp - 15) + rnd.gauss(0, 2))), 1),
                    "barometric_pressure": round(925 + rnd.gauss(0, 0.6), 1),
                })
            if 8 <= h <= 22 and rnd.random() < 0.12:
                publicar(no, t + timedelta(minutes=rnd.randrange(1, 25)), "text", {"text": "teste de alcance"})
            t += passo

    # sujeira controlada para a quarentena
    gw = "A"
    for i in range(12):
        quando = inicio + timedelta(minutes=rnd.randrange(1, int(horas * 60)))
        docs.append({"topico": f"sensores/tempUmid/2/json/LongFast/{_hex(GATEWAYS[gw])}", "recebido_em": quando,
                     "mensagem_raw": "{\"from\": 1, \"type\": \"tele", "_sintetico": True})
        docs.append(envelope(quando, {"from": nos[0].num, "id": 9_000_000 + i, "type": "foguete", "payload": {}}, gw))
        docs.append(envelope(quando, {"id": 9_100_000 + i, "type": "telemetry", "payload": {"voltage": 3.9}}, gw))
        docs.append(envelope(quando, {"from": nos[1].num, "id": 9_200_000 + i, "type": "telemetry",
                                      "payload": {"voltage": "alto"}}, gw))
    docs.sort(key=lambda d: d["recebido_em"])
    return docs


def limpar(destinos, redis_client, prefixo: str) -> None:
    for tabela in reversed(destinos.t.metadata.sorted_tables):
        tabela.drop(destinos.engine, checkfirst=True)
    chaves = list(redis_client.scan_iter(f"{prefixo}:*", count=500))
    if chaves:
        redis_client.delete(*chaves)


def executar_consultas(engine, prefixo: str) -> list[tuple[str, list[str], list[tuple]]]:
    from sqlalchemy import text

    sys.path.insert(0, str(RAIZ / "consultas"))
    from executar_consultas import ler_sql, listar_consultas

    saida = []
    with engine.connect() as conn:
        for caminho in listar_consultas():
            sql = ler_sql(caminho).replace("jonah_", prefixo + "_")
            res = conn.execute(text(sql))
            saida.append((caminho.name, list(res.keys()), [tuple(r) for r in res.fetchall()]))
    return saida


def tabela_md(colunas: list[str], linhas: list[tuple]) -> str:
    def fmt(v):
        if v is None:
            return ""
        return f"{v:.3f}".rstrip("0").rstrip(".") if isinstance(v, float) else str(v)

    cab = "| " + " | ".join(colunas) + " |\n|" + "---|" * len(colunas) + "\n"
    return cab + "\n".join("| " + " | ".join(fmt(v) for v in l) + " |" for l in linhas)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--horas", type=float, default=72)
    ap.add_argument("--semente", type=int, default=42)
    ap.add_argument("--prefixo", default="jonahsim")
    ap.add_argument("--limpar", action="store_true", help="apaga as tabelas e chaves do prefixo antes de rodar")
    args = ap.parse_args(argv)

    if args.prefixo == "jonah" or not args.prefixo.startswith("jonah") or not args.prefixo.isidentifier():
        print("RECUSADO: o prefixo da simulacao deve comecar com 'jonah' e ser diferente de 'jonah'.", file=sys.stderr)
        return 2

    import logging

    import mongomock
    import redis as redislib

    from pipeline import config as cfgmod
    from pipeline.destinos import Destinos, criar_engine
    from pipeline.fonte import Fonte
    from pipeline.main import Pipeline

    logging.basicConfig(level="WARNING", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = cfgmod.carregar()
    cfg.prefixo = args.prefixo
    cfg.intervalo_esperado_min = 30  # telemetria a cada 30 min -> lacuna = mais de 60 min
    cfg.sobreposicao_segundos = 0
    if not cfg.mariadb_url or not cfg.redis_url:
        print("Defina MARIADB_URL e REDIS_URL no .env.", file=sys.stderr)
        return 2

    fim = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    docs = gerar_documentos(fim, args.horas, args.semente)
    colecao = mongomock.MongoClient()["simulacao"]["pacotes_brutos"]
    colecao.insert_many(docs)

    engine = criar_engine(cfg.mariadb_url)
    r = redislib.Redis.from_url(cfg.redis_url, decode_responses=True, socket_timeout=10)
    destinos = Destinos(engine, r, cfg.prefixo, cfg.limite_lacuna_s)
    if args.limpar:
        limpar(destinos, r, cfg.prefixo)
    pipe = Pipeline(cfg, Fonte(colecao, "pacotes_brutos"), destinos)
    pipe.iniciar()
    print(f"SIMULACAO: {len(docs)} documentos sinteticos, {args.horas:.0f}h ate {fim:%Y-%m-%d %H:%M} UTC; processando...")
    lidos = pipe.ciclo()

    contadores = destinos.ler_contadores()
    consultas = executar_consultas(engine, cfg.prefixo)

    PASTA_RELATORIO.mkdir(parents=True, exist_ok=True)
    rel = PASTA_RELATORIO / "relatorio_simulacao.md"
    partes = [
        "# SIMULACAO de 72h (dados gerados por programa)\n",
        "> **Estes numeros NAO sao operacao real.** Os pacotes foram gerados por "
        "`ferramentas/simular_72h.py` (semente %d) e processados pelo pipeline real, "
        "gravando nas tabelas `%s_*` e chaves `%s:*`. A operacao real esta nas tabelas `jonah_*`.\n"
        % (args.semente, cfg.prefixo, cfg.prefixo),
        f"- Janela simulada: {fim - timedelta(hours=args.horas):%Y-%m-%d %H:%M} a {fim:%Y-%m-%d %H:%M} UTC "
        f"(processada em segundos, em {datetime.now():%d/%m/%Y %H:%M})",
        f"- Documentos gerados: {len(docs)}; lidos pelo pipeline: {lidos}\n",
        "## Contadores do pipeline\n",
        tabela_md(["contador", "valor"], sorted(contadores.items())),
    ]
    for nome, colunas, linhas in consultas:
        partes += [f"\n## {nome}\n", tabela_md(colunas, linhas) if linhas else "_sem linhas_"]
    rel.write_text("\n".join(partes) + "\n", encoding="utf-8")

    print("=" * 70)
    print(f"SIMULACAO concluida: {lidos} documentos lidos. Tabelas {cfg.prefixo}_*, chaves {cfg.prefixo}:*")
    for k, v in sorted(contadores.items()):
        if ":" not in k:
            print(f"  {k}: {v}")
    print(f"Relatorio: {rel}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
