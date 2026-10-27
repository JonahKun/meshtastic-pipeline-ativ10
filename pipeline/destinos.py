"""Destinos: MariaDB (serie limpa, agregados, quarentena, contadores, checkpoint) e Redis.

Garantia central: `gravar_lote` faz TUDO que e MariaDB (dedup, serie, quarentena,
contadores, agregados, lacunas e checkpoint) numa UNICA transacao. Todo contador
so e incrementado quando um INSERT IGNORE realmente inseriu uma linha nova
(rowcount == 1); por isso reprocessar o mesmo documento nunca altera nada.

O dialeto fica isolado em `_insert_ignore` e `_upsert`: MariaDB/MySQL em producao
(INSERT IGNORE, ON DUPLICATE KEY UPDATE) e SQLite nos testes (INSERT OR IGNORE,
ON CONFLICT DO UPDATE). A logica de negocio nao conhece o dialeto.
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    delete,
    func,
    select,
    text,
)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .transformar import Resultado

log = logging.getLogger("pipeline.destinos")

ARQUIVO_SQL = Path(__file__).resolve().parent.parent / "schema" / "mariadb.sql"
NOME_CHECKPOINT = "principal"


# ------------------------------------------------------------------- esquema


class Esquema:
    """Tabelas (SQLAlchemy Core) com o prefixo configurado. Espelha schema/mariadb.sql."""

    def __init__(self, prefixo: str = "jonah"):
        self.prefixo = prefixo
        m = self.metadata = MetaData()
        p = prefixo + "_"
        self.vistos = Table(
            p + "pacotes_vistos", m,
            Column("no", String(16), primary_key=True),
            Column("pacote_id", BigInteger, primary_key=True),
            Column("bruto_id", String(64), nullable=False),
            Column("gateway", String(16)),
            Column("tipo", String(24), nullable=False),
            Column("recebido_em", DateTime, nullable=False),
        )
        self.duplicatas = Table(
            p + "duplicatas", m,
            Column("bruto_id", String(64), primary_key=True),
            Column("no", String(16), nullable=False),
            Column("pacote_id", BigInteger, nullable=False),
            Column("gateway", String(16)),
            Column("recebido_em", DateTime, nullable=False),
        )
        self.serie = Table(
            p + "serie", m,
            Column("no", String(16), primary_key=True),
            Column("pacote_id", BigInteger, primary_key=True),
            Column("metrica", String(32), primary_key=True),
            Column("instante", DateTime, nullable=False),
            Column("valor", Float, nullable=False),
            Column("instante_no", DateTime),
            Column("instante_no_valido", Boolean, nullable=False),
            Column("hops_away", Integer),
            Column("hop_start", Integer),
            Column("rssi", Integer),
            Column("snr", Float),
        )
        for nome in ("agg_hora", "agg_dia"):
            setattr(self, nome, Table(
                p + nome, m,
                Column("no", String(16), primary_key=True),
                Column("metrica", String(32), primary_key=True),
                Column("janela", DateTime, primary_key=True),
                Column("minimo", Float, nullable=False),
                Column("maximo", Float, nullable=False),
                Column("media", Float, nullable=False),
                Column("n", Integer, nullable=False),
            ))
        self.quarentena = Table(
            p + "quarentena", m,
            Column("bruto_id", String(64), primary_key=True),
            Column("motivo", String(64), nullable=False),
            Column("detalhe", String(500)),
            Column("recebido_em", DateTime),
            Column("documento", Text, nullable=False),
        )
        self.contadores = Table(
            p + "contadores", m,
            Column("nome", String(80), primary_key=True),
            Column("valor", BigInteger, nullable=False),
        )
        self.checkpoint = Table(
            p + "checkpoint", m,
            Column("nome", String(40), primary_key=True),
            Column("recebido_em", DateTime, nullable=False),
            Column("ultimo_id", String(64), nullable=False),
            Column("atualizado_em", DateTime, nullable=False),
        )
        self.nos = Table(
            p + "nos", m,
            Column("no", String(16), primary_key=True),
            Column("nome_curto", String(64)),
            Column("nome_longo", String(64)),
            Column("hardware", Integer),
            Column("papel", Integer),
            Column("atualizado_em", DateTime, nullable=False),
        )
        self.lacunas = Table(
            p + "lacunas", m,
            Column("no", String(16), primary_key=True),
            Column("inicio", DateTime, primary_key=True),
            Column("fim", DateTime, nullable=False),
            Column("duracao_s", Integer, nullable=False),
        )


def criar_engine(url: str):
    kwargs: dict = {}
    if not url.startswith("sqlite"):
        kwargs = {"pool_pre_ping": True, "pool_recycle": 1800}
    return create_engine(url, **kwargs)


# ------------------------------------------------------------------ resultado


@dataclass
class ResumoLote:
    contagens: Counter = field(default_factory=Counter)
    # (no, metrica) -> (instante, valor): ultimo valor NOVO deste lote
    ultimos: dict = field(default_factory=dict)
    # no -> info (nodeinfo) NOVA neste lote, para o Redis
    infos: dict = field(default_factory=dict)


def _inicio_hora(t: datetime) -> datetime:
    return t.replace(minute=0, second=0, microsecond=0)


def _inicio_dia(t: datetime) -> datetime:
    return t.replace(hour=0, minute=0, second=0, microsecond=0)


# ------------------------------------------------------------------- MariaDB


class Destinos:
    def __init__(self, engine, redis_client=None, prefixo: str = "jonah", limite_lacuna_s: int = 1800):
        self.engine = engine
        self.redis = redis_client
        self.prefixo = prefixo
        self.t = Esquema(prefixo)
        self.limite_lacuna_s = limite_lacuna_s
        self._mysql = engine.dialect.name in ("mysql", "mariadb")

    # ---- isolamento de dialeto -------------------------------------------

    def _insert_ignore(self, tabela):
        if self._mysql:
            return mysql_insert(tabela).prefix_with("IGNORE")
        return sqlite_insert(tabela).prefix_with("OR IGNORE")

    def _upsert(self, conn, tabela, valores: dict, pk: list[str], somar: Optional[list[str]] = None):
        """INSERT ... ON DUPLICATE KEY UPDATE (MariaDB) / ON CONFLICT DO UPDATE (SQLite).

        Colunas nao-PK sao substituidas; as listadas em `somar` sao incrementadas.
        """
        somar = somar or []
        if self._mysql:
            stmt = mysql_insert(tabela).values(**valores)
            novo = stmt.inserted
            atualizar = {
                c: (tabela.c[c] + novo[c] if c in somar else novo[c]) for c in valores if c not in pk
            }
            stmt = stmt.on_duplicate_key_update(**atualizar)
        else:
            stmt = sqlite_insert(tabela).values(**valores)
            novo = stmt.excluded
            atualizar = {
                c: (tabela.c[c] + novo[c] if c in somar else novo[c]) for c in valores if c not in pk
            }
            stmt = stmt.on_conflict_do_update(index_elements=pk, set_=atualizar)
        conn.execute(stmt)

    # ---- esquema ----------------------------------------------------------

    def criar_esquema(self) -> None:
        if not self._mysql:
            self.t.metadata.create_all(self.engine)
            return
        sql = ARQUIVO_SQL.read_text(encoding="utf-8-sig")
        sql = re.sub(r"--[^\n]*", "", sql)
        if self.prefixo != "jonah":
            sql = sql.replace("jonah_", self.prefixo + "_")
        with self.engine.begin() as conn:
            for comando in (c.strip() for c in sql.split(";")):
                if comando:
                    conn.execute(text(comando))

    # ---- checkpoint -------------------------------------------------------

    def ler_checkpoint(self) -> Optional[tuple[datetime, str]]:
        with self.engine.connect() as conn:
            linha = conn.execute(
                select(self.t.checkpoint.c.recebido_em, self.t.checkpoint.c.ultimo_id).where(
                    self.t.checkpoint.c.nome == NOME_CHECKPOINT
                )
            ).first()
        return (linha[0], linha[1]) if linha else None

    # ---- gravacao do lote (uma transacao) ----------------------------------

    def gravar_lote(
        self,
        itens: list[tuple[dict, Resultado]],
        checkpoint: Optional[tuple[datetime, str]],
    ) -> ResumoLote:
        t = self.t
        resumo = ResumoLote()
        cont = resumo.contagens
        janelas_hora: set = set()
        janelas_dia: set = set()
        nos_afetados: dict[str, list[datetime]] = {}

        with self.engine.begin() as conn:
            for doc, r in itens:
                if r.status == "quarentena":
                    res = conn.execute(self._insert_ignore(t.quarentena).values(
                        bruto_id=r.bruto_id,
                        motivo=r.motivo,
                        detalhe=(r.detalhe or "")[:500] or None,
                        recebido_em=r.recebido_em,
                        documento=json.dumps(doc, default=str, ensure_ascii=False),
                    ))
                    if res.rowcount == 1:
                        cont["processados"] += 1
                        cont["quarentena"] += 1
                        cont[f"quarentena:{r.motivo}"] += 1
                    continue

                res = conn.execute(self._insert_ignore(t.vistos).values(
                    no=r.no, pacote_id=r.pacote_id, bruto_id=r.bruto_id, gateway=r.gateway,
                    tipo=r.tipo, recebido_em=r.recebido_em,
                ))
                if res.rowcount != 1:
                    # (no, id) ja visto: ou e o MESMO documento relido (sobreposicao/reinicio)
                    # ou e outra copia do pacote vinda por outro gateway (duplicata real).
                    primeiro = conn.execute(
                        select(t.vistos.c.bruto_id).where(
                            t.vistos.c.no == r.no, t.vistos.c.pacote_id == r.pacote_id
                        )
                    ).scalar()
                    if primeiro != r.bruto_id:
                        res2 = conn.execute(self._insert_ignore(t.duplicatas).values(
                            bruto_id=r.bruto_id, no=r.no, pacote_id=r.pacote_id,
                            gateway=r.gateway, recebido_em=r.recebido_em,
                        ))
                        if res2.rowcount == 1:
                            cont["processados"] += 1
                            cont["duplicatas"] += 1
                    continue

                # Pacote novo.
                cont["processados"] += 1
                cont["validos"] += 1
                cont[f"tipo:{r.tipo}"] += 1
                if not r.instante_no_valido:
                    cont["timestamp_invalido"] += 1
                    cont[f"timestamp_invalido:{r.motivo_timestamp}"] += 1
                nos_afetados.setdefault(r.no, []).append(r.recebido_em)
                if r.info_no:
                    self._gravar_info_no(conn, r)
                    resumo.infos[r.no] = r.info_no
                if not r.medidas:
                    cont["sem_metrica"] += 1
                    continue
                linhas = [
                    dict(
                        no=r.no, pacote_id=r.pacote_id, metrica=m, instante=r.recebido_em, valor=v,
                        instante_no=r.instante_no, instante_no_valido=r.instante_no_valido,
                        hops_away=r.hops_away, hop_start=r.hop_start, rssi=r.rssi, snr=r.snr,
                    )
                    for m, v in r.medidas
                ]
                conn.execute(self._insert_ignore(t.serie), linhas)
                cont["medidas"] += len(linhas)
                for m, v in r.medidas:
                    janelas_hora.add((r.no, m, _inicio_hora(r.recebido_em)))
                    janelas_dia.add((r.no, m, _inicio_dia(r.recebido_em)))
                    atual = resumo.ultimos.get((r.no, m))
                    if atual is None or r.recebido_em >= atual[0]:
                        resumo.ultimos[(r.no, m)] = (r.recebido_em, v)

            # Derivados, ainda dentro da MESMA transacao.
            self._recalcular_agregados(conn, t.agg_hora, janelas_hora, timedelta(hours=1))
            self._recalcular_agregados(conn, t.agg_dia, janelas_dia, timedelta(days=1))
            for no, tempos in nos_afetados.items():
                self._recalcular_lacunas(conn, no, min(tempos), max(tempos))
            for nome, n in cont.items():
                if n:
                    self._upsert(conn, t.contadores, {"nome": nome, "valor": n}, ["nome"], somar=["valor"])
            if checkpoint is not None:
                self._avancar_checkpoint(conn, checkpoint)
        return resumo

    def _gravar_info_no(self, conn, r: Resultado) -> None:
        """Cadastro do no (nodeinfo): so substitui se este pacote for mais novo que o gravado."""
        n = self.t.nos
        atual = conn.execute(select(n.c.atualizado_em).where(n.c.no == r.no)).scalar()
        if atual is not None and atual > r.recebido_em:
            return
        self._upsert(conn, n, {"no": r.no, "atualizado_em": r.recebido_em, **r.info_no}, ["no"])

    def _avancar_checkpoint(self, conn, novo: tuple[datetime, str]) -> None:
        t = self.t.checkpoint
        atual = conn.execute(select(t.c.recebido_em, t.c.ultimo_id).where(t.c.nome == NOME_CHECKPOINT)).first()
        if atual is not None and (atual[0], atual[1]) >= (novo[0], novo[1]):
            return  # nunca regride
        self._upsert(
            conn, t,
            {"nome": NOME_CHECKPOINT, "recebido_em": novo[0], "ultimo_id": novo[1],
             "atualizado_em": datetime.now(timezone.utc).replace(tzinfo=None)},
            ["nome"],
        )

    def _recalcular_agregados(self, conn, tabela, janelas: set, tamanho: timedelta) -> None:
        s = self.t.serie
        for no, metrica, ini in sorted(janelas):
            linha = conn.execute(
                select(func.min(s.c.valor), func.max(s.c.valor), func.avg(s.c.valor), func.count())
                .where(s.c.no == no, s.c.metrica == metrica, s.c.instante >= ini, s.c.instante < ini + tamanho)
            ).one()
            if not linha[3]:
                continue
            self._upsert(
                conn, tabela,
                {"no": no, "metrica": metrica, "janela": ini, "minimo": float(linha[0]),
                 "maximo": float(linha[1]), "media": float(linha[2]), "n": int(linha[3])},
                ["no", "metrica", "janela"],
            )

    def _recalcular_lacunas(self, conn, no: str, tmin: datetime, tmax: datetime) -> None:
        """Recalcula as lacunas do no ao redor dos pacotes novos (idempotente).

        Lacuna = intervalo entre dois pacotes consecutivos > limite configurado.
        Nada e interpolado: so se registra QUE faltou e por quanto tempo.
        """
        v, lac = self.t.vistos, self.t.lacunas
        anterior = conn.execute(select(func.max(v.c.recebido_em)).where(v.c.no == no, v.c.recebido_em < tmin)).scalar()
        posterior = conn.execute(select(func.min(v.c.recebido_em)).where(v.c.no == no, v.c.recebido_em > tmax)).scalar()
        inferior = anterior or tmin
        consulta = select(v.c.recebido_em).where(v.c.no == no, v.c.recebido_em >= inferior)
        remover = delete(lac).where(lac.c.no == no, lac.c.inicio >= inferior)
        if posterior is not None:
            consulta = consulta.where(v.c.recebido_em <= posterior)
            remover = remover.where(lac.c.inicio < posterior)
        tempos = [x[0] for x in conn.execute(consulta.order_by(v.c.recebido_em)).all()]
        conn.execute(remover)
        for a, b in zip(tempos, tempos[1:]):
            dur = int((b - a).total_seconds())
            if dur > self.limite_lacuna_s:
                conn.execute(self._insert_ignore(lac).values(no=no, inicio=a, fim=b, duracao_s=dur))

    # ---- leitura de apoio (relatorios) --------------------------------------

    def ler_contadores(self) -> dict[str, int]:
        c = self.t.contadores
        with self.engine.connect() as conn:
            return {n: int(v) for n, v in conn.execute(select(c.c.nome, c.c.valor).order_by(c.c.nome))}

    # ---- Redis --------------------------------------------------------------

    def _chave(self, no: str, metrica: str) -> str:
        return f"{self.prefixo}:ultimo:{no}:{metrica}"

    def atualizar_info_redis(self, infos: dict) -> None:
        """Hash `jonah:no:<no>` com nome curto/longo/hardware (dado derivado do cadastro)."""
        if self.redis is None:
            return
        for no, info in infos.items():
            campos = {k: v for k, v in info.items() if v is not None}
            if campos:
                self.redis.hset(f"{self.prefixo}:no:{no}", mapping=campos)
            self.redis.sadd(f"{self.prefixo}:nos", no)

    def reconstruir_info_redis(self) -> None:
        n = self.t.nos
        with self.engine.connect() as conn:
            linhas = conn.execute(select(n.c.no, n.c.nome_curto, n.c.nome_longo, n.c.hardware, n.c.papel)).all()
        self.atualizar_info_redis({
            l[0]: dict(nome_curto=l[1], nome_longo=l[2], hardware=l[3], papel=l[4]) for l in linhas})

    def atualizar_redis(self, ultimos: dict) -> int:
        """Grava o ultimo valor por (no, metrica) SOMENTE se o instante for mais novo.

        Usa WATCH/MULTI/EXEC (optimistic locking): se outro processo mexer na
        chave entre a leitura e a escrita, a transacao e repetida. Idempotente:
        reaplicar o mesmo valor nao muda nada (instante nao e estritamente maior).
        """
        if self.redis is None:
            return 0
        atualizados = 0
        for (no, metrica), (instante, valor) in ultimos.items():
            ms = int(instante.replace(tzinfo=timezone.utc).timestamp() * 1000)
            chave = self._chave(no, metrica)

            def escrever(pipe, chave=chave, ms=ms, valor=valor, instante=instante):
                atual = pipe.hget(chave, "instante")
                if atual is not None and int(atual) >= ms:
                    return False
                pipe.multi()
                pipe.hset(chave, mapping={"valor": repr(valor), "instante": ms, "instante_iso": instante.isoformat() + "Z"})
                pipe.sadd(f"{self.prefixo}:nos", no)
                return True

            if self.redis.transaction(escrever, chave, value_from_callable=True):
                atualizados += 1
            else:
                self.redis.sadd(f"{self.prefixo}:nos", no)
        return atualizados

    def reconstruir_redis(self) -> int:
        """Reaplica o ultimo valor de cada (no, metrica) lido do MariaDB.

        Cobre a janela em que o processo cai depois do COMMIT e antes da escrita
        no Redis. Como a atualizacao e monotona, rodar isto sempre e seguro.
        """
        self.reconstruir_info_redis()
        s = self.t.serie
        m = (
            select(s.c.no, s.c.metrica, func.max(s.c.instante).label("mi"))
            .group_by(s.c.no, s.c.metrica)
            .subquery()
        )
        q = select(s.c.no, s.c.metrica, s.c.instante, s.c.valor).join(
            m, (s.c.no == m.c.no) & (s.c.metrica == m.c.metrica) & (s.c.instante == m.c.mi)
        )
        ultimos: dict = {}
        with self.engine.connect() as conn:
            for no, metrica, instante, valor in conn.execute(q):
                ultimos[(no, metrica)] = (instante, valor)
        return self.atualizar_redis(ultimos)
