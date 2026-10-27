"""Executa as consultas de consultas/*.sql no MariaDB e imprime o resultado em tabela.

Uso:
    python consultas/executar_consultas.py            # as 3 consultas
    python consultas/executar_consultas.py 2          # so a de numero 2
    python consultas/executar_consultas.py --url mysql+pymysql://...   # (ou MARIADB_URL no .env)
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PASTA = Path(__file__).resolve().parent


def listar_consultas() -> list[Path]:
    return sorted(p for p in PASTA.glob("[0-9][0-9]_*.sql"))


def ler_sql(caminho: Path) -> str:
    return caminho.read_text(encoding="utf-8-sig").strip().rstrip(";")


def formatar_tabela(colunas: list[str], linhas: list[tuple]) -> str:
    textos = [["" if v is None else str(v) for v in linha] for linha in linhas]
    larguras = [max([len(c)] + [len(l[i]) for l in textos]) for i, c in enumerate(colunas)]
    sep = "+-" + "-+-".join("-" * w for w in larguras) + "-+"
    def fmt(vals):
        return "| " + " | ".join(v.ljust(w) for v, w in zip(vals, larguras)) + " |"
    saida = [sep, fmt(colunas), sep] + [fmt(l) for l in textos] + [sep]
    return "\n".join(saida)


def executar(engine, caminho: Path) -> tuple[list[str], list[tuple]]:
    from sqlalchemy import text

    with engine.connect() as conn:
        res = conn.execute(text(ler_sql(caminho)))
        return list(res.keys()), [tuple(r) for r in res.fetchall()]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("numero", nargs="?", type=int, help="numero da consulta (1, 2 ou 3); omitido = todas")
    ap.add_argument("--url", help="URL SQLAlchemy do MariaDB (padrao: MARIADB_URL)")
    args = ap.parse_args(argv)

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    url = args.url or os.environ.get("MARIADB_URL")
    if not url:
        print("Defina MARIADB_URL no .env ou use --url.", file=sys.stderr)
        return 2

    from sqlalchemy import create_engine

    engine = create_engine(url, pool_pre_ping=True)
    consultas = listar_consultas()
    if args.numero:
        consultas = [c for c in consultas if int(c.name[:2]) == args.numero]
        if not consultas:
            print(f"Consulta {args.numero} nao existe.", file=sys.stderr)
            return 2
    for caminho in consultas:
        print(f"\n== {caminho.name} ==")
        colunas, linhas = executar(engine, caminho)
        print(formatar_tabela(colunas, linhas))
        print(f"({len(linhas)} linha(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
