"""Relatorio final dos contadores do pipeline (para o artigo e a entrega das 72h).

Uso:  python relatorio_contadores.py            (usa MARIADB_URL e PREFIXO do .env)
Os numeros vem de jonah_contadores e de contagens diretas nas tabelas: nada e estimado.
"""
from __future__ import annotations

import argparse
import os
import sys

from sqlalchemy import func, select

from pipeline.destinos import Destinos, criar_engine


def montar_relatorio(destinos: Destinos) -> str:
    t = destinos.t
    cont = destinos.ler_contadores()
    with destinos.engine.connect() as c:
        def n(tabela):
            return c.execute(select(func.count()).select_from(tabela)).scalar()

        inicio, fim = c.execute(select(func.min(t.vistos.c.recebido_em), func.max(t.vistos.c.recebido_em))).one()
        nos = c.execute(select(func.count(func.distinct(t.vistos.c.no)))).scalar()
        checkpoint = destinos.ler_checkpoint()
        linhas_tabelas = {
            "pacotes_vistos": n(t.vistos), "duplicatas": n(t.duplicatas), "serie": n(t.serie),
            "agg_hora": n(t.agg_hora), "agg_dia": n(t.agg_dia), "quarentena": n(t.quarentena), "lacunas": n(t.lacunas),
        }
    p = destinos.prefixo
    s = [f"RELATORIO DE CONTADORES ({p}_*)", "=" * 60]
    s.append(f"Periodo coberto (recebido_em, UTC): {inicio} ate {fim}")
    s.append(f"Nos distintos: {nos}")
    s.append(f"Checkpoint: {checkpoint[0] if checkpoint else '-'}")
    s.append("")
    s.append("Contadores cumulativos (jonah_contadores)")
    chaves_resumo = ["processados", "validos", "duplicatas", "timestamp_invalido", "quarentena", "sem_metrica", "medidas"]
    for k in chaves_resumo:
        s.append(f"  {k:<32}{cont.get(k, 0):>10}")
    for prefixo, titulo in (("timestamp_invalido:", "Timestamps invalidos por motivo"),
                            ("quarentena:", "Quarentena por motivo"), ("tipo:", "Pacotes validos por tipo")):
        itens = {k[len(prefixo):]: v for k, v in cont.items() if k.startswith(prefixo)}
        s.append("")
        s.append(titulo)
        s += [f"  {k:<32}{v:>10}" for k, v in sorted(itens.items())] or ["  (nenhum)"]
    s.append("")
    s.append("Linhas por tabela (contagem direta)")
    s += [f"  {p}_{k:<28}{v:>10}" for k, v in linhas_tabelas.items()]
    # conferencia interna: processados = validos + duplicatas + quarentena
    soma = cont.get("validos", 0) + cont.get("duplicatas", 0) + cont.get("quarentena", 0)
    s.append("")
    s.append(f"Conferencia: validos + duplicatas + quarentena = {soma} | processados = {cont.get('processados', 0)}"
             + ("  [OK]" if soma == cont.get("processados", 0) else "  [DIVERGENTE]"))
    return "\n".join(s)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", help="URL SQLAlchemy do MariaDB (padrao: MARIADB_URL)")
    ap.add_argument("--prefixo", help="padrao: PREFIXO ou jonah")
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
    destinos = Destinos(criar_engine(url), None, args.prefixo or os.environ.get("PREFIXO", "jonah"))
    print(montar_relatorio(destinos))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
