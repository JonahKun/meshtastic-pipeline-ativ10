import pytest

from .helpers import Ambiente


@pytest.fixture
def amb(tmp_path):
    return Ambiente(tmp_path / "teste.db")


@pytest.fixture
def fabrica(tmp_path):
    """Cria ambientes independentes (para comparar uma execucao com uma referencia)."""
    contador = {"n": 0}

    def criar(**kw):
        contador["n"] += 1
        return Ambiente(tmp_path / f"db{contador['n']}.db", **kw)

    return criar
