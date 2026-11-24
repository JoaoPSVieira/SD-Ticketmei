"""
Infraestrutura de testes do Payment.

- Base de dados: cada teste usa um ficheiro SQLite novo numa pasta temporária.
- Falhas injetadas: cada TestClient arranca com a configuração inicial (sem falhas). Os testes que
  precisam de falhas mudam-nas pela própria API (POST /payments/configure-faults), como o k6 faz.
"""

import uuid

import pytest
from fastapi.testclient import TestClient

import main


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "PAYMENTS_DB_PATH", tmp_path / "payments.db")
    with TestClient(main.app) as c:
        yield c


def payment_request(**overrides) -> dict:
    """Um pedido POST /payments tal como o Order o vai enviar (ver o contrato, secção 8 do plano)."""
    request = {"order_id": str(uuid.uuid4()), "amount": 170.0}
    request.update(overrides)
    return request
