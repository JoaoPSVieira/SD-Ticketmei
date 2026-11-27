"""
Infraestrutura de testes do Inventory.

- Base de dados: cada teste usa um ficheiro SQLite novo numa pasta temporária. No arranque o serviço
  carrega o stock inicial de data/stock.json, como em produção.
- Os testes preparam o stock que precisam pela própria API (POST /inventory/stock), como o k6 faz.
"""

import uuid

import pytest
from fastapi.testclient import TestClient

import main


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "INVENTORY_DB_PATH", tmp_path / "inventory.db")
    with TestClient(main.app) as c:
        yield c


def reserve_request(**overrides) -> dict:
    """Um pedido POST /inventory/reserve tal como o Order o envia (order/saga.py)."""
    request = {"order_id": str(uuid.uuid4()), "event_id": "evt-101", "quantity": 2}
    request.update(overrides)
    return request
