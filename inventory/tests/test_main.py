"""Testes do serviço Inventory. Correr com: python -m pytest (dentro da pasta inventory/)."""

import json

import pytest
from fastapi.testclient import TestClient

import main


def set_stock(client, event_id: str, total_tickets: int) -> dict:
    response = client.post("/inventory/stock", json={"event_id": event_id, "total_tickets": total_tickets})
    assert response.status_code == 200, response.text
    return response.json()


# --- Stock inicial ----------------------------------------------------------------

def test_initial_stock_is_loaded_from_the_stock_file(client):
    expected = json.loads(main.STOCK_FILE.read_text(encoding="utf-8"))

    for entry in expected:
        stock = client.get(f"/inventory/{entry['event_id']}").json()
        assert stock == {"event_id": entry["event_id"], "total_tickets": entry["total_tickets"],
                         "available_tickets": entry["total_tickets"]}


def test_initial_stock_is_not_loaded_again_over_existing_stock(monkeypatch, tmp_path):
    # Depois de um reinício, o stock é o da base de dados, e não o do ficheiro
    monkeypatch.setattr(main, "INVENTORY_DB_PATH", tmp_path / "inventory.db")
    with TestClient(main.app) as first:
        set_stock(first, "evt-101", 20)
    with TestClient(main.app) as second:
        assert second.get("/inventory/evt-101").json()["available_tickets"] == 20


def test_stock_file_with_repeated_events_is_refused(tmp_path):
    bad = tmp_path / "stock.json"
    bad.write_text(json.dumps([{"event_id": "x", "total_tickets": 1}] * 2), encoding="utf-8")

    with pytest.raises(ValueError):
        main.load_stock_file(bad)


def test_stock_file_with_missing_fields_is_refused(tmp_path):
    bad = tmp_path / "stock.json"
    bad.write_text(json.dumps([{"event_id": "x"}]), encoding="utf-8")

    with pytest.raises(ValueError):
        main.load_stock_file(bad)


# --- GET /inventory/{event_id} e POST /inventory/stock -------------------------------

def test_unknown_event_returns_404(client):
    assert client.get("/inventory/evt-999").status_code == 404


def test_set_stock_defines_the_total_and_the_available_tickets(client):
    assert set_stock(client, "evt-101", 20) == {"event_id": "evt-101", "total_tickets": 20, "available_tickets": 20}
    assert client.get("/inventory/evt-101").json() == {"event_id": "evt-101", "total_tickets": 20,
                                                        "available_tickets": 20}


def test_set_stock_can_create_a_new_event(client):
    set_stock(client, "evt-500", 5)

    assert client.get("/inventory/evt-500").json()["available_tickets"] == 5


@pytest.mark.parametrize("body", [
    {"event_id": "evt-101", "total_tickets": -1},
    {"event_id": "evt-101"},
    {"total_tickets": 10},
    {"event_id": "", "total_tickets": 10},
])
def test_set_stock_rejects_invalid_requests(client, body):
    assert client.post("/inventory/stock", json=body).status_code == 422


def test_infinite_total_is_rejected_with_422_and_not_500(client):
    # O JSON do Python aceita Infinity; a resposta 422 por omissão do FastAPI rebentava a escrevê-lo
    response = client.post("/inventory/stock", content='{"event_id": "evt-101", "total_tickets": Infinity}',
                           headers={"Content-Type": "application/json"})

    assert response.status_code == 422


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
