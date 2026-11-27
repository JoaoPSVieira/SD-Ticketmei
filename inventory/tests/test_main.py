"""Testes do serviço Inventory. Correr com: python -m pytest (dentro da pasta inventory/)."""

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

import main
from conftest import reserve_request


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


def available(client, event_id: str = "evt-101") -> int:
    return client.get(f"/inventory/{event_id}").json()["available_tickets"]


# --- POST /inventory/reserve ----------------------------------------------------------

def test_reserve_takes_the_tickets_and_returns_what_is_left(client):
    set_stock(client, "evt-101", 20)
    request = reserve_request(quantity=3)

    response = client.post("/inventory/reserve", json=request)

    assert response.status_code == 200
    assert response.json() == {"order_id": request["order_id"], "event_id": "evt-101", "quantity": 3,
                               "available_tickets": 17}
    assert available(client) == 17


def test_reserve_without_enough_tickets_returns_409_and_takes_nothing(client):
    set_stock(client, "evt-101", 2)

    response = client.post("/inventory/reserve", json=reserve_request(quantity=3))

    assert response.status_code == 409
    assert response.json() == {"error": "Not enough tickets"}
    assert available(client) == 2


def test_the_last_tickets_can_be_reserved(client):
    set_stock(client, "evt-101", 2)

    assert client.post("/inventory/reserve", json=reserve_request(quantity=2)).status_code == 200
    assert client.post("/inventory/reserve", json=reserve_request(quantity=1)).status_code == 409
    assert available(client) == 0


def test_reserve_for_an_event_without_stock_returns_404(client):
    response = client.post("/inventory/reserve", json=reserve_request(event_id="evt-999"))

    assert response.status_code == 404


def test_repeating_a_reservation_does_not_take_the_tickets_twice(client):
    # O Order pode repetir a reserva (retry): a mesma compra só desconta uma vez
    set_stock(client, "evt-101", 20)
    request = reserve_request(quantity=3)
    first = client.post("/inventory/reserve", json=request)

    second = client.post("/inventory/reserve", json=request)

    assert second.status_code == 200
    assert second.json() == first.json()
    assert available(client) == 17


def test_200_simultaneous_reservations_with_20_tickets_sell_exactly_20(client):
    set_stock(client, "evt-101", 20)

    with ThreadPoolExecutor(max_workers=50) as pool:
        codes = list(pool.map(lambda _: client.post("/inventory/reserve", json=reserve_request(quantity=1))
                              .status_code, range(200)))

    assert codes.count(200) == 20
    assert codes.count(409) == 180
    assert available(client) == 0


@pytest.mark.parametrize("change", [
    {"quantity": 0},
    {"quantity": -1},
    {"order_id": ""},
    {"event_id": ""},
])
def test_reserve_rejects_invalid_requests(client, change):
    assert client.post("/inventory/reserve", json=reserve_request(**change)).status_code == 422


# --- POST /inventory/release (compensação da Saga) --------------------------------------

def release(client, order_id: str):
    return client.post("/inventory/release", json={"order_id": order_id})


def test_release_gives_the_tickets_back(client):
    set_stock(client, "evt-101", 20)
    request = reserve_request(quantity=3)
    client.post("/inventory/reserve", json=request)

    response = release(client, request["order_id"])

    assert response.status_code == 200
    assert response.json() == {"released": True}
    assert available(client) == 20


def test_releasing_twice_gives_the_tickets_back_only_once(client):
    # O Order repete a compensação se não receber resposta: a segunda não pode repor outra vez
    set_stock(client, "evt-101", 20)
    request = reserve_request(quantity=3)
    client.post("/inventory/reserve", json=request)
    release(client, request["order_id"])

    response = release(client, request["order_id"])

    assert response.status_code == 200
    assert response.json() == {"released": False}
    assert available(client) == 20


def test_releasing_an_order_without_reservation_returns_false(client):
    response = release(client, "compra-sem-reserva")

    assert response.status_code == 200
    assert response.json() == {"released": False}


def test_a_reservation_that_arrives_after_its_release_is_refused(client):
    # A reserva demorou mais do que o timeout do Order (1 s): o Order desistiu e libertou "por
    # segurança". Se a reserva atrasada chegasse depois e descontasse, os bilhetes ficavam presos
    # numa compra FAILED. Por isso a libertação fica registada e a reserva atrasada é recusada.
    set_stock(client, "evt-101", 20)
    request = reserve_request(quantity=3)
    assert release(client, request["order_id"]).json() == {"released": False}

    response = client.post("/inventory/reserve", json=request)

    assert response.status_code == 409
    assert response.json() == {"error": "Order already released"}
    assert available(client) == 20


def test_a_released_order_cannot_reserve_again(client):
    set_stock(client, "evt-101", 20)
    request = reserve_request(quantity=3)
    client.post("/inventory/reserve", json=request)
    release(client, request["order_id"])

    response = client.post("/inventory/reserve", json=request)

    assert response.status_code == 409
    assert response.json() == {"error": "Order already released"}
    assert available(client) == 20


def test_set_stock_deletes_the_reservations_of_that_event_only(client):
    set_stock(client, "evt-101", 20)
    set_stock(client, "evt-102", 20)
    on_101, on_102 = reserve_request(quantity=3), reserve_request(event_id="evt-102", quantity=3)
    client.post("/inventory/reserve", json=on_101)
    client.post("/inventory/reserve", json=on_102)

    set_stock(client, "evt-101", 20)

    # A reserva do evt-101 já não existe: libertá-la não pode pôr o disponível acima do total
    assert release(client, on_101["order_id"]).json() == {"released": False}
    assert available(client, "evt-101") == 20
    assert release(client, on_102["order_id"]).json() == {"released": True}
    assert available(client, "evt-102") == 20


def test_release_requires_the_order_id(client):
    assert client.post("/inventory/release", json={}).status_code == 422


def test_the_database_refuses_negative_stock(tmp_path):
    # Mesmo que o código tivesse um erro, a base de dados não deixava vender bilhetes que não existem
    conn = main.open_database(tmp_path / "inventory.db")
    main.set_stock(conn, "evt-1", 1)

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE stock SET available_tickets = available_tickets - 2 WHERE event_id = 'evt-1'")
    conn.close()


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
