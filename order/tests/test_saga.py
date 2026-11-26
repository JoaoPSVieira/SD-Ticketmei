"""
Testes da Saga orquestrada (saga.py): reservar no Inventory -> cobrar no Payment -> confirmar,
e libertar os bilhetes (compensação) quando o pagamento falha.

O Inventory e o Payment são os falsos do conftest.py. O Inventory falso tem stock a sério, por isso
os testes confirmam também que o stock volta ao valor inicial depois de uma compensação.
"""

import sqlite3
import time

import pytest
from prometheus_client import REGISTRY

import main
import saga
from conftest import CALL_LOG, INITIAL_STOCK, VALID_ORDER

PAYMENT_UNAVAILABLE = {"error": "Payment system unavailable, try again later"}


def metric(name: str, labels: dict | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


class Metrics:
    """Fotografia das métricas da saga, para comparar antes e depois de cada teste."""

    NAMES = {
        "initiated": ("saga_transactions_initiated_total", None),
        "success": ("saga_transactions_success_total", None),
        "aborted": ("saga_transactions_aborted_total", None),
        "comp_success": ("saga_compensations_executed_total", {"outcome": "success"}),
        "comp_failed": ("saga_compensations_executed_total", {"outcome": "failed"}),
        "dur_success": ("saga_duration_seconds_count", {"outcome": "success"}),
        "dur_compensated": ("saga_duration_seconds_count", {"outcome": "compensated"}),
        "dur_aborted": ("saga_duration_seconds_count", {"outcome": "aborted"}),
        "confirmed": ("orders_initiated_total", {"status": "CONFIRMED"}),
        "cancelled": ("orders_initiated_total", {"status": "CANCELLED"}),
        "failed": ("orders_initiated_total", {"status": "FAILED"}),
    }

    def __init__(self):
        self.values = {key: metric(*spec) for key, spec in self.NAMES.items()}

    def delta(self) -> dict:
        """O que mudou desde a fotografia (só as métricas que mudaram)."""
        now = Metrics().values
        return {key: now[key] - self.values[key] for key in now if now[key] != self.values[key]}


def db_row(table: str, key: str, value: str) -> dict:
    """Lê uma linha com uma ligação nova (a do Order pertence a outra thread)."""
    with sqlite3.connect(main.ORDERS_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(f"SELECT * FROM {table} WHERE {key} = ?", (value,)).fetchone()
        return dict(row) if row else None


def steps(order_id: str) -> list[str]:
    """Os pedidos que a compra fez aos serviços falsos, por ordem."""
    return [step for step, oid in CALL_LOG if oid == order_id]


def last_order_id() -> str:
    """Id da última compra gravada (as respostas de erro do circuito aberto não trazem o id)."""
    with sqlite3.connect(main.ORDERS_DB_PATH) as conn:
        return conn.execute("SELECT id FROM orders ORDER BY rowid DESC LIMIT 1").fetchone()[0]


# --- Caminho feliz -----------------------------------------------------------

def test_successful_saga_reserves_then_pays_then_confirms(make_client, inventory, payment, published):
    before = Metrics()
    with make_client(with_inventory=True) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 201
    order = response.json()
    assert order["status"] == "CONFIRMED"
    assert steps(order["id"]) == ["reserve", "payment"]  # reservar antes de cobrar, sem libertar
    assert inventory.stock["evt-101"] == INITIAL_STOCK["evt-101"] - VALID_ORDER["quantity"]
    assert [o.id for o in published] == [order["id"]]  # só depois de CONFIRMED

    saga_row = db_row("sagas", "order_id", order["id"])
    assert saga_row["status"] == "SUCCEEDED"
    assert (saga_row["reserved"], saga_row["paid"], saga_row["step"]) == (1, 1, "DONE")
    assert saga_row["finished_at"] is not None
    assert before.delta() == {"initiated": 1, "success": 1, "dur_success": 1, "confirmed": 1}


def test_reserve_request_carries_the_order(make_client, inventory, payment):
    with make_client(with_inventory=True) as client:
        order = client.post("/orders", json={**VALID_ORDER, "quantity": 3}).json()

    assert inventory.reservations[order["id"]] == {"event_id": "evt-101", "quantity": 3, "status": "RESERVED"}


# --- Falhas na reserva: nada a compensar -------------------------------------

def test_not_enough_tickets_fails_with_409_without_charging(make_client, inventory, payment, published):
    inventory.stock["evt-104"] = 1
    before = Metrics()
    with make_client(with_inventory=True) as client:
        response = client.post("/orders", json={**VALID_ORDER, "event_id": "evt-104", "quantity": 2})

    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "Not enough tickets"
    assert db_row("orders", "id", body["order_id"])["status"] == "FAILED"
    assert db_row("sagas", "order_id", body["order_id"])["status"] == "ABORTED"
    assert steps(body["order_id"]) == ["reserve"]  # nem cobra nem liberta
    assert payment.requests == [] and published == []
    assert before.delta() == {"initiated": 1, "aborted": 1, "dur_aborted": 1, "failed": 1}


def test_event_without_stock_in_inventory_returns_422(make_client, inventory, payment):
    del inventory.stock["evt-104"]
    with make_client(with_inventory=True) as client:
        response = client.post("/orders", json={**VALID_ORDER, "event_id": "evt-104"})

    assert response.status_code == 422
    order_id = response.json()["order_id"]
    assert db_row("orders", "id", order_id)["status"] == "FAILED"
    assert steps(order_id) == ["reserve"]


def test_last_tickets_go_to_exactly_one_buyer(make_client, inventory, payment):
    inventory.stock["evt-104"] = 2
    with make_client(with_inventory=True) as client:
        codes = [client.post("/orders", json={**VALID_ORDER, "event_id": "evt-104", "quantity": 1}).status_code
                 for _ in range(4)]
        stats = client.get("/orders/stats").json()

    assert codes == [201, 201, 409, 409]
    assert inventory.stock["evt-104"] == 0
    assert stats["orders"]["CONFIRMED"] == 2 and stats["orders"]["FAILED"] == 2


def test_inventory_error_fails_with_503_and_releases_just_in_case(make_client, inventory, payment):
    inventory.reserve_mode = "fail"
    before = Metrics()
    with make_client(with_inventory=True) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 503
    body = response.json()
    assert body["error"] == "Inventory unavailable, try again later"
    assert db_row("orders", "id", body["order_id"])["status"] == "FAILED"
    # Não se sabe se a reserva ficou feita: liberta por segurança (o release é idempotente) e não cobra
    assert steps(body["order_id"]) == ["reserve", "release"]
    assert payment.requests == []
    assert db_row("sagas", "order_id", body["order_id"])["status"] == "COMPENSATED"
    assert before.delta() == {"initiated": 1, "aborted": 1, "comp_success": 1, "dur_compensated": 1, "failed": 1}


def test_inventory_timeout_does_not_leave_tickets_reserved(make_client, inventory, payment):
    # A reserva demora mais do que o timeout: o Order desiste e liberta por segurança. O release chega
    # ao Inventory antes da reserva atrasada, e por isso a reserva atrasada é recusada (regra do contrato).
    inventory.reserve_mode = "slow"
    with make_client(with_inventory=True) as client:
        response = client.post("/orders", json=VALID_ORDER)
        time.sleep(0.6)  # deixar o Inventory falso acabar a reserva lenta

    assert response.status_code == 503
    order_id = response.json()["order_id"]
    assert steps(order_id) == ["reserve", "release"]
    assert payment.requests == []
    assert inventory.stock["evt-101"] == INITIAL_STOCK["evt-101"]  # nenhum bilhete ficou preso


def test_inventory_down_returns_503(make_client, inventory, payment, unused_url):
    with make_client(inventory_url=unused_url, compensation_attempts=1, inventory_timeout=10.0) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 503
    order_id = response.json()["order_id"]
    assert db_row("orders", "id", order_id)["status"] == "FAILED"
    assert payment.requests == []


# --- Falhas no pagamento: compensação ----------------------------------------

def test_failed_payment_releases_the_tickets_and_cancels(make_client, inventory, payment, published):
    payment.mode = "fail"
    before = Metrics()
    with make_client(with_inventory=True, attempts=2) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 502
    body = response.json()
    assert body["error"] == "Payment failed"
    order_id = body["order_id"]
    assert db_row("orders", "id", order_id)["status"] == "CANCELLED"
    assert steps(order_id) == ["reserve", "payment", "payment", "release"]  # o release exatamente uma vez
    assert inventory.stock["evt-101"] == INITIAL_STOCK["evt-101"]  # o stock voltou
    assert inventory.reservations[order_id]["status"] == "RELEASED"
    assert published == []
    saga_row = db_row("sagas", "order_id", order_id)
    assert (saga_row["status"], saga_row["reserved"], saga_row["paid"]) == ("COMPENSATED", 1, 0)
    assert saga_row["error"].startswith("pagamento:")
    assert before.delta() == {"initiated": 1, "aborted": 1, "comp_success": 1, "dur_compensated": 1,
                              "cancelled": 1}


def test_open_circuit_cancels_with_the_exact_503_and_releases(make_client, inventory, payment):
    payment.mode = "fail"
    with make_client(with_inventory=True, attempts=1, failure_threshold=1) as client:
        client.post("/orders", json=VALID_ORDER)  # abre o circuito
        payment.requests.clear()
        before = Metrics()
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 503
    assert response.json() == PAYMENT_UNAVAILABLE
    order_id = last_order_id()
    assert steps(order_id) == ["reserve", "release"]  # o Payment nem foi chamado
    assert payment.requests == []
    assert db_row("orders", "id", order_id)["status"] == "CANCELLED"
    assert inventory.stock["evt-101"] == INITIAL_STOCK["evt-101"]
    assert before.delta() == {"initiated": 1, "aborted": 1, "comp_success": 1, "dur_compensated": 1,
                              "cancelled": 1}


def test_compensation_is_retried_when_the_release_fails_once(make_client, inventory, payment):
    payment.mode = "fail"
    inventory.release_mode = "fail_once"
    with make_client(with_inventory=True, attempts=1) as client:
        order_id = client.post("/orders", json=VALID_ORDER).json()["order_id"]

    assert steps(order_id) == ["reserve", "payment", "release", "release"]
    assert db_row("sagas", "order_id", order_id)["status"] == "COMPENSATED"
    assert inventory.stock["evt-101"] == INITIAL_STOCK["evt-101"]


def test_failed_compensation_is_retried_in_the_background(make_client, inventory, payment):
    payment.mode = "fail"
    inventory.release_mode = "fail"
    before = Metrics()
    with make_client(with_inventory=True, attempts=1, compensation_attempts=2, recovery_seconds=0.1) as client:
        response = client.post("/orders", json=VALID_ORDER)
        order_id = response.json()["order_id"]

        # As tentativas da compensação falharam: a compra fica CANCELLED (não foi cobrada), mas os
        # bilhetes continuam reservados e a saga fica pendente
        assert response.status_code == 502
        assert db_row("sagas", "order_id", order_id)["status"] == "COMPENSATION_FAILED"
        assert db_row("orders", "id", order_id)["status"] == "CANCELLED"
        assert client.get("/orders/stats").json()["sagas"]["COMPENSATION_FAILED"] == 1

        # O Inventory volta: a recuperação em segundo plano liberta os bilhetes
        inventory.release_mode = "ok"
        deadline = time.monotonic() + 3
        while db_row("sagas", "order_id", order_id)["status"] != "COMPENSATED" and time.monotonic() < deadline:
            time.sleep(0.05)

    assert db_row("sagas", "order_id", order_id)["status"] == "COMPENSATED"
    assert inventory.stock["evt-101"] == INITIAL_STOCK["evt-101"]
    assert before.delta()["comp_failed"] == 1
    assert before.delta()["comp_success"] == 1


# --- Recuperação no arranque -------------------------------------------------

def seed_interrupted(db_path, order_id: str, step: str, reserved: int, paid: int) -> None:
    """Simula uma saga que ficou a meio: compra PENDING e saga RUNNING, como se o Order tivesse caído."""
    conn = main.open_database(db_path)
    conn.execute(
        "INSERT INTO orders VALUES (?, 'c', 'a@b.pt', 'evt-101', 2, 170.0, 'PENDING', '2026-11-25T10:00:00+00:00')",
        (order_id,),
    )
    conn.execute(
        "INSERT INTO sagas (order_id, status, step, reserved, paid, started_at) VALUES (?, 'RUNNING', ?, ?, ?, ?)",
        (order_id, step, reserved, paid, "2026-11-25T10:00:00+00:00"),
    )
    conn.close()


def test_interrupted_saga_after_payment_is_confirmed_on_startup(make_client, inventory, tmp_path):
    db_path = tmp_path / "orders.db"
    seed_interrupted(db_path, "ord-pago", step="CONFIRM", reserved=1, paid=1)

    with make_client(with_inventory=True, db_path=db_path) as client:
        status = client.get("/orders/ord-pago").json()["status"]

    assert status == "CONFIRMED"
    assert db_row("sagas", "order_id", "ord-pago")["status"] == "SUCCEEDED"
    assert steps("ord-pago") == []  # já estava pago: não liberta nada


def test_interrupted_saga_before_payment_releases_the_tickets(make_client, inventory, tmp_path):
    db_path = tmp_path / "orders.db"
    inventory.stock["evt-101"] -= 2
    inventory.reservations["ord-meio"] = {"event_id": "evt-101", "quantity": 2, "status": "RESERVED"}
    seed_interrupted(db_path, "ord-meio", step="PAY", reserved=1, paid=0)

    with make_client(with_inventory=True, db_path=db_path, recovery_seconds=0.05) as client:
        assert client.get("/orders/ord-meio").json()["status"] == "CANCELLED"
        deadline = time.monotonic() + 3
        while db_row("sagas", "order_id", "ord-meio")["status"] != "COMPENSATED" and time.monotonic() < deadline:
            time.sleep(0.05)

    assert db_row("sagas", "order_id", "ord-meio")["status"] == "COMPENSATED"
    assert steps("ord-meio") == ["release"]
    assert inventory.stock["evt-101"] == INITIAL_STOCK["evt-101"]


def test_interrupted_saga_without_inventory_just_fails(make_client, tmp_path):
    db_path = tmp_path / "orders.db"
    seed_interrupted(db_path, "ord-sem-inv", step="PAY", reserved=0, paid=0)

    with make_client(db_path=db_path) as client:
        assert client.get("/orders/ord-sem-inv").json()["status"] == "FAILED"

    assert db_row("sagas", "order_id", "ord-sem-inv")["status"] == "ABORTED"


def test_pending_order_without_saga_fails_on_startup(make_client, tmp_path):
    db_path = tmp_path / "orders.db"
    conn = main.open_database(db_path)
    conn.execute("INSERT INTO orders VALUES ('ord-orfa', 'c', 'a@b.pt', 'evt-101', 1, 85.0, 'PENDING', 'x')")
    conn.close()

    with make_client(db_path=db_path) as client:
        assert client.get("/orders/ord-orfa").json()["status"] == "FAILED"


# --- Sem Inventory (Fase 1) --------------------------------------------------

def test_without_inventory_the_saga_is_payment_then_confirm(make_client, payment, inventory):
    before = Metrics()
    with make_client() as client:
        assert client.get("/health").json()["inventory"] == "disabled"
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 201
    assert steps(response.json()["id"]) == ["payment"]
    assert before.delta() == {"initiated": 1, "success": 1, "dur_success": 1, "confirmed": 1}


def test_without_inventory_a_failed_payment_is_failed_not_cancelled(make_client, payment, inventory):
    payment.mode = "fail"
    with make_client(attempts=1) as client:
        order_id = client.post("/orders", json=VALID_ORDER).json()["order_id"]

    assert db_row("orders", "id", order_id)["status"] == "FAILED"
    assert db_row("sagas", "order_id", order_id)["status"] == "ABORTED"
    assert "release" not in steps(order_id)


# --- Endpoints e métricas ----------------------------------------------------

def test_stats_count_orders_and_sagas_by_status(make_client, inventory, payment):
    inventory.stock["evt-104"] = 1
    with make_client(with_inventory=True, attempts=1) as client:
        client.post("/orders", json=VALID_ORDER)                                    # CONFIRMED
        client.post("/orders", json={**VALID_ORDER, "event_id": "evt-104", "quantity": 2})  # FAILED (409)
        payment.mode = "fail"
        client.post("/orders", json=VALID_ORDER)                                    # CANCELLED
        stats = client.get("/orders/stats").json()

    assert stats == {
        "orders": {"PENDING": 0, "CONFIRMED": 1, "CANCELLED": 1, "FAILED": 1},
        "sagas": {"RUNNING": 0, "SUCCEEDED": 1, "ABORTED": 1, "COMPENSATED": 1, "COMPENSATION_FAILED": 0},
    }


def test_health_shows_inventory_enabled(make_client, inventory):
    with make_client(with_inventory=True) as client:
        assert client.get("/health").json()["inventory"] == "enabled"


def test_saga_metrics_are_exposed_with_the_names_of_the_assignment(client):
    text = client.get("/metrics").text
    for name in ("saga_transactions_initiated_total", "saga_transactions_success_total",
                 "saga_transactions_aborted_total", 'saga_compensations_executed_total{outcome="success"}',
                 'saga_compensations_executed_total{outcome="failed"}', 'saga_duration_seconds_bucket{le="0.1",outcome="success"}',
                 'orders_initiated_total{status="CANCELLED"}'):
        assert name in text, name


@pytest.mark.parametrize("mode", ["WAL", "DELETE"])
def test_database_opens_in_both_journal_modes(tmp_path, mode):
    conn = main.open_database(tmp_path / "orders.db", mode)
    assert conn.execute("PRAGMA journal_mode").fetchone()[0].upper() == mode
    assert {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")} >= {"orders", "sagas"}
    conn.close()


def test_count_sagas_lists_every_status(tmp_path):
    conn = main.open_database(tmp_path / "orders.db")
    assert saga.count_sagas(conn) == dict.fromkeys(saga.SAGA_STATUSES, 0)
    conn.close()
