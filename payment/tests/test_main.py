"""Testes do serviço Payment. Correr com: python -m pytest (dentro da pasta payment/)."""

import time
from datetime import datetime

import pytest

import main
from conftest import payment_request


def configure(client, **faults):
    response = client.post("/payments/configure-faults", json=faults)
    assert response.status_code == 200, response.text
    return response.json()


# --- Pagamentos ----------------------------------------------------------------

def test_successful_payment_returns_201_with_the_payment(client):
    request = payment_request(amount=170.0)

    response = client.post("/payments", json=request)

    assert response.status_code == 201
    payment = response.json()
    assert payment["order_id"] == request["order_id"]
    assert payment["amount"] == 170.0
    assert payment["status"] == "SUCCESS"
    assert isinstance(payment["id"], int)
    datetime.fromisoformat(payment["processed_at"])  # timestamp ISO 8601
    assert response.headers["location"] == f"/payments/{payment['id']}"


def test_payment_can_be_read_by_id(client):
    created = client.post("/payments", json=payment_request()).json()

    response = client.get(f"/payments/{created['id']}")

    assert response.status_code == 200
    assert response.json() == created


def test_unknown_payment_returns_404(client):
    response = client.get("/payments/999999")

    assert response.status_code == 404


def test_payments_are_kept_in_the_database_after_a_restart(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    import main

    monkeypatch.setattr(main, "PAYMENTS_DB_PATH", tmp_path / "payments.db")
    with TestClient(main.app) as first:
        created = first.post("/payments", json=payment_request()).json()
    with TestClient(main.app) as second:
        assert second.get(f"/payments/{created['id']}").json() == created


def test_order_id_is_required(client):
    request = payment_request()
    del request["order_id"]

    assert client.post("/payments", json=request).status_code == 422


def test_amount_must_be_positive(client):
    assert client.post("/payments", json=payment_request(amount=0)).status_code == 422
    assert client.post("/payments", json=payment_request(amount=-10)).status_code == 422


def test_infinite_amount_is_rejected_with_422_and_not_500(client):
    # Um 500 aqui contava como falha do pagamento no dashboard do circuit breaker
    response = client.post("/payments", content='{"order_id": "abc", "amount": Infinity}',
                           headers={"Content-Type": "application/json"})

    assert response.status_code == 422


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


# --- Falhas injetadas: taxa de erros --------------------------------------------

def test_injected_failure_returns_500_with_the_exact_payload(client):
    configure(client, error_rate=1)

    response = client.post("/payments", json=payment_request())

    assert response.status_code == 500
    assert response.json() == {"error": "Payment failed", "status": "FAILED"}


def test_without_faults_every_payment_succeeds(client):
    configure(client, error_rate=0)

    statuses = {client.post("/payments", json=payment_request()).status_code for _ in range(20)}

    assert statuses == {201}


def test_a_payment_fails_when_the_draw_is_below_the_error_rate(client, monkeypatch):
    configure(client, error_rate=0.5)

    monkeypatch.setattr(main.random, "random", lambda: 0.49)
    assert client.post("/payments", json=payment_request()).status_code == 500

    monkeypatch.setattr(main.random, "random", lambda: 0.5)
    assert client.post("/payments", json=payment_request()).status_code == 201


def test_stats_count_successful_and_failed_payments(client):
    assert client.get("/payments/stats").json() == {"SUCCESS": 0, "FAILED": 0}

    client.post("/payments", json=payment_request())
    configure(client, error_rate=1)
    client.post("/payments", json=payment_request())
    client.post("/payments", json=payment_request())

    assert client.get("/payments/stats").json() == {"SUCCESS": 1, "FAILED": 2}


# --- Falhas injetadas: latência -------------------------------------------------

def test_configured_latency_delays_the_payment(client):
    configure(client, latency_seconds=0.3)

    start = time.perf_counter()
    response = client.post("/payments", json=payment_request())
    elapsed = time.perf_counter() - start

    assert response.status_code == 201
    assert 0.3 <= elapsed < 1.5


def test_latency_is_applied_before_the_failure(client):
    configure(client, error_rate=1, latency_seconds=0.3)

    start = time.perf_counter()
    response = client.post("/payments", json=payment_request())
    elapsed = time.perf_counter() - start

    assert response.status_code == 500
    assert elapsed >= 0.3


# --- POST e GET /payments/configure-faults --------------------------------------

def test_configure_faults_returns_the_current_configuration(client):
    response = client.post("/payments/configure-faults", json={"error_rate": 0.8, "latency_seconds": 10})

    assert response.status_code == 200
    assert response.json() == {"error_rate": 0.8, "latency_seconds": 10.0}
    assert client.get("/payments/configure-faults").json() == {"error_rate": 0.8, "latency_seconds": 10.0}


def test_configure_faults_starts_without_faults(client):
    assert client.get("/payments/configure-faults").json() == {"error_rate": 0.0, "latency_seconds": 0.0}


def test_configure_faults_changes_only_the_fields_sent(client):
    configure(client, error_rate=0.8, latency_seconds=10)

    assert configure(client, error_rate=0) == {"error_rate": 0.0, "latency_seconds": 10.0}
    assert configure(client, latency_seconds=0) == {"error_rate": 0.0, "latency_seconds": 0.0}


@pytest.mark.parametrize("faults", [
    {"error_rate": 1.5},
    {"error_rate": -0.1},
    {"latency_seconds": -1},
    {"error_rate": "muito"},
    {},
])
def test_configure_faults_rejects_values_out_of_bounds(client, faults):
    response = client.post("/payments/configure-faults", json=faults)

    assert response.status_code == 422
    assert client.get("/payments/configure-faults").json() == {"error_rate": 0.0, "latency_seconds": 0.0}


def test_configure_faults_rejects_infinite_latency(client):
    # O JSON do Python aceita "Infinity"; um asyncio.sleep infinito deixaria os pedidos pendurados
    response = client.post("/payments/configure-faults", content='{"latency_seconds": Infinity}',
                           headers={"Content-Type": "application/json"})

    assert response.status_code == 422


# --- Configuração inicial (variáveis de ambiente) -------------------------------

def test_initial_faults_come_from_the_environment():
    faults = main.faults_from_env({"PAYMENT_ERROR_RATE": "0.5", "PAYMENT_LATENCY_SECONDS": "2"})

    assert faults.error_rate == 0.5
    assert faults.latency_seconds == 2.0


def test_initial_faults_default_to_none():
    faults = main.faults_from_env({})

    assert faults.error_rate == 0.0
    assert faults.latency_seconds == 0.0


def test_invalid_initial_faults_stop_the_service():
    with pytest.raises(ValueError):
        main.faults_from_env({"PAYMENT_ERROR_RATE": "2"})
