"""Testes do serviço Payment. Correr com: python -m pytest (dentro da pasta payment/)."""

import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

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


# --- Idempotência pelo order_id -------------------------------------------------

def test_paying_the_same_order_twice_returns_the_first_payment_with_200(client):
    request = payment_request()
    first = client.post("/payments", json=request)

    second = client.post("/payments", json=request)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json() == first.json()
    assert client.get("/payments/stats").json() == {"SUCCESS": 1, "FAILED": 0}


def test_an_order_already_paid_is_not_affected_by_new_faults(client):
    # Um retry do Order depois de um timeout não pode transformar um pagamento feito numa falha
    request = payment_request()
    first = client.post("/payments", json=request).json()
    configure(client, error_rate=1)

    retry = client.post("/payments", json=request)

    assert retry.status_code == 200
    assert retry.json() == first


def test_a_failed_payment_can_be_tried_again(client):
    request = payment_request()
    configure(client, error_rate=1)
    assert client.post("/payments", json=request).status_code == 500

    configure(client, error_rate=0)
    assert client.post("/payments", json=request).status_code == 201
    assert client.get("/payments/stats").json() == {"SUCCESS": 1, "FAILED": 1}


def test_concurrent_payments_of_the_same_order_charge_only_once(client):
    # Vários pedidos da mesma compra ao mesmo tempo (ex.: o retry chega enquanto o 1.º ainda espera)
    configure(client, latency_seconds=0.2)
    request = payment_request()

    with ThreadPoolExecutor(max_workers=5) as pool:
        responses = list(pool.map(lambda _: client.post("/payments", json=request), range(5)))

    assert sorted(r.status_code for r in responses) == [200, 200, 200, 200, 201]
    assert len({r.json()["id"] for r in responses}) == 1
    assert client.get("/payments/stats").json() == {"SUCCESS": 1, "FAILED": 0}


def test_the_database_refuses_two_successful_payments_for_the_same_order(tmp_path):
    conn = main.open_database(tmp_path / "payments.db")
    main.insert_payment(conn, "order-1", 10.0, "SUCCESS")
    main.insert_payment(conn, "order-1", 10.0, "FAILED")  # tentativas falhadas podem repetir-se

    with pytest.raises(sqlite3.IntegrityError):
        main.insert_payment(conn, "order-1", 10.0, "SUCCESS")
    conn.close()


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


# --- Métricas -------------------------------------------------------------------

def payment_requests(http_status: str) -> float:
    return REGISTRY.get_sample_value("payment_requests_total", {"http_status": http_status}) or 0.0


def all_payment_requests() -> float:
    return sum(payment_requests(code) for code in ("200", "201", "404", "422", "500"))


def durations() -> tuple[float, float]:
    """(número de observações, soma dos tempos) do payment_processing_duration_seconds."""
    return (REGISTRY.get_sample_value("payment_processing_duration_seconds_count") or 0.0,
            REGISTRY.get_sample_value("payment_processing_duration_seconds_sum") or 0.0)


def test_metrics_endpoint_exposes_the_payment_metrics(client):
    text = client.get("/metrics").text

    assert 'payment_requests_total{http_status="201"}' in text
    assert 'payment_requests_total{http_status="500"}' in text
    assert "payment_processing_duration_seconds_bucket" in text
    assert "payment_fault_error_rate" in text
    assert "payment_fault_latency_seconds" in text


def test_payment_requests_are_counted_by_http_status(client):
    request = payment_request()
    before = {code: payment_requests(code) for code in ("200", "201", "422", "500")}

    client.post("/payments", json=request)                        # 201
    client.post("/payments", json=request)                        # 200 (já paga)
    client.post("/payments", json=payment_request(amount=-1))     # 422
    configure(client, error_rate=1)
    client.post("/payments", json=payment_request())              # 500

    assert {code: payment_requests(code) - before[code] for code in before} == {
        "200": 1, "201": 1, "422": 1, "500": 1,
    }


def test_only_post_payments_is_counted(client):
    created = client.post("/payments", json=payment_request()).json()
    before = all_payment_requests(), durations()[0]

    client.get(f"/payments/{created['id']}")
    client.get("/payments/stats")
    client.get("/payments/configure-faults")
    configure(client, error_rate=0)
    client.get("/health")
    client.get("/metrics")

    assert (all_payment_requests(), durations()[0]) == before


def test_processing_duration_includes_the_injected_latency(client):
    configure(client, latency_seconds=0.3)
    count_before, sum_before = durations()

    client.post("/payments", json=payment_request())

    count_after, sum_after = durations()
    assert count_after == count_before + 1
    assert sum_after - sum_before >= 0.3


def test_duration_histogram_goes_up_to_15_seconds(client):
    # Para apanhar o atraso de 10 s do teste do circuit breaker num intervalo próprio
    text = client.get("/metrics").text

    assert 'payment_processing_duration_seconds_bucket{le="10.0"}' in text
    assert 'payment_processing_duration_seconds_bucket{le="15.0"}' in text


def test_fault_gauges_follow_the_configuration(client):
    assert REGISTRY.get_sample_value("payment_fault_error_rate") == 0.0
    assert REGISTRY.get_sample_value("payment_fault_latency_seconds") == 0.0

    configure(client, error_rate=0.8, latency_seconds=10)

    assert REGISTRY.get_sample_value("payment_fault_error_rate") == 0.8
    assert REGISTRY.get_sample_value("payment_fault_latency_seconds") == 10.0


def test_fault_gauges_start_with_the_initial_configuration(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "PAYMENTS_DB_PATH", tmp_path / "payments.db")
    monkeypatch.setenv("PAYMENT_ERROR_RATE", "0.25")

    with TestClient(main.app):
        assert REGISTRY.get_sample_value("payment_fault_error_rate") == 0.25


# --- SQLite e processos -----------------------------------------------------------

@pytest.mark.parametrize("mode", ["WAL", "DELETE"])
def test_journal_mode_is_configurable(tmp_path, mode):
    # WAL no cluster local; DELETE no global, porque o WAL não funciona bem em discos de rede (NFS)
    conn = main.open_database(tmp_path / "payments.db", journal_mode=mode)

    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == mode.lower()
    conn.close()


def test_invalid_journal_mode_is_refused(tmp_path):
    with pytest.raises(ValueError):
        main.open_database(tmp_path / "payments.db", journal_mode="WAL; DROP TABLE payments")


def test_the_service_refuses_to_run_with_more_than_one_process():
    main.check_single_process({"WEB_CONCURRENCY": "1"})
    main.check_single_process({})

    with pytest.raises(RuntimeError):
        main.check_single_process({"WEB_CONCURRENCY": "2"})
