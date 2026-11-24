"""
Testes do passo do pagamento no POST /orders: chamada ao Payment, retry com jitter e circuit breaker.

O Payment é o falso do conftest.py. Cada teste escolhe o modo dele (ok, fail, slow, reject) e a
configuração do retry e do circuito (make_client(attempts=..., failure_threshold=..., open_seconds=...)).
"""

import sqlite3
import time

from prometheus_client import REGISTRY

import main
from conftest import DOWN_SERVICE_TIMEOUT, PAYMENT_TEST_TIMEOUT, VALID_ORDER

PAYMENT_UNAVAILABLE = {"error": "Payment system unavailable, try again later"}


def metric(name: str, labels: dict | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


def client_requests(status: str) -> float:
    return metric("payment_client_requests_total", {"status": status})


def fallbacks() -> float:
    return metric("circuit_breaker_fallbacks_total")


def breaker_state() -> float:
    return metric("circuit_breaker_state")


# --- Pagamento aceite --------------------------------------------------------

def test_paid_order_is_confirmed_and_returns_201(client, payment, published):
    response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 201
    order = response.json()
    assert order["status"] == "CONFIRMED"
    assert client.get(f"/orders/{order['id']}").json()["status"] == "CONFIRMED"
    assert [p.id for p in published] == [order["id"]]


def test_payment_receives_the_order_id_and_the_amount(client, payment):
    order = client.post("/orders", json={**VALID_ORDER, "quantity": 3}).json()

    assert payment.requests == [{"order_id": order["id"], "amount": 255.0}]  # 3 x 85 €


def test_successful_payment_is_counted_as_2xx(client):
    before = client_requests("2xx")

    client.post("/orders", json=VALID_ORDER)

    assert client_requests("2xx") == before + 1


def test_without_payment_url_orders_are_confirmed_without_payment(make_client, payment):
    with make_client(payment_url="") as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 201
    assert response.json()["status"] == "CONFIRMED"
    assert payment.requests == []


# --- Pagamento falhado (circuito ainda fechado) ------------------------------

def test_failed_payment_is_retried_then_returns_502_and_order_failed(make_client, payment, published):
    payment.mode = "fail"
    before = client_requests("500")

    with make_client(attempts=3) as client:
        response = client.post("/orders", json=VALID_ORDER)
        assert response.status_code == 502
        body = response.json()
        assert body["error"] == "Payment failed"
        assert client.get(f"/orders/{body['order_id']}").json()["status"] == "FAILED"

    assert len(payment.requests) == 3          # 1 tentativa + 2 retries
    assert client_requests("500") == before + 3
    assert published == []                     # sem pagamento não há email


def test_payment_timeout_counts_as_a_failure(make_client, payment):
    payment.mode = "slow"
    before = client_requests("timeout")

    with make_client(attempts=2) as client:
        start = time.perf_counter()
        response = client.post("/orders", json=VALID_ORDER)
        elapsed = time.perf_counter() - start

    assert response.status_code == 502
    assert client_requests("timeout") == before + 2
    assert elapsed < 2 * PAYMENT_TEST_TIMEOUT + 1  # não fica à espera do Payment lento


def test_payment_down_counts_as_a_failure(make_client, unused_url):
    before = client_requests("error")

    # Timeout grande, como no teste do Catalog em baixo: no Windows, a ligação a uma porta fechada
    # só é recusada ao fim de ~2 s, e com o timeout curto dos testes contava como "timeout".
    with make_client(payment_url=unused_url, attempts=1, payment_timeout=DOWN_SERVICE_TIMEOUT) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 502
    assert client_requests("error") == before + 1


def test_4xx_from_payment_is_not_retried(make_client, payment):
    payment.mode = "reject"

    with make_client(attempts=3, failure_threshold=1) as client:
        response = client.post("/orders", json=VALID_ORDER)
        assert response.status_code == 502
        assert len(payment.requests) == 1
        # Um 4xx não é culpa do Payment: o circuito continua fechado
        assert client.get("/health").json()["payment"] == "CLOSED"


def test_payment_recovers_on_a_retry(make_client, payment):
    payment.mode = "fail_once"

    with make_client(attempts=3) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 201
    assert len(payment.requests) == 2  # a 1.ª falhou, o retry foi aceite


# --- Circuit breaker -----------------------------------------------------------

def test_circuit_opens_after_repeated_failures_and_fails_fast_with_503(make_client, payment):
    payment.mode = "fail"
    before_fallbacks = fallbacks()

    with make_client(attempts=1, failure_threshold=2) as client:
        assert client.post("/orders", json=VALID_ORDER).status_code == 502
        assert client.post("/orders", json=VALID_ORDER).status_code == 502
        assert breaker_state() == 1  # OPEN
        calls_before = len(payment.requests)

        start = time.perf_counter()
        response = client.post("/orders", json=VALID_ORDER)
        elapsed = time.perf_counter() - start

        assert response.status_code == 503
        assert response.json() == PAYMENT_UNAVAILABLE   # o payload exato do enunciado
        assert len(payment.requests) == calls_before    # o Payment não foi chamado
        assert elapsed < 0.5                            # falha rápida
        assert client.get("/health").json()["payment"] == "OPEN"

    assert fallbacks() == before_fallbacks + 1


def test_order_is_failed_when_the_circuit_is_open(make_client, payment):
    payment.mode = "fail"

    with make_client(attempts=1, failure_threshold=1) as client:
        client.post("/orders", json=VALID_ORDER)
        created = client.post("/orders", json=VALID_ORDER)
        assert created.status_code == 503

    # A resposta 503 tem só o payload do enunciado (sem id): vê-se o estado na base de dados
    with sqlite3.connect(main.ORDERS_DB_PATH) as conn:
        stored = conn.execute("SELECT status, COUNT(*) FROM orders GROUP BY status").fetchall()

    assert dict(stored) == {"FAILED": 2}


def test_open_circuit_does_not_count_payment_client_requests(make_client, payment):
    payment.mode = "fail"

    with make_client(attempts=1, failure_threshold=1) as client:
        client.post("/orders", json=VALID_ORDER)
        before = sum(client_requests(s) for s in ("2xx", "500", "timeout", "error"))

        client.post("/orders", json=VALID_ORDER)  # circuito aberto: não há pedido ao Payment

        assert sum(client_requests(s) for s in ("2xx", "500", "timeout", "error")) == before


def test_retries_stop_as_soon_as_the_circuit_opens(make_client, payment):
    payment.mode = "fail"

    with make_client(attempts=5, failure_threshold=2) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 503
    assert len(payment.requests) == 2  # abriu na 2.ª falha: as outras 3 tentativas não aconteceram


def test_circuit_goes_half_open_and_closes_when_payment_recovers(make_client, payment):
    payment.mode = "fail"

    with make_client(attempts=1, failure_threshold=1, open_seconds=0.2) as client:
        assert client.post("/orders", json=VALID_ORDER).status_code == 502
        assert breaker_state() == 1  # OPEN

        time.sleep(0.25)
        assert client.get("/health").json()["payment"] == "HALF_OPEN"
        assert breaker_state() == 2  # HALF_OPEN

        payment.mode = "ok"
        assert client.post("/orders", json=VALID_ORDER).status_code == 201
        assert breaker_state() == 0  # CLOSED
        assert client.post("/orders", json=VALID_ORDER).status_code == 201


def test_circuit_reopens_when_the_trial_call_fails(make_client, payment):
    payment.mode = "fail"

    with make_client(attempts=1, failure_threshold=1, open_seconds=0.2) as client:
        client.post("/orders", json=VALID_ORDER)
        time.sleep(0.25)

        assert client.post("/orders", json=VALID_ORDER).status_code == 502  # a chamada de teste falha
        assert breaker_state() == 1                                         # volta a abrir
        assert client.post("/orders", json=VALID_ORDER).status_code == 503


def test_breaker_uses_the_configured_values(make_client):
    with make_client(failure_threshold=7, open_seconds=12, half_open_max_calls=2) as client:
        breaker = client.app.state.breaker

    assert (breaker.failure_threshold, breaker.open_seconds, breaker.half_open_max_calls) == (7, 12, 2)


# --- Métricas ------------------------------------------------------------------

def test_metrics_endpoint_exposes_the_circuit_breaker_metrics(client):
    text = client.get("/metrics").text

    for name in ("circuit_breaker_state", "circuit_breaker_fallbacks_total", "payment_client_requests_total",
                 "orders_initiated_total"):
        assert name in text


# --- RabbitMQ no cluster global ------------------------------------------------

def test_rabbitmq_url_is_used_when_no_host_is_given(monkeypatch):
    monkeypatch.delenv("RABBITMQ_HOST", raising=False)
    monkeypatch.setattr(main, "RABBITMQ_URL", "amqp://u:p@rabbitmq:5672/")

    assert main.rabbitmq_url() == "amqp://u:p@rabbitmq:5672/"


def test_rabbitmq_url_is_built_from_parts_with_the_vhost_encoded(monkeypatch):
    monkeypatch.setenv("RABBITMQ_HOST", "rabbitmq.rabbitmq-system.svc.cluster.local")
    monkeypatch.setenv("RABBITMQ_VHOST", "/grupo01")
    monkeypatch.setenv("RABBITMQ_USER", "grupo01-user")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "p@ss/word")
    monkeypatch.delenv("RABBITMQ_PORT", raising=False)

    assert main.rabbitmq_url() == (
        "amqp://grupo01-user:p%40ss%2Fword@rabbitmq.rabbitmq-system.svc.cluster.local:5672/%2Fgrupo01"
    )
