"""Testes do serviço Order. Correr com: python -m pytest (dentro da pasta order/)."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from prometheus_client import REGISTRY

import main
from conftest import DOWN_SERVICE_TIMEOUT, SLOW_SECONDS, VALID_ORDER


def initiated(status: str) -> float:
    return REGISTRY.get_sample_value("orders_initiated_total", {"status": status}) or 0.0


def publish_failures() -> float:
    return REGISTRY.get_sample_value("order_publish_failures_total") or 0.0


def requests_count(method: str, endpoint: str, http_status: str) -> float:
    value = REGISTRY.get_sample_value(
        "order_requests_total", {"method": method, "endpoint": endpoint, "http_status": http_status}
    )
    return value or 0.0


# --- Criar compras -----------------------------------------------------------

def test_create_order_returns_201_with_confirmed_order(client):
    response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 201
    order = response.json()
    assert order["status"] == "CONFIRMED"
    assert order["customer_id"] == "cliente-1"
    assert order["customer_email"] == "ana@exemplo.pt"
    assert order["event_id"] == "evt-101"
    assert order["quantity"] == 2
    assert order["id"]
    assert order["created_at"]
    assert response.headers["location"] == f"/orders/{order['id']}"


def test_total_amount_uses_price_from_catalog(client):
    response = client.post("/orders", json={**VALID_ORDER, "event_id": "evt-104", "quantity": 3})

    assert response.json()["total_amount"] == 1050.0  # 3 x 350 €


def test_order_created_message_is_published(client, published):
    order = client.post("/orders", json=VALID_ORDER).json()

    assert len(published) == 1
    assert published[0].id == order["id"]
    assert published[0].customer_email == "ana@exemplo.pt"
    assert published[0].status == "CONFIRMED"


def test_created_order_can_be_read_back(client):
    created = client.post("/orders", json=VALID_ORDER).json()

    response = client.get(f"/orders/{created['id']}")

    assert response.status_code == 200
    assert response.json() == created


def test_unknown_order_returns_404(client):
    response = client.get("/orders/nao-existe")

    assert response.status_code == 404
    assert "nao-existe" in response.json()["detail"]


def test_orders_survive_a_restart(make_client, tmp_path):
    db = tmp_path / "persistente.db"
    with make_client(db_path=db) as client:
        order_id = client.post("/orders", json=VALID_ORDER).json()["id"]

    with make_client(db_path=db) as restarted:
        assert restarted.get(f"/orders/{order_id}").status_code == 200


# --- Validação do pedido -----------------------------------------------------

def test_more_than_four_tickets_is_rejected(client, published):
    response = client.post("/orders", json={**VALID_ORDER, "quantity": 5})

    assert response.status_code == 422
    assert published == []


def test_zero_tickets_is_rejected(client):
    assert client.post("/orders", json={**VALID_ORDER, "quantity": 0}).status_code == 422


def test_four_tickets_is_accepted(client):
    assert client.post("/orders", json={**VALID_ORDER, "quantity": 4}).status_code == 201


def test_invalid_email_is_rejected(client):
    assert client.post("/orders", json={**VALID_ORDER, "customer_email": "sem-arroba"}).status_code == 422


def test_missing_fields_are_rejected(client):
    assert client.post("/orders", json={"event_id": "evt-101"}).status_code == 422


def test_unknown_event_is_rejected(client, published):
    response = client.post("/orders", json={**VALID_ORDER, "event_id": "evt-999"})

    assert response.status_code == 422
    assert "evt-999" in response.json()["detail"]
    assert published == []


# --- Falhas do Catalog -------------------------------------------------------

def test_catalog_error_returns_502(client):
    assert client.post("/orders", json={**VALID_ORDER, "event_id": "boom"}).status_code == 502


def test_catalog_down_returns_502(make_client, unused_url, published):
    with make_client(catalog_url=unused_url, catalog_timeout=DOWN_SERVICE_TIMEOUT) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 502
    assert published == []


def test_catalog_timeout_returns_504(make_client):
    with make_client(catalog_timeout=SLOW_SECONDS / 5) as client:
        assert client.post("/orders", json={**VALID_ORDER, "event_id": "slow"}).status_code == 504


# --- Falhas do RabbitMQ ------------------------------------------------------

def test_rabbitmq_down_keeps_the_paid_order_confirmed(make_client):
    # A compra já foi paga: sem RabbitMQ só o email é que não é enviado (fica na métrica)
    before = publish_failures()
    with make_client(rabbitmq="down") as client:
        response = client.post("/orders", json=VALID_ORDER)
        assert response.status_code == 201
        order = response.json()
        assert order["status"] == "CONFIRMED"

        stored = client.get(f"/orders/{order['id']}").json()
        assert stored["status"] == "CONFIRMED"

    assert publish_failures() == before + 1


def test_health_does_not_depend_on_rabbitmq(make_client):
    with make_client(rabbitmq="down") as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "rabbitmq": "disconnected", "payment": "CLOSED",
                               "inventory": "disabled"}


def test_health_shows_when_payment_is_disabled(make_client):
    with make_client(payment_url="") as client:
        assert client.get("/health").json()["payment"] == "disabled"


# --- Métricas ----------------------------------------------------------------

def test_metrics_endpoint_exposes_order_metrics(client):
    client.post("/orders", json=VALID_ORDER)
    text = client.get("/metrics").text

    assert "orders_initiated_total" in text
    assert "order_requests_total" in text
    assert "order_request_duration_seconds_bucket" in text


def test_successful_orders_are_counted_as_confirmed(client):
    before = initiated("CONFIRMED")

    client.post("/orders", json=VALID_ORDER)

    assert initiated("CONFIRMED") == before + 1


def test_failed_orders_are_counted_as_failed(make_client, payment):
    payment.mode = "fail"
    before_failed, before_confirmed = initiated("FAILED"), initiated("CONFIRMED")

    with make_client() as client:
        client.post("/orders", json=VALID_ORDER)

    assert initiated("FAILED") == before_failed + 1
    assert initiated("CONFIRMED") == before_confirmed


def test_rejected_requests_are_not_counted_as_initiated(client):
    before = initiated("CONFIRMED") + initiated("FAILED")

    client.post("/orders", json={**VALID_ORDER, "quantity": 9})
    client.post("/orders", json={**VALID_ORDER, "event_id": "evt-999"})

    assert initiated("CONFIRMED") + initiated("FAILED") == before


def test_http_requests_are_counted_by_route_template(client):
    before_post = requests_count("POST", "/orders", "201")
    before_get = requests_count("GET", "/orders/{order_id}", "200")

    order_id = client.post("/orders", json=VALID_ORDER).json()["id"]
    client.get(f"/orders/{order_id}")

    assert requests_count("POST", "/orders", "201") == before_post + 1
    assert requests_count("GET", "/orders/{order_id}", "200") == before_get + 1
    assert REGISTRY.get_sample_value(
        "order_requests_total", {"method": "GET", "endpoint": f"/orders/{order_id}", "http_status": "200"}
    ) is None


def test_metrics_and_health_are_not_counted(client):
    client.get("/metrics")
    client.get("/health")

    assert requests_count("GET", "/metrics", "200") == 0
    assert requests_count("GET", "/health", "200") == 0


# --- RabbitMQ: exchange, fila e publicação (sem RabbitMQ, com objetos falsos) -------------

def test_connecting_declares_the_exchange_and_binds_the_queue(monkeypatch):
    channel = AsyncMock()
    channel.declare_queue.return_value = queue = AsyncMock()
    connection = AsyncMock()
    connection.channel.return_value = channel

    async def fake_connect(url, **kwargs):
        return connection

    monkeypatch.setattr(main.aio_pika, "connect_robust", fake_connect)
    app = SimpleNamespace(state=SimpleNamespace())

    asyncio.run(main.keep_rabbitmq_connected(app))

    channel.declare_exchange.assert_awaited_once_with(
        "orders.exchange", main.aio_pika.ExchangeType.DIRECT, durable=True
    )
    channel.declare_queue.assert_awaited_once_with("order_created", durable=True)
    queue.bind.assert_awaited_once_with(channel.declare_exchange.return_value, routing_key="order.created")
    assert app.state.amqp_exchange is channel.declare_exchange.return_value


def test_order_created_is_published_to_the_exchange_with_the_routing_key():
    exchange = AsyncMock()
    channel = SimpleNamespace(is_closed=False)
    app = SimpleNamespace(state=SimpleNamespace(amqp_channel=channel, amqp_exchange=exchange))
    order = main.Order(id="ord-1", customer_id="c", customer_email="a@b.pt", event_id="evt-101", quantity=2,
                       total_amount=170.0, status="CONFIRMED", created_at="2026-11-22T10:00:00+00:00")

    asyncio.run(main.publish_order_created(app, order))

    message = exchange.publish.await_args.args[0]
    assert exchange.publish.await_args.kwargs["routing_key"] == "order.created"
    assert json.loads(message.body)["status"] == "CONFIRMED"
    assert message.delivery_mode == main.aio_pika.DeliveryMode.PERSISTENT
