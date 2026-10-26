"""
Teste de integração com um RabbitMQ verdadeiro.

Corre só se houver um RabbitMQ acessível em RABBITMQ_TEST_URL (por omissão o do docker compose,
em localhost:5672, utilizador ticketmei). Caso contrário é saltado. Usa uma fila própria, com um
nome único, que é apagada no fim: não interfere com a fila real nem com o Notification.
"""

import time
import uuid

import pytest
from fastapi.testclient import TestClient

import main
from conftest import VALID_ORDER
from rabbitmq_helpers import RABBITMQ_TEST_URL, body, delete_queue, rabbitmq_available, read_messages

pytestmark = pytest.mark.skipif(not rabbitmq_available(), reason="Sem RabbitMQ acessível em RABBITMQ_TEST_URL")


@pytest.fixture
def queue_name():
    name = f"test_order_created_{uuid.uuid4().hex[:8]}"
    yield name
    delete_queue(name)


@pytest.fixture
def connected_client(monkeypatch, tmp_path, fake_catalog_url, queue_name):
    monkeypatch.setattr(main, "CATALOG_URL", fake_catalog_url)
    monkeypatch.setattr(main, "ORDERS_DB_PATH", tmp_path / "orders.db")
    monkeypatch.setattr(main, "RABBITMQ_URL", RABBITMQ_TEST_URL)
    monkeypatch.setattr(main, "ORDER_QUEUE", queue_name)

    with TestClient(main.app) as client:
        deadline = time.monotonic() + 15
        while client.get("/health").json()["rabbitmq"] != "connected":
            if time.monotonic() > deadline:
                pytest.fail("O Order não ligou ao RabbitMQ")
            time.sleep(0.2)
        yield client


def test_order_created_message_reaches_the_queue(connected_client, queue_name):
    order = connected_client.post("/orders", json=VALID_ORDER).json()
    assert order["status"] == "PENDING"

    [message] = read_messages(queue_name)

    assert body(message) == {
        "type": "OrderCreated",
        "order_id": order["id"],
        "customer_id": "cliente-1",
        "customer_email": "ana@exemplo.pt",
        "event_id": "evt-101",
        "quantity": 2,
        "total_amount": 170.0,
        "created_at": order["created_at"],
    }
    assert message.content_type == "application/json"
    assert message.type == "OrderCreated"
    assert message.message_id == order["id"]
    assert message.delivery_mode == 2  # persistente: o RabbitMQ grava-a em disco


def test_each_order_produces_exactly_one_message(connected_client, queue_name):
    ids = {connected_client.post("/orders", json=VALID_ORDER).json()["id"] for _ in range(20)}

    messages = read_messages(queue_name)

    assert {body(m)["order_id"] for m in messages} == ids
    assert len(messages) == 20
