"""
Testes de integração com um RabbitMQ verdadeiro.

Correm só se houver um RabbitMQ acessível em RABBITMQ_TEST_URL (por omissão o do docker compose,
em localhost:5672, utilizador ticketmei). Caso contrário são saltados. Cada teste usa uma fila
própria, com nome único, apagada no fim: não interferem com a fila real order_created.
"""

import asyncio
import os
import time
import uuid

import aio_pika
import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

import main
from conftest import encode, order_created, read_log

RABBITMQ_TEST_URL = os.getenv("RABBITMQ_TEST_URL", "amqp://ticketmei:ticketmei@localhost:5672/")


def rabbitmq_available() -> bool:
    async def _check():
        connection = await aio_pika.connect(RABBITMQ_TEST_URL, timeout=2)
        await connection.close()

    try:
        asyncio.run(_check())
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not rabbitmq_available(), reason="Sem RabbitMQ acessível em RABBITMQ_TEST_URL")


def publish(queue_name: str, bodies: list[bytes]) -> None:
    """Publica mensagens na fila, como o Order faz (persistentes, exchange por omissão)."""

    async def _publish():
        connection = await aio_pika.connect(RABBITMQ_TEST_URL)
        async with connection:
            channel = await connection.channel()
            await channel.declare_queue(queue_name, durable=True)
            for body in bodies:
                await channel.default_exchange.publish(
                    aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=queue_name
                )

    asyncio.run(_publish())


def messages_in_queue(queue_name: str) -> int:
    """Mensagens à espera na fila, ainda não entregues a nenhum consumidor."""

    async def _count():
        connection = await aio_pika.connect(RABBITMQ_TEST_URL)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(queue_name, durable=True)
            return queue.declaration_result.message_count

    return asyncio.run(_count())


def delete_queue(queue_name: str) -> None:
    async def _delete():
        connection = await aio_pika.connect(RABBITMQ_TEST_URL)
        async with connection:
            channel = await connection.channel()
            await channel.queue_delete(queue_name)

    asyncio.run(_delete())


def processed_total() -> float:
    return sum(REGISTRY.get_sample_value("notifications_processed_total", {"status": s}) or 0.0
               for s in ("SENT", "FAILED"))


def wait_for_processed(target: float, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while processed_total() < target:
        if time.monotonic() > deadline:
            pytest.fail(f"Só {processed_total()} notificações processadas; esperava {target}")
        time.sleep(0.05)


@pytest.fixture
def queue_name():
    name = f"test_notifications_{uuid.uuid4().hex[:8]}"
    yield name
    delete_queue(name)


@pytest.fixture
def start_consumer(monkeypatch, log_path, queue_name):
    """Devolve uma função que arranca o Notification (a consumir a fila de teste)."""
    monkeypatch.setattr(main, "RABBITMQ_URL", RABBITMQ_TEST_URL)
    monkeypatch.setattr(main, "ORDER_QUEUE", queue_name)

    def _start():
        client = TestClient(main.app)
        client.__enter__()
        deadline = time.monotonic() + 15
        while client.get("/health").json()["rabbitmq"] != "connected":
            if time.monotonic() > deadline:
                pytest.fail("O Notification não ligou ao RabbitMQ")
            time.sleep(0.1)
        started.append(client)
        return client

    started = []
    yield _start
    for client in started:
        client.__exit__(None, None, None)


def test_messages_waiting_in_the_queue_are_processed_when_the_service_starts(start_consumer, queue_name, log_path):
    # As mensagens chegam com o Notification em baixo: ficam à espera na fila
    messages = [order_created() for _ in range(20)]
    publish(queue_name, [encode(m) for m in messages])
    assert messages_in_queue(queue_name) == 20
    before = processed_total()

    start_consumer()
    wait_for_processed(before + 20)

    assert messages_in_queue(queue_name) == 0  # todas confirmadas (ack)
    logged = {line["order_id"] for line in read_log(log_path) if line["status"] == "SENT"}
    assert logged == {m["order_id"] for m in messages}


def test_prefetch_processes_messages_in_parallel(start_consumer, queue_name, monkeypatch):
    monkeypatch.setattr(main, "PREFETCH_COUNT", 10)
    publish(queue_name, [encode(order_created()) for _ in range(50)])
    before = processed_total()

    start = time.monotonic()
    start_consumer()
    wait_for_processed(before + 50)
    elapsed = time.monotonic() - start

    # Uma a uma seriam 50 x 100 ms = 5 s; com 10 em paralelo, ~0,5 s (mais o tempo de ligar)
    assert elapsed < 3


def test_message_is_redelivered_after_an_unexpected_error(start_consumer, queue_name, monkeypatch):
    real_process = main.process_notification
    calls = []

    async def fails_the_first_time(body):
        calls.append(body)
        if len(calls) == 1:
            raise RuntimeError("erro inesperado a meio do processamento")
        return await real_process(body)

    monkeypatch.setattr(main, "process_notification", fails_the_first_time)
    publish(queue_name, [encode(order_created())])
    before = processed_total()

    start_consumer()
    wait_for_processed(before + 1)

    assert len(calls) == 2  # a mesma mensagem foi entregue duas vezes
    assert calls[0] == calls[1]
    assert messages_in_queue(queue_name) == 0
