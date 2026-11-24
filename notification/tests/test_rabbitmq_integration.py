"""
Testes de integração com um RabbitMQ verdadeiro.

Correm só se houver um RabbitMQ acessível em RABBITMQ_TEST_URL (por omissão o do docker compose,
em localhost:5672, utilizador ticketmei). Caso contrário são saltados. Cada teste usa uma fila e
uma exchange próprias, com nomes únicos, apagadas no fim: não interferem com a fila order_created
nem com a exchange orders.exchange verdadeiras.
"""

import asyncio
import os
import time
import uuid

import aio_pika
import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY
from yarl import URL

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


def publish_to_exchange(exchange_name: str, routing_key: str, bodies: list[bytes]) -> None:
    """Publica mensagens na exchange, como o Order da Parte II faz (orders.exchange, order.created)."""

    async def _publish():
        connection = await aio_pika.connect(RABBITMQ_TEST_URL)
        async with connection:
            channel = await connection.channel()
            exchange = await channel.get_exchange(exchange_name)  # tem de já existir (declarada pelo Notification)
            for body in bodies:
                await exchange.publish(
                    aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=routing_key
                )

    asyncio.run(_publish())


def exchange_exists(exchange_name: str) -> bool:
    """Declaração passiva: só confirma se a exchange existe, sem a criar."""

    async def _check():
        connection = await aio_pika.connect(RABBITMQ_TEST_URL)
        async with connection:
            channel = await connection.channel()
            try:
                await channel.declare_exchange(exchange_name, passive=True)
                return True
            except aio_pika.exceptions.ChannelNotFoundEntity:
                return False

    return asyncio.run(_check())


def declare_exchange(exchange_name: str, exchange_type: aio_pika.ExchangeType) -> None:
    """Declara a exchange como o Order a vai declarar. Falha se já existir com outro tipo ou outra durabilidade."""

    async def _declare():
        connection = await aio_pika.connect(RABBITMQ_TEST_URL)
        async with connection:
            channel = await connection.channel()
            await channel.declare_exchange(exchange_name, exchange_type, durable=True)

    asyncio.run(_declare())


def delete_exchange(exchange_name: str) -> None:
    async def _delete():
        connection = await aio_pika.connect(RABBITMQ_TEST_URL)
        async with connection:
            channel = await connection.channel()
            await channel.exchange_delete(exchange_name)

    asyncio.run(_delete())


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
def exchange_name():
    name = f"test_orders_{uuid.uuid4().hex[:8]}"
    yield name
    delete_exchange(name)


@pytest.fixture
def start_consumer(monkeypatch, log_path, queue_name, exchange_name):
    """Devolve uma função que arranca o Notification (a consumir a fila de teste, ligada à exchange de teste)."""
    monkeypatch.setattr(main, "RABBITMQ_URL", RABBITMQ_TEST_URL)
    monkeypatch.setattr(main, "ORDER_QUEUE", queue_name)
    monkeypatch.setattr(main, "ORDERS_EXCHANGE", exchange_name)

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


def test_messages_published_to_the_orders_exchange_are_processed(start_consumer, exchange_name, queue_name, log_path):
    # O caminho da Parte II: o Order publica em orders.exchange com a routing key order.created
    start_consumer()
    messages = [order_created(status="CONFIRMED") for _ in range(5)]
    before = processed_total()

    publish_to_exchange(exchange_name, "order.created", [encode(m) for m in messages])
    wait_for_processed(before + 5)

    assert messages_in_queue(queue_name) == 0
    logged = {line["order_id"] for line in read_log(log_path) if line["status"] == "SENT"}
    assert logged == {m["order_id"] for m in messages}


def test_the_order_can_declare_the_same_exchange(start_consumer, exchange_name):
    # O Order (A1) também declara a exchange. Declarar uma que já existe só funciona com o mesmo tipo
    # e a mesma durabilidade: se não batessem certo, o RabbitMQ fechava o canal do Order com erro.
    assert not exchange_exists(exchange_name)
    start_consumer()
    assert exchange_exists(exchange_name)  # criada pelo Notification

    declare_exchange(exchange_name, aio_pika.ExchangeType.DIRECT)  # não pode levantar exceção

    with pytest.raises(aio_pika.exceptions.ChannelPreconditionFailed):
        declare_exchange(exchange_name, aio_pika.ExchangeType.FANOUT)


def test_messages_from_the_part_i_order_still_arrive(start_consumer, queue_name):
    # Enquanto o A1 não entra no main, o Order publica pela exchange por omissão (routing key = nome
    # da fila). A fila continua a recebê-las: o B2 pode entrar no main antes ou depois do A1.
    start_consumer()
    before = processed_total()

    publish(queue_name, [encode(order_created()) for _ in range(3)])
    wait_for_processed(before + 3)

    assert messages_in_queue(queue_name) == 0


def test_connects_with_the_rabbitmq_host_variables(start_consumer, monkeypatch):
    # Como no cluster global: endereço montado a partir das partes, com o vhost "/" codificado (%2F)
    test_url = URL(RABBITMQ_TEST_URL)
    url = main.rabbitmq_url_from_env({
        "RABBITMQ_HOST": test_url.host,
        "RABBITMQ_PORT": str(test_url.port),
        "RABBITMQ_VHOST": "/",
        "RABBITMQ_USER": test_url.user,
        "RABBITMQ_PASSWORD": test_url.password,
    })
    monkeypatch.setattr(main, "RABBITMQ_URL", url)

    client = start_consumer()  # falha o teste se não ligar

    assert client.get("/health").json()["rabbitmq"] == "connected"


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
