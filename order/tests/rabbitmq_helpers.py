"""Funções de apoio para os testes que usam um RabbitMQ verdadeiro."""

import asyncio
import json
import os

import aio_pika

# RabbitMQ usado nos testes de integração (o do docker compose, se estiver a correr)
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


def read_messages(queue_name: str) -> list[aio_pika.abc.AbstractIncomingMessage]:
    """Lê (e confirma) todas as mensagens que estão na fila neste momento."""

    async def _read():
        connection = await aio_pika.connect(RABBITMQ_TEST_URL)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(queue_name, durable=True)
            messages = []
            while (message := await queue.get(fail=False, no_ack=True)) is not None:
                messages.append(message)
            return messages

    return asyncio.run(_read())


def delete_queue(queue_name: str) -> None:
    async def _delete():
        connection = await aio_pika.connect(RABBITMQ_TEST_URL)
        async with connection:
            channel = await connection.channel()
            await channel.queue_delete(queue_name)

    asyncio.run(_delete())


def body(message) -> dict:
    return json.loads(message.body)
