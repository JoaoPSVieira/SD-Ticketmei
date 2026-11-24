"""
Testes do endereço do RabbitMQ (regra da secção 8 do plano da Parte II). Não precisam de RabbitMQ.

No cluster local usa-se RABBITMQ_URL. No cluster global (RabbitMQ partilhado da UMa) o endereço é
montado a partir de RABBITMQ_HOST, RABBITMQ_VHOST, RABBITMQ_USER e RABBITMQ_PASSWORD, com as
credenciais a virem do Secret que a docente criou.
"""

import asyncio

import aiormq
import pytest

import main

GLOBAL = {
    "RABBITMQ_HOST": "rabbitmq.rabbitmq-system.svc.cluster.local",
    "RABBITMQ_VHOST": "/grupo07",
    "RABBITMQ_USER": "grupo07",
    "RABBITMQ_PASSWORD": "segredo",
}


def without(env: dict, key: str) -> dict:
    return {k: v for k, v in env.items() if k != key}


def test_without_rabbitmq_host_the_url_comes_from_rabbitmq_url():
    assert main.rabbitmq_url_from_env({"RABBITMQ_URL": "amqp://a:b@rabbitmq:5672/"}) == "amqp://a:b@rabbitmq:5672/"


def test_without_any_variable_the_default_is_the_local_rabbitmq():
    assert main.rabbitmq_url_from_env({}) == "amqp://ticketmei:ticketmei@localhost:5672/"


def test_with_rabbitmq_host_the_url_is_built_from_the_parts():
    url = main.rabbitmq_url_from_env(GLOBAL)

    assert url == "amqp://grupo07:segredo@rabbitmq.rabbitmq-system.svc.cluster.local:5672/%2Fgrupo07"


def test_rabbitmq_host_takes_precedence_over_rabbitmq_url():
    url = main.rabbitmq_url_from_env({**GLOBAL, "RABBITMQ_URL": "amqp://x:y@outro:5672/"})

    assert url.startswith("amqp://grupo07:segredo@rabbitmq.rabbitmq-system.svc.cluster.local:5672/")


def test_default_vhost_is_the_root_vhost():
    assert main.rabbitmq_url_from_env(without(GLOBAL, "RABBITMQ_VHOST")).endswith(":5672/%2F")


def test_the_port_can_be_changed():
    assert ":5673/" in main.rabbitmq_url_from_env({**GLOBAL, "RABBITMQ_PORT": "5673"})


def test_special_characters_in_the_credentials_are_encoded():
    url = main.rabbitmq_url_from_env({**GLOBAL, "RABBITMQ_PASSWORD": "a@b:c/d%"})

    assert "grupo07:a%40b%3Ac%2Fd%25@" in url


@pytest.mark.parametrize("missing", ["RABBITMQ_USER", "RABBITMQ_PASSWORD"])
def test_rabbitmq_host_without_credentials_is_refused(missing):
    with pytest.raises(ValueError):
        main.rabbitmq_url_from_env(without(GLOBAL, missing))


def test_the_amqp_client_reads_the_vhost_and_the_credentials_back():
    # O aio-pika usa o aiormq, que lê o vhost do caminho do URL: "/%2Fgrupo07" tem de dar "/grupo07"
    url = main.rabbitmq_url_from_env({**GLOBAL, "RABBITMQ_PASSWORD": "a@b:c/d%"})

    async def parse():
        return aiormq.Connection(url)  # só lê o URL; não liga a nada

    connection = asyncio.run(parse())

    assert connection.vhost == "/grupo07"
    assert connection.url.user == "grupo07"
    assert connection.url.password == "a@b:c/d%"
    assert connection.url.host == "rabbitmq.rabbitmq-system.svc.cluster.local"
    assert connection.url.port == 5672
