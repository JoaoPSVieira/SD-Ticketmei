"""
Infraestrutura de testes do Notification.

- Log de entregas: cada teste escreve num ficheiro novo numa pasta temporária.
- RabbitMQ: nos testes unitários não há RabbitMQ (a ligação em segundo plano é desligada e as
  mensagens são passadas diretamente a process_notification). Os testes com um RabbitMQ verdadeiro
  estão em test_rabbitmq_integration.py.
"""

import json
import uuid

import pytest
from fastapi.testclient import TestClient

import main


def order_created(**overrides) -> dict:
    """Uma mensagem OrderCreated tal como o Order a publica (ver CONTRATO-FASE2.md)."""
    message = {
        "type": "OrderCreated",
        "order_id": str(uuid.uuid4()),
        "customer_id": "cliente-1",
        "customer_email": "ana@exemplo.pt",
        "event_id": "evt-101",
        "quantity": 2,
        "total_amount": 170.0,
        "created_at": "2026-10-25T23:53:02+00:00",
    }
    message.update(overrides)
    return message


def encode(message) -> bytes:
    return json.dumps(message).encode("utf-8")


def read_log(path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


@pytest.fixture
def log_path(tmp_path, monkeypatch):
    """Prepara o log de entregas num ficheiro temporário (como faz o arranque do serviço)."""
    path = tmp_path / "notifications.log"
    monkeypatch.setattr(main, "NOTIFICATIONS_LOG_PATH", path)
    main.app.state.delivery_log = main.setup_delivery_log(path)
    yield path
    for handler in list(main.app.state.delivery_log.handlers):
        handler.close()


@pytest.fixture
def client(monkeypatch, log_path):
    """TestClient sem RabbitMQ (para testar /health e /metrics)."""

    async def no_rabbitmq(app):
        return None

    monkeypatch.setattr(main, "keep_consuming", no_rabbitmq)
    with TestClient(main.app) as c:
        yield c
