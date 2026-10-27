"""Testes do serviço Notification. Correr com: python -m pytest (dentro da pasta notification/)."""

import asyncio
import time

from prometheus_client import REGISTRY

import main
from conftest import encode, order_created, read_log


def processed(status: str) -> float:
    return REGISTRY.get_sample_value("notifications_processed_total", {"status": status}) or 0.0


def durations_observed() -> float:
    return REGISTRY.get_sample_value("notification_processing_duration_seconds_count") or 0.0


def process(body: bytes) -> main.Notification:
    return asyncio.run(main.process_notification(body))


# --- Envio -------------------------------------------------------------------

def test_valid_message_is_sent(log_path):
    message = order_created()

    notification = process(encode(message))

    assert notification.status == "SENT"
    assert notification.order_id == message["order_id"]
    assert notification.email == "ana@exemplo.pt"
    assert notification.reason is None


def test_sending_takes_about_100_ms(log_path):
    start = time.perf_counter()
    process(encode(order_created()))
    elapsed = time.perf_counter() - start

    assert 0.1 <= elapsed < 0.5


def test_each_notification_is_written_to_the_delivery_log(log_path):
    first, second = order_created(), order_created(customer_email="rui@exemplo.pt")

    process(encode(first))
    process(encode(second))

    lines = read_log(log_path)
    assert [(line["order_id"], line["email"], line["status"]) for line in lines] == [
        (first["order_id"], "ana@exemplo.pt", "SENT"),
        (second["order_id"], "rui@exemplo.pt", "SENT"),
    ]
    assert all(line["timestamp"] for line in lines)


# --- Mensagens inválidas e falhas -------------------------------------------

def test_invalid_json_is_failed(log_path):
    notification = process(b"isto nao e json")

    assert notification.status == "FAILED"
    assert "mensagem inválida" in notification.reason


def test_message_that_is_not_an_object_is_failed(log_path):
    assert process(b"[1, 2, 3]").status == "FAILED"


def test_message_without_email_is_failed(log_path):
    message = order_created()
    del message["customer_email"]

    notification = process(encode(message))

    assert notification.status == "FAILED"
    assert notification.order_id == message["order_id"]


def test_invalid_email_is_failed(log_path):
    notification = process(encode(order_created(customer_email="sem-arroba")))

    assert notification.status == "FAILED"
    assert "email inválido" in notification.reason


def test_simulated_send_failure_is_failed(log_path, monkeypatch):
    monkeypatch.setattr(main, "NOTIFICATION_FAILURE_RATE", 1.0)

    notification = process(encode(order_created()))

    assert notification.status == "FAILED"
    assert notification.reason == "falha simulada no envio do email"


def test_failed_notifications_are_also_logged(log_path):
    process(encode(order_created(customer_email="sem-arroba")))

    [line] = read_log(log_path)
    assert line["status"] == "FAILED"
    assert line["reason"]


# --- Métricas ----------------------------------------------------------------

def test_sent_and_failed_are_counted(log_path):
    before_sent, before_failed = processed("SENT"), processed("FAILED")

    process(encode(order_created()))
    process(encode(order_created()))
    process(b"invalido")

    assert processed("SENT") == before_sent + 2
    assert processed("FAILED") == before_failed + 1


def test_processing_duration_is_observed(log_path):
    before = durations_observed()

    process(encode(order_created()))

    assert durations_observed() == before + 1


def test_metrics_endpoint_exposes_notification_metrics(client):
    text = client.get("/metrics").text

    assert 'notifications_processed_total{status="SENT"}' in text
    assert 'notifications_processed_total{status="FAILED"}' in text
    assert "notification_processing_duration_seconds_bucket" in text


def test_health_does_not_depend_on_rabbitmq(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "rabbitmq": "disconnected"}
