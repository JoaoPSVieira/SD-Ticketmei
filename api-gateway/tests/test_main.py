"""Testes do API Gateway. Correr com: python -m pytest (dentro da pasta api-gateway/)."""

import json
from urllib.parse import unquote

from prometheus_client import REGISTRY

from conftest import DOWN_SERVICE_TIMEOUT, EVENTS, ORDER, SLOW_SECONDS, FakeCatalogHandler, FakeServicesHandler


def requests_count(method: str, endpoint: str, http_status: str) -> float:
    value = REGISTRY.get_sample_value(
        "gateway_requests_total",
        {"method": method, "endpoint": endpoint, "http_status": http_status},
    )
    return value or 0.0


def duration_count(endpoint: str) -> float:
    value = REGISTRY.get_sample_value("gateway_request_duration_seconds_count", {"endpoint": endpoint})
    return value or 0.0


# --- Encaminhamento ----------------------------------------------------------

def test_list_events_is_forwarded_to_catalog(client):
    response = client.get("/events")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert response.json() == EVENTS


def test_event_details_are_forwarded_to_catalog(client):
    response = client.get("/events/evt-101")

    assert response.status_code == 200
    assert response.json() == EVENTS[0]


def test_catalog_404_is_passed_through(client):
    response = client.get("/events/evt-999")

    assert response.status_code == 404
    assert "evt-999" in response.json()["detail"]


def test_event_id_is_url_encoded_before_forwarding(client):
    # O id "evt?x=1" não pode transformar-se numa query string no pedido ao Catalog
    FakeCatalogHandler.received_paths.clear()

    client.get("/events/evt%3Fx%3D1")

    [path] = FakeCatalogHandler.received_paths
    assert "?" not in path
    assert unquote(path) == "/events/evt?x=1"


# --- Falhas do Catalog -------------------------------------------------------

def test_catalog_server_error_becomes_502(client):
    response = client.get("/events/boom")

    assert response.status_code == 502
    assert response.json() == {"detail": "O serviço Catalog devolveu um erro"}


def test_catalog_down_becomes_502(make_client, unused_url):
    with make_client(unused_url, timeout_seconds=DOWN_SERVICE_TIMEOUT) as client:
        response = client.get("/events")

    assert response.status_code == 502
    assert response.json() == {"detail": "O serviço Catalog está indisponível"}


def test_catalog_timeout_becomes_504(make_client, fake_catalog_url):
    with make_client(fake_catalog_url, timeout_seconds=SLOW_SECONDS / 5) as client:
        response = client.get("/events/slow")

    assert response.status_code == 504
    assert response.json() == {"detail": "O serviço Catalog não respondeu a tempo"}


def test_health_does_not_depend_on_catalog(make_client, unused_url):
    with make_client(unused_url) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# --- Métricas ----------------------------------------------------------------

def test_metrics_endpoint_exposes_gateway_metrics(client):
    client.get("/events")
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "gateway_requests_total" in response.text
    assert "gateway_request_duration_seconds_bucket" in response.text


def test_requests_are_counted_with_method_endpoint_and_status(client):
    before_list = requests_count("GET", "/events", "200")
    before_detail = requests_count("GET", "/events/{event_id}", "200")
    before_404 = requests_count("GET", "/events/{event_id}", "404")
    before_502 = requests_count("GET", "/events/{event_id}", "502")
    before_duration = duration_count("/events/{event_id}")

    client.get("/events")
    client.get("/events/evt-101")
    client.get("/events/nao-existe")
    client.get("/events/boom")

    assert requests_count("GET", "/events", "200") == before_list + 1
    assert requests_count("GET", "/events/{event_id}", "200") == before_detail + 1
    assert requests_count("GET", "/events/{event_id}", "404") == before_404 + 1
    assert requests_count("GET", "/events/{event_id}", "502") == before_502 + 1
    assert duration_count("/events/{event_id}") == before_duration + 3


def test_catalog_failures_are_counted_as_5xx(make_client, unused_url):
    # O painel "Error Rate" do dashboard conta os pedidos com http_status=~"5.."
    before = requests_count("GET", "/events", "502")

    with make_client(unused_url, timeout_seconds=DOWN_SERVICE_TIMEOUT) as client:
        client.get("/events")

    assert requests_count("GET", "/events", "502") == before + 1


def test_endpoint_label_uses_route_template_not_raw_path(client):
    client.get("/events/evt-101")

    assert REGISTRY.get_sample_value(
        "gateway_requests_total", {"method": "GET", "endpoint": "/events/evt-101", "http_status": "200"}
    ) is None


def test_unknown_paths_are_grouped_as_unmatched(client):
    before = requests_count("GET", "unmatched", "404")

    client.get("/caminho/que/nao/existe")

    assert requests_count("GET", "unmatched", "404") == before + 1


def test_metrics_and_health_are_not_counted(client):
    before_unmatched = duration_count("unmatched")

    client.get("/metrics")
    client.get("/health")

    assert duration_count("/metrics") == 0
    assert duration_count("/health") == 0
    assert duration_count("unmatched") == before_unmatched
    assert requests_count("GET", "/metrics", "200") == 0
    assert requests_count("GET", "/health", "200") == 0


# --- Compras (fase 2): encaminhamento para o Order ---------------------------

NEW_ORDER = {"customer_id": "cliente-1", "customer_email": "ana@exemplo.pt", "event_id": "evt-101", "quantity": 2}


def test_create_order_is_forwarded_to_order(client):
    FakeServicesHandler.received_posts.clear()

    response = client.post("/orders", json=NEW_ORDER)

    assert response.status_code == 202
    assert response.json() == ORDER
    assert response.headers["location"] == "/orders/ord-1"
    # O Order recebe exatamente o corpo enviado pelo cliente
    [(path, content_type, body)] = FakeServicesHandler.received_posts
    assert path == "/orders"
    assert content_type == "application/json"
    assert json.loads(body) == NEW_ORDER


def test_order_details_are_forwarded_to_order(client):
    response = client.get("/orders/ord-1")

    assert response.status_code == 200
    assert response.json() == ORDER


def test_unknown_order_404_is_passed_through(client):
    response = client.get("/orders/nao-existe")

    assert response.status_code == 404
    assert "nao-existe" in response.json()["detail"]


def test_invalid_order_422_is_passed_through(client):
    response = client.post("/orders", json={**NEW_ORDER, "event_id": "invalid"})

    assert response.status_code == 422
    assert response.json() == {"detail": [{"msg": "quantity must be <= 4"}]}


def test_order_503_is_passed_through_with_the_failed_order(client):
    # O 503 é uma resposta pensada pelo Order (RabbitMQ em baixo): o cliente recebe a compra FAILED
    response = client.post("/orders", json={**NEW_ORDER, "event_id": "unavailable"})

    assert response.status_code == 503
    assert response.json()["order"]["status"] == "FAILED"


def test_unexpected_order_error_becomes_502(client):
    response = client.post("/orders", json={**NEW_ORDER, "event_id": "boom"})

    assert response.status_code == 502
    assert response.json() == {"detail": "O serviço Order devolveu um erro"}


def test_order_down_becomes_502(make_client, fake_catalog_url, unused_url):
    with make_client(fake_catalog_url, order_url=unused_url, order_timeout_seconds=DOWN_SERVICE_TIMEOUT) as client:
        response = client.post("/orders", json=NEW_ORDER)
        events = client.get("/events")

    assert response.status_code == 502
    assert response.json() == {"detail": "O serviço Order está indisponível"}
    assert events.status_code == 200  # o Catalog continua a funcionar


def test_order_timeout_becomes_504(make_client, fake_catalog_url):
    with make_client(fake_catalog_url, order_timeout_seconds=SLOW_SECONDS / 5) as client:
        response = client.post("/orders", json={**NEW_ORDER, "event_id": "slow"})

    assert response.status_code == 504
    assert response.json() == {"detail": "O serviço Order não respondeu a tempo"}


def test_order_id_is_url_encoded_before_forwarding(client):
    FakeServicesHandler.received_paths.clear()

    client.get("/orders/ord%3Fx%3D1")

    [path] = FakeServicesHandler.received_paths
    assert "?" not in path
    assert unquote(path) == "/orders/ord?x=1"


def test_order_requests_are_counted_by_method_and_route(client):
    before_post = requests_count("POST", "/orders", "202")
    before_get = requests_count("GET", "/orders/{order_id}", "200")

    client.post("/orders", json=NEW_ORDER)
    client.get("/orders/ord-1")

    assert requests_count("POST", "/orders", "202") == before_post + 1
    assert requests_count("GET", "/orders/{order_id}", "200") == before_get + 1


def test_docs_show_the_order_request_example(client):
    spec = client.get("/openapi.json").json()

    body = spec["paths"]["/orders"]["post"]["requestBody"]["content"]["application/json"]
    assert body["example"]["event_id"] == "evt-101"
