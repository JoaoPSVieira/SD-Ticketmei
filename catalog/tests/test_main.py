"""Testes do serviço Catalog. Correr com: python -m pytest (dentro da pasta catalog/)."""

import json

import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

import main

client = TestClient(main.app)


def requests_count(endpoint: str, http_status: str) -> float:
    """Valor atual de catalog_requests_total para um par de labels (0 se ainda não existir)."""
    value = REGISTRY.get_sample_value(
        "catalog_requests_total", {"endpoint": endpoint, "http_status": http_status}
    )
    return value or 0.0


def duration_count() -> float:
    return REGISTRY.get_sample_value("catalog_request_duration_seconds_count") or 0.0


# --- Endpoints ---------------------------------------------------------------

def test_list_events_returns_all_events():
    response = client.get("/events")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    events = response.json()
    assert [e["id"] for e in events] == ["evt-101", "evt-102", "evt-103", "evt-104"]


def test_list_events_matches_data_file():
    with main.EVENTS_FILE.open(encoding="utf-8") as f:
        expected = json.load(f)

    assert client.get("/events").json() == expected


def test_get_event_returns_details():
    response = client.get("/events/evt-102")

    assert response.status_code == 200
    event = response.json()
    assert event["id"] == "evt-102"
    assert event["price_eur"] == 120.0


def test_get_unknown_event_returns_404():
    response = client.get("/events/evt-999")

    assert response.status_code == 404
    assert "evt-999" in response.json()["detail"]


def test_health():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "events_loaded": 4}


# --- Métricas ----------------------------------------------------------------

def test_metrics_endpoint_exposes_catalog_metrics():
    client.get("/events")
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "catalog_requests_total" in response.text
    assert "catalog_request_duration_seconds_bucket" in response.text


def test_requests_are_counted_by_endpoint_and_status():
    before_ok = requests_count("/events", "200")
    before_detail = requests_count("/events/{event_id}", "200")
    before_404 = requests_count("/events/{event_id}", "404")
    before_duration = duration_count()

    client.get("/events")
    client.get("/events/evt-101")
    client.get("/events/nao-existe")

    assert requests_count("/events", "200") == before_ok + 1
    assert requests_count("/events/{event_id}", "200") == before_detail + 1
    assert requests_count("/events/{event_id}", "404") == before_404 + 1
    assert duration_count() == before_duration + 3


def test_endpoint_label_uses_route_template_not_raw_path():
    client.get("/events/evt-103")

    assert REGISTRY.get_sample_value(
        "catalog_requests_total", {"endpoint": "/events/evt-103", "http_status": "200"}
    ) is None


def test_unknown_paths_are_grouped_as_unmatched():
    before = requests_count("unmatched", "404")

    client.get("/caminho/que/nao/existe")

    assert requests_count("unmatched", "404") == before + 1


def test_metrics_and_health_are_not_counted():
    before = duration_count()

    client.get("/metrics")
    client.get("/health")

    assert duration_count() == before
    assert requests_count("/metrics", "200") == 0
    assert requests_count("/health", "200") == 0


# --- Carregamento dos dados --------------------------------------------------

def test_load_events_rejects_missing_fields(tmp_path):
    bad = tmp_path / "events.json"
    bad.write_text(json.dumps([{"id": "x", "title": "Sem local"}]), encoding="utf-8")

    with pytest.raises(Exception):
        main.load_events(bad)


def test_load_events_rejects_duplicate_ids(tmp_path):
    event = {
        "id": "dup",
        "title": "Evento",
        "venue": "Funchal",
        "date": "2027-01-01T20:00:00Z",
        "price_eur": 10.0,
        "total_tickets": 100,
    }
    bad = tmp_path / "events.json"
    bad.write_text(json.dumps([event, event]), encoding="utf-8")

    with pytest.raises(ValueError):
        main.load_events(bad)
