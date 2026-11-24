"""Testes do serviço Payment. Correr com: python -m pytest (dentro da pasta payment/)."""

from datetime import datetime

from conftest import payment_request


# --- Pagamentos ----------------------------------------------------------------

def test_successful_payment_returns_201_with_the_payment(client):
    request = payment_request(amount=170.0)

    response = client.post("/payments", json=request)

    assert response.status_code == 201
    payment = response.json()
    assert payment["order_id"] == request["order_id"]
    assert payment["amount"] == 170.0
    assert payment["status"] == "SUCCESS"
    assert isinstance(payment["id"], int)
    datetime.fromisoformat(payment["processed_at"])  # timestamp ISO 8601
    assert response.headers["location"] == f"/payments/{payment['id']}"


def test_payment_can_be_read_by_id(client):
    created = client.post("/payments", json=payment_request()).json()

    response = client.get(f"/payments/{created['id']}")

    assert response.status_code == 200
    assert response.json() == created


def test_unknown_payment_returns_404(client):
    response = client.get("/payments/999999")

    assert response.status_code == 404


def test_payments_are_kept_in_the_database_after_a_restart(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    import main

    monkeypatch.setattr(main, "PAYMENTS_DB_PATH", tmp_path / "payments.db")
    with TestClient(main.app) as first:
        created = first.post("/payments", json=payment_request()).json()
    with TestClient(main.app) as second:
        assert second.get(f"/payments/{created['id']}").json() == created


def test_order_id_is_required(client):
    request = payment_request()
    del request["order_id"]

    assert client.post("/payments", json=request).status_code == 422


def test_amount_must_be_positive(client):
    assert client.post("/payments", json=payment_request(amount=0)).status_code == 422
    assert client.post("/payments", json=payment_request(amount=-10)).status_code == 422


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
