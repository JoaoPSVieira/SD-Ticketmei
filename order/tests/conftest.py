"""
Infraestrutura de testes do Order.

- Catalog: um Catalog "falso" (servidor HTTP da biblioteca padrão) numa porta livre, com respostas
  controladas, incluindo erros difíceis de provocar no serviço real (500, lentidão, em baixo).
- Base de dados: cada teste usa um ficheiro SQLite novo numa pasta temporária.
- Payment: um Payment "falso", também numa porta livre, que responde conforme o modo escolhido em
  cada teste (aceita, falha com 500, demora mais do que o timeout, recusa com 422) e conta os pedidos.
- RabbitMQ: nos testes unitários não há RabbitMQ. A ligação em segundo plano é desligada e a
  publicação é substituída por uma função que guarda as mensagens numa lista (ou que falha).
  O teste com um RabbitMQ verdadeiro está em test_rabbitmq_integration.py.
"""

import json
import socket
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from fastapi.testclient import TestClient

import main

EVENTS = {
    "evt-101": {"id": "evt-101", "title": "Concerto A", "venue": "Funchal", "date": "2027-05-20T20:00:00Z",
                "price_eur": 85.0, "total_tickets": 60000},
    "evt-104": {"id": "evt-104", "title": "Conferência", "venue": "Lisboa", "date": "2027-11-11T09:00:00Z",
                "price_eur": 350.0, "total_tickets": 20000},
}

SLOW_SECONDS = 1.0

# Timeout usado nos testes de "serviço em baixo". Tem de ser bem maior do que o tempo que o sistema
# operativo demora a recusar a ligação: em Linux é imediato, mas no Windows a ligação a uma porta
# fechada só é recusada ao fim de ~2 s (o Windows volta a tentar). Com o timeout normal de 2 s, no
# Windows esses testes dariam 504 (timeout) em vez de 502 (serviço em baixo).
DOWN_SERVICE_TIMEOUT = 10.0


class FakeCatalogHandler(BaseHTTPRequestHandler):
    """Simula o Catalog. Os ids especiais "boom" e "slow" provocam falhas."""

    def do_GET(self):
        event_id = self.path.rsplit("/", 1)[-1]
        if event_id == "boom":
            self._send(500, {"detail": "Internal Server Error"})
        elif event_id == "slow":
            time.sleep(SLOW_SECONDS)
            self._send(200, EVENTS["evt-101"])
        elif event_id in EVENTS:
            self._send(200, EVENTS[event_id])
        else:
            self._send(404, {"detail": f"Evento '{event_id}' não encontrado"})

    def _send(self, status: int, payload) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture(scope="session")
def fake_catalog_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeCatalogHandler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


# Timeout do Payment nos testes, e quanto o Payment falso demora no modo "slow" (mais do que o timeout)
PAYMENT_TEST_TIMEOUT = 0.3
PAYMENT_SLOW_SECONDS = 0.6


class FakePaymentHandler(BaseHTTPRequestHandler):
    """
    Simula o Payment (POST /payments). O comportamento muda com FakePaymentHandler.mode:

    "ok"        201 com o pagamento (status SUCCESS)
    "fail"      500 (falha injetada)
    "fail_once" 500 no próximo pedido e depois passa a "ok"
    "slow"   demora PAYMENT_SLOW_SECONDS a responder (mais do que o timeout do Order)
    "reject" 422 (pedido inválido)
    """

    mode = "ok"
    requests: list[dict] = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        FakePaymentHandler.requests.append(body)
        mode = FakePaymentHandler.mode
        if mode == "fail_once":
            # Falha só este pedido; os seguintes são aceites
            FakePaymentHandler.mode = "ok"
            mode = "fail"
        if mode == "fail":
            self._send(500, {"error": "Payment failed", "status": "FAILED"})
        elif mode == "reject":
            self._send(422, {"detail": "pedido inválido"})
        else:
            if mode == "slow":
                time.sleep(PAYMENT_SLOW_SECONDS)
            self._send(201, {"id": len(FakePaymentHandler.requests), "order_id": body.get("order_id"),
                             "amount": body.get("amount"), "status": "SUCCESS",
                             "processed_at": "2026-11-22T10:00:00+00:00"})

    def _send(self, status: int, payload) -> None:
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except ConnectionError:
            # O Order desistiu (timeout) antes da resposta. No Linux é BrokenPipeError ou
            # ConnectionResetError; no Windows é ConnectionAbortedError (WinError 10053).
            pass

    def log_message(self, *args):
        pass


@pytest.fixture(scope="session")
def fake_payment_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakePaymentHandler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


@pytest.fixture
def payment():
    """O Payment falso, a começar cada teste no modo "ok" e sem pedidos."""
    FakePaymentHandler.mode = "ok"
    FakePaymentHandler.requests = []
    yield FakePaymentHandler
    FakePaymentHandler.mode = "ok"


def unused_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def unused_url():
    """URL de uma porta onde nada está à escuta (simula um serviço em baixo)."""
    return f"http://127.0.0.1:{unused_port()}"


@pytest.fixture
def published():
    """Lista onde a publicação simulada guarda as compras "enviadas" para o RabbitMQ."""
    return []


@pytest.fixture
def make_client(monkeypatch, tmp_path, fake_catalog_url, fake_payment_url, payment, published):
    """
    Cria um TestClient do Order.

    rabbitmq="ok":   a publicação funciona (as compras vão para a lista `published`)
    rabbitmq="down": sem ligação ao RabbitMQ (a função de publicação verdadeira falha)
    payment_url:     por omissão o Payment falso; "" desliga o passo do pagamento
    attempts, failure_threshold, open_seconds: configuração do retry e do circuit breaker
    """

    @contextmanager
    def _make(catalog_url: str | None = None, rabbitmq: str = "ok", catalog_timeout: float = 2.0,
              db_path=None, payment_url: str | None = None, attempts: int = 3, failure_threshold: int = 5,
              open_seconds: float = 30.0, half_open_max_calls: int = 1,
              payment_timeout: float = PAYMENT_TEST_TIMEOUT):
        monkeypatch.setattr(main, "CATALOG_URL", catalog_url or fake_catalog_url)
        monkeypatch.setattr(main, "CATALOG_TIMEOUT_SECONDS", catalog_timeout)
        monkeypatch.setattr(main, "ORDERS_DB_PATH", db_path or tmp_path / "orders.db")
        monkeypatch.setattr(main, "PAYMENT_URL", fake_payment_url if payment_url is None else payment_url)
        monkeypatch.setattr(main, "PAYMENT_TIMEOUT_SECONDS", payment_timeout)
        monkeypatch.setattr(main, "PAYMENT_RETRY_ATTEMPTS", attempts)
        monkeypatch.setattr(main, "PAYMENT_RETRY_BASE_SECONDS", 0.01)
        monkeypatch.setattr(main, "CB_FAILURE_THRESHOLD", failure_threshold)
        monkeypatch.setattr(main, "CB_OPEN_SECONDS", open_seconds)
        monkeypatch.setattr(main, "CB_HALF_OPEN_MAX_CALLS", half_open_max_calls)

        async def no_rabbitmq_connection(app):
            return None

        monkeypatch.setattr(main, "keep_rabbitmq_connected", no_rabbitmq_connection)

        if rabbitmq == "ok":
            async def fake_publish(app, order):
                published.append(order)

            monkeypatch.setattr(main, "publish_order_created", fake_publish)

        with TestClient(main.app) as client:
            yield client

    return _make


@pytest.fixture
def client(make_client):
    with make_client() as c:
        yield c


VALID_ORDER = {
    "customer_id": "cliente-1",
    "customer_email": "ana@exemplo.pt",
    "event_id": "evt-101",
    "quantity": 2,
}
