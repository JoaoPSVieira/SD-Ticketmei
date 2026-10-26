"""
Infraestrutura de testes do Order.

- Catalog: um Catalog "falso" (servidor HTTP da biblioteca padrão) numa porta livre, com respostas
  controladas, incluindo erros difíceis de provocar no serviço real (500, lentidão, em baixo).
- Base de dados: cada teste usa um ficheiro SQLite novo numa pasta temporária.
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
def make_client(monkeypatch, tmp_path, fake_catalog_url, published):
    """
    Cria um TestClient do Order.

    rabbitmq="ok":   a publicação funciona (as compras vão para a lista `published`)
    rabbitmq="down": sem ligação ao RabbitMQ (a função de publicação verdadeira falha)
    """

    @contextmanager
    def _make(catalog_url: str | None = None, rabbitmq: str = "ok", catalog_timeout: float = 2.0,
              db_path=None):
        monkeypatch.setattr(main, "CATALOG_URL", catalog_url or fake_catalog_url)
        monkeypatch.setattr(main, "CATALOG_TIMEOUT_SECONDS", catalog_timeout)
        monkeypatch.setattr(main, "ORDERS_DB_PATH", db_path or tmp_path / "orders.db")

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
