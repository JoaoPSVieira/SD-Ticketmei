"""
Infraestrutura de testes do API Gateway.

Os testes não dependem do serviço Catalog verdadeiro: arrancam um Catalog "falso" (um pequeno
servidor HTTP da biblioteca padrão do Python) numa porta livre, com respostas controladas,
incluindo casos de erro difíceis de provocar no serviço real (500, lentidão, serviço em baixo).
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

EVENTS = [
    {"id": "evt-101", "title": "Concerto A", "venue": "Funchal", "date": "2027-05-20T20:00:00Z",
     "price_eur": 85.0, "total_tickets": 60000},
    {"id": "evt-102", "title": "Concerto B", "venue": "Lisboa", "date": "2027-06-15T19:30:00Z",
     "price_eur": 120.0, "total_tickets": 50000},
]

SLOW_SECONDS = 1.0


class FakeCatalogHandler(BaseHTTPRequestHandler):
    """Simula o Catalog. Os caminhos especiais /events/boom e /events/slow provocam falhas."""

    received_paths: list[str] = []

    def do_GET(self):
        FakeCatalogHandler.received_paths.append(self.path)

        if self.path == "/events":
            self._send(200, EVENTS)
        elif self.path == "/events/evt-101":
            self._send(200, EVENTS[0])
        elif self.path == "/events/boom":
            self._send(500, {"detail": "Internal Server Error"})
        elif self.path == "/events/slow":
            time.sleep(SLOW_SECONDS)
            self._send(200, EVENTS[0])
        else:
            event_id = self.path.rsplit("/", 1)[-1]
            self._send(404, {"detail": f"Evento '{event_id}' não encontrado"})

    def _send(self, status: int, payload) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # silencia o log de cada pedido
        pass


@pytest.fixture(scope="session")
def fake_catalog_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeCatalogHandler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


@pytest.fixture
def unused_url():
    """URL de uma porta onde nada está à escuta (simula o Catalog em baixo)."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    return f"http://127.0.0.1:{port}"


@pytest.fixture
def make_client(monkeypatch):
    """
    Cria um TestClient do Gateway apontado para um Catalog à escolha.

    O "with TestClient(...)" executa o lifespan da aplicação, ou seja, cria a sessão HTTP
    tal como acontece no arranque real do serviço.
    """

    @contextmanager
    def _make(catalog_url: str, timeout_seconds: float = 2.0):
        monkeypatch.setattr(main, "CATALOG_URL", catalog_url)
        monkeypatch.setattr(main, "CATALOG_TIMEOUT_SECONDS", timeout_seconds)
        with TestClient(main.app) as client:
            yield client

    return _make


@pytest.fixture
def client(make_client, fake_catalog_url):
    with make_client(fake_catalog_url) as c:
        yield c
