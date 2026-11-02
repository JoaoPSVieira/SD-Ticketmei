"""
Infraestrutura de testes do API Gateway.

Os testes não dependem dos serviços verdadeiros: arrancam um servidor "falso" (um pequeno servidor
HTTP da biblioteca padrão do Python) numa porta livre, que simula o Catalog (/events) e o Order
(/orders), com respostas controladas, incluindo casos de erro difíceis de provocar nos serviços
reais (500, 503, lentidão, serviço em baixo).
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

ORDER = {
    "id": "ord-1", "customer_id": "cliente-1", "customer_email": "ana@exemplo.pt", "event_id": "evt-101",
    "quantity": 2, "total_amount": 170.0, "status": "PENDING", "created_at": "2026-10-26T10:00:00+00:00",
}

SLOW_SECONDS = 1.0

# Timeout usado nos testes de "serviço em baixo". Tem de ser bem maior do que o tempo que o sistema
# operativo demora a recusar a ligação: em Linux é imediato, mas no Windows a ligação a uma porta
# fechada só é recusada ao fim de ~2 s (o Windows volta a tentar). Com o timeout normal de 2 s, no
# Windows esses testes dariam 504 (timeout) em vez de 502 (serviço em baixo).
DOWN_SERVICE_TIMEOUT = 10.0


class FakeServicesHandler(BaseHTTPRequestHandler):
    """
    Simula o Catalog e o Order.

    Catalog: /events/boom e /events/slow provocam falhas.
    Order:   o event_id do POST escolhe a resposta: "boom" (500), "unavailable" (503 com a compra FAILED),
             "invalid" (422), "slow" (demora) e qualquer outro (202).
    """

    received_paths: list[str] = []
    received_posts: list[tuple[str, str, bytes]] = []  # (caminho, Content-Type, corpo)

    def do_GET(self):
        FakeServicesHandler.received_paths.append(self.path)

        if self.path.startswith("/orders/"):
            if self.path == "/orders/ord-1":
                self._send(200, ORDER)
            else:
                order_id = self.path.rsplit("/", 1)[-1]
                self._send(404, {"detail": f"Compra '{order_id}' não encontrada"})
        elif self.path == "/events":
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

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        FakeServicesHandler.received_posts.append((self.path, self.headers.get("Content-Type"), body))
        event_id = json.loads(body).get("event_id") if body else None

        if event_id == "boom":
            self._send(500, {"detail": "Internal Server Error"})
        elif event_id == "unavailable":
            self._send(503, {"detail": "Sistema de mensagens indisponível", "order": {**ORDER, "status": "FAILED"}})
        elif event_id == "invalid":
            self._send(422, {"detail": [{"msg": "quantity must be <= 4"}]})
        elif event_id == "slow":
            time.sleep(SLOW_SECONDS)
            self._send(202, ORDER, headers={"Location": "/orders/ord-1"})
        else:
            self._send(202, ORDER, headers={"Location": "/orders/ord-1"})

    def _send(self, status: int, payload, headers: dict | None = None) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # silencia o log de cada pedido
        pass


# Nome antigo, mantido para os testes da fase 1
FakeCatalogHandler = FakeServicesHandler


@pytest.fixture(scope="session")
def fake_catalog_url():
    """Servidor falso que responde como Catalog e como Order (os caminhos não se sobrepõem)."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeServicesHandler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


@pytest.fixture
def fake_order_url(fake_catalog_url):
    return fake_catalog_url


@pytest.fixture
def unused_url():
    """URL de uma porta onde nada está à escuta (simula um serviço em baixo)."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    return f"http://127.0.0.1:{port}"


@pytest.fixture
def make_client(monkeypatch, fake_catalog_url):
    """
    Cria um TestClient do Gateway apontado para os serviços à escolha.

    O "with TestClient(...)" executa o lifespan da aplicação, ou seja, cria a sessão HTTP
    tal como acontece no arranque real do serviço.
    """

    @contextmanager
    def _make(catalog_url: str, timeout_seconds: float = 2.0, order_url: str | None = None,
              order_timeout_seconds: float = 2.0):
        monkeypatch.setattr(main, "CATALOG_URL", catalog_url)
        monkeypatch.setattr(main, "CATALOG_TIMEOUT_SECONDS", timeout_seconds)
        monkeypatch.setattr(main, "ORDER_URL", order_url or fake_catalog_url)
        monkeypatch.setattr(main, "ORDER_TIMEOUT_SECONDS", order_timeout_seconds)
        with TestClient(main.app) as client:
            yield client

    return _make


@pytest.fixture
def client(make_client, fake_catalog_url):
    with make_client(fake_catalog_url) as c:
        yield c
