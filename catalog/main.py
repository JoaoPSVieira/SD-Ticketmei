"""
Serve os metadados estáticos dos eventos (concertos, locais, preços base)
É o único serviço com acesso aos dados dos eventos (data/events.json)

Endpoints:
    GET /events          -> array com todos os eventos
    GET /events/{id}     -> detalhes de um evento
    GET /health          -> estado do serviço (healthcheck do Docker)
    GET /metrics         -> métricas no formato Prometheus

Métricas:
    catalog_requests_total            Counter   labels: endpoint, http_status
    catalog_request_duration_seconds  Histogram sem labels
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, TypeAdapter

# ----------------
# - Configuração -
# ----------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [catalog] %(message)s",
)
logger = logging.getLogger("catalog")

# Caminho do ficheiro de dados
EVENTS_FILE = Path(os.getenv("EVENTS_FILE", Path(__file__).parent / "data" / "events.json"))


# -------------------
# - Modelo de dados -
# -------------------
class Event(BaseModel):
    """Estrutura de um evento. Usada para validar o ficheiro no arranque e para documentar a API em /docs"""
    id: str
    title: str
    venue: str
    date: datetime
    price_eur: float
    total_tickets: int


def load_events(path: Path) -> list[dict]:
    """
    Lê e valida o ficheiro de eventos

    Falha logo no arranque se o ficheiro não existir, não for JSON válido,
    tiver campos em falta ou ids repetidos
    """
    with path.open(encoding="utf-8") as f:
        raw = json.load(f)

    # Valida a estrutura de cada evento
    TypeAdapter(list[Event]).validate_python(raw)

    ids = [event["id"] for event in raw]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Existem ids de eventos repetidos em {path}")

    return raw


# Os dados são lidos uma única vez, no arranque, e ficam em memória
EVENTS: list[dict] = load_events(EVENTS_FILE)
EVENTS_BY_ID: dict[str, dict] = {event["id"]: event for event in EVENTS}

# As respostas são serializadas para JSON uma única vez. Como os dados são estáticos,
# cada pedido só tem de devolver bytes já prontos, o que é muito mais rápido sob carga
EVENTS_JSON: bytes = json.dumps(EVENTS, ensure_ascii=False).encode("utf-8")
EVENT_JSON_BY_ID: dict[str, bytes] = {
    event_id: json.dumps(event, ensure_ascii=False).encode("utf-8")
    for event_id, event in EVENTS_BY_ID.items()
}

logger.info("Carregados %d eventos de %s", len(EVENTS), EVENTS_FILE)


# -----------------------
# - Métricas Prometheus -
# -----------------------

REQUESTS_TOTAL = Counter(
    "catalog_requests_total",
    "Total de pedidos HTTP processados pelo Catalog",
    ["endpoint", "http_status"],
)

REQUEST_DURATION = Histogram(
    "catalog_request_duration_seconds",
    "Duração dos pedidos HTTP no Catalog, em segundos",
    # O Catalog responde a partir de memória, por isso os buckets concentram-se abaixo de 100 ms
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

# Pedidos de infraestrutura que não contam como tráfego de clientes
UNINSTRUMENTED_PATHS = {"/metrics", "/health"}


class MetricsMiddleware:
    """
    Middleware ASGI que mede todos os pedidos HTTP.

    É um middleware ASGI "puro" (e não @app.middleware("http")) porque este último
    acrescenta bastante overhead por pedido, o que se nota com 500 utilizadores virtuais.

    Para cada pedido regista:
      - o código HTTP devolvido (apanhado na mensagem http.response.start);
      - o template da rota (ex.: "/events/{event_id}" e não "/events/evt-101"),
        para que cada id não crie uma série temporal nova no Prometheus;
      - a duração total do pedido.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] in UNINSTRUMENTED_PATHS:
            await self.app(scope, receive, send)
            return

        # Se a aplicação lançar uma exceção antes de responder (erro 500)
        status_code = 500

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        start = time.perf_counter()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - start
            # O router preenche scope["route"] com a rota que fez match (None se nenhuma fez)
            route = scope.get("route")
            endpoint = getattr(route, "path", None) or "unmatched"
            REQUESTS_TOTAL.labels(endpoint=endpoint, http_status=str(status_code)).inc()
            REQUEST_DURATION.observe(duration)


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

app = FastAPI(
    title="TicketMEI - Catalog",
    description="Metadados dos eventos disponíveis na plataforma TicketMEI.",
    version="1.0.0",
)
app.add_middleware(MetricsMiddleware)

JSON = "application/json"


@app.get("/events", responses={200: {"model": list[Event]}})
async def list_events() -> Response:
    """Devolve todos os eventos disponíveis."""
    return Response(content=EVENTS_JSON, media_type=JSON)


@app.get("/events/{event_id}", responses={200: {"model": Event}, 404: {"description": "Evento não encontrado"}})
async def get_event(event_id: str) -> Response:
    """Devolve os detalhes de um evento."""
    body = EVENT_JSON_BY_ID.get(event_id)
    if body is None:
        raise HTTPException(status_code=404, detail=f"Evento '{event_id}' não encontrado")
    return Response(content=body, media_type=JSON)


@app.get("/health")
async def health() -> dict:
    """Indica se o serviço está pronto a responder."""
    return {"status": "ok", "events_loaded": len(EVENTS)}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Expõe as métricas para o Prometheus."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
