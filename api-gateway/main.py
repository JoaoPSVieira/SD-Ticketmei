"""
TicketMEI - serviço API Gateway.

Ponto de entrada único de todo o tráfego dos clientes. Nesta fase encaminha os pedidos
de eventos para o serviço Catalog, de forma síncrona (o cliente espera pela resposta do Catalog).

Endpoints:
    GET /events          -> encaminha para {CATALOG_URL}/events
    GET /events/{id}     -> encaminha para {CATALOG_URL}/events/{id}
    GET /health          -> estado do próprio Gateway (healthcheck do Docker; probes do Kubernetes na fase 3)
    GET /metrics         -> métricas no formato Prometheus

Métricas:
    gateway_requests_total            Counter    labels: method, endpoint, http_status
    gateway_request_duration_seconds  Histogram  labels: endpoint

Tratamento de falhas do Catalog:
    Catalog responde 2xx/4xx      -> devolve a mesma resposta ao cliente
    Catalog responde 5xx          -> 502 Bad Gateway
    Catalog inacessível           -> 502 Bad Gateway
    Catalog não responde a tempo  -> 504 Gateway Timeout
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from urllib.parse import quote

import aiohttp
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

# ---------------------------------------------------------------------------
# Configuração (por variáveis de ambiente, para não mudar código entre ambientes)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [api-gateway] %(message)s",
)
logger = logging.getLogger("api-gateway")

# Endereço do Catalog. No Docker Compose é http://catalog:8001 (nome do serviço na rede interna).
# O valor por omissão serve para correr o Gateway diretamente no PC, com o Catalog em localhost.
CATALOG_URL = os.getenv("CATALOG_URL", "http://localhost:8001").rstrip("/")

# Tempo máximo à espera do Catalog. Sem timeout, um Catalog bloqueado prenderia o Gateway indefinidamente.
CATALOG_TIMEOUT_SECONDS = float(os.getenv("CATALOG_TIMEOUT_SECONDS", "2"))

# Número máximo de ligações TCP abertas em simultâneo para o Catalog (reutilizadas entre pedidos).
CATALOG_MAX_CONNECTIONS = int(os.getenv("CATALOG_MAX_CONNECTIONS", "100"))


# ---------------------------------------------------------------------------
# Métricas Prometheus
# ---------------------------------------------------------------------------

REQUESTS_TOTAL = Counter(
    "gateway_requests_total",
    "Total de pedidos HTTP recebidos pelo API Gateway",
    ["method", "endpoint", "http_status"],
)

REQUEST_DURATION = Histogram(
    "gateway_request_duration_seconds",
    "Duração dos pedidos HTTP no API Gateway (inclui a chamada ao serviço de destino), em segundos",
    ["endpoint"],
    # Buckets concentrados à volta dos 200 ms (o threshold do teste k6), para que o p95/p99
    # calculado pelo Grafana seja preciso exatamente onde interessa. O 2.5 apanha os timeouts (2 s).
    buckets=(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25, 0.3, 0.5, 1.0, 2.5, 5.0),
)

# Pedidos de infraestrutura que não contam como tráfego de clientes
UNINSTRUMENTED_PATHS = {"/metrics", "/health"}


class MetricsMiddleware:
    """
    Middleware ASGI que mede todos os pedidos HTTP (o mesmo desenho do Catalog).

    Para cada pedido regista o método, o template da rota (ex.: "/events/{event_id}",
    e não "/events/evt-101", para não criar uma série temporal por id), o código HTTP
    devolvido ao cliente e a duração total, incluindo a espera pelo Catalog.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] in UNINSTRUMENTED_PATHS:
            await self.app(scope, receive, send)
            return

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
            route = scope.get("route")
            endpoint = getattr(route, "path", None) or "unmatched"
            REQUESTS_TOTAL.labels(
                method=scope["method"], endpoint=endpoint, http_status=str(status_code)
            ).inc()
            REQUEST_DURATION.labels(endpoint=endpoint).observe(duration)


# ---------------------------------------------------------------------------
# Cliente HTTP para os serviços internos
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Cria UMA sessão HTTP no arranque e fecha-a no encerramento.

    A sessão mantém um pool de ligações TCP abertas (keep-alive) para o Catalog. Criar uma
    ligação nova por pedido (handshake TCP a cada pedido) seria a principal causa de latência sob carga.
    """
    app.state.http = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=CATALOG_TIMEOUT_SECONDS),
        connector=aiohttp.TCPConnector(limit=CATALOG_MAX_CONNECTIONS),
    )
    logger.info(
        "A encaminhar pedidos para o Catalog em %s (timeout %.1fs)", CATALOG_URL, CATALOG_TIMEOUT_SECONDS
    )
    yield
    await app.state.http.close()


async def forward_get(request: Request, url: str) -> Response:
    """
    Faz um GET síncrono a um serviço interno e devolve a resposta ao cliente.

    O corpo da resposta é reencaminhado tal como chega (bytes), sem ser convertido de/para JSON:
    o Gateway não precisa de perceber o conteúdo, só de o entregar.
    """
    session: aiohttp.ClientSession = request.app.state.http
    try:
        async with session.get(url) as upstream:
            body = await upstream.read()

            if upstream.status >= 500:
                logger.warning("Catalog respondeu %d a GET %s", upstream.status, url)
                return error_response(502, "O serviço Catalog devolveu um erro")

            return Response(
                content=body,
                status_code=upstream.status,
                media_type=upstream.headers.get("Content-Type", "application/json"),
            )

    except asyncio.TimeoutError:
        logger.warning("Timeout (%.1fs) em GET %s", CATALOG_TIMEOUT_SECONDS, url)
        return error_response(504, "O serviço Catalog não respondeu a tempo")

    except aiohttp.ClientError as exc:
        logger.warning("Falha a contactar o Catalog em GET %s: %s", url, exc)
        return error_response(502, "O serviço Catalog está indisponível")


def error_response(status_code: int, detail: str) -> JSONResponse:
    # Mesmo formato de erro que o FastAPI usa nos 404 do Catalog: {"detail": "..."}
    return JSONResponse(status_code=status_code, content={"detail": detail})


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

app = FastAPI(
    title="TicketMEI - API Gateway",
    description="Ponto de entrada único da plataforma TicketMEI.",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(MetricsMiddleware)

UPSTREAM_ERRORS = {
    502: {"description": "Catalog indisponível ou com erro"},
    504: {"description": "Catalog não respondeu a tempo"},
}


@app.get("/events", responses=UPSTREAM_ERRORS)
async def list_events(request: Request) -> Response:
    """Lista todos os eventos (encaminhado para o Catalog)."""
    return await forward_get(request, f"{CATALOG_URL}/events")


@app.get("/events/{event_id}", responses={404: {"description": "Evento não encontrado"}, **UPSTREAM_ERRORS})
async def get_event(request: Request, event_id: str) -> Response:
    """Detalhes de um evento (encaminhado para o Catalog)."""
    # quote() codifica caracteres especiais do id, para que nunca alterem o caminho do pedido ao Catalog
    return await forward_get(request, f"{CATALOG_URL}/events/{quote(event_id, safe='')}")


@app.get("/health")
async def health() -> dict:
    """
    Indica se o próprio Gateway está a funcionar.

    Não consulta o Catalog de propósito: se o Catalog falhar, o Gateway continua vivo e a
    devolver 502, em vez de ser reiniciado pelo Docker/Kubernetes sem necessidade.
    """
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Expõe as métricas para o Prometheus."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
