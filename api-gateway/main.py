"""
TicketMEI - serviço API Gateway.

Ponto de entrada único de todo o tráfego dos clientes. Encaminha cada pedido para o serviço
responsável, de forma síncrona (o cliente espera pela resposta do serviço):

    GET  /events           -> Catalog  {CATALOG_URL}/events
    GET  /events/{id}      -> Catalog  {CATALOG_URL}/events/{id}
    POST /orders           -> Order    {ORDER_URL}/orders          (fase 2; limite de pedidos por IP na fase 3)
    GET  /orders/{id}      -> Order    {ORDER_URL}/orders/{id}     (fase 2)
    GET  /health           -> estado do próprio Gateway (healthcheck do Docker; probes do Kubernetes na fase 3)
    GET  /metrics          -> métricas no formato Prometheus

Métricas:
    gateway_requests_total            Counter    labels: method, endpoint, http_status
    gateway_request_duration_seconds  Histogram  labels: endpoint

Tratamento de falhas dos serviços:
    O serviço responde 2xx/4xx            -> a mesma resposta (incluindo 202, 404, 422)
    O serviço responde 502, 503 ou 504    -> a mesma resposta (é o serviço a explicar o que falhou;
                                             p.ex. o Order responde 503 com a compra FAILED)
    O serviço responde outro 5xx (ex.: 500, um erro inesperado) -> 502 Bad Gateway
    O serviço está inacessível            -> 502 Bad Gateway
    O serviço não responde a tempo        -> 504 Gateway Timeout

Rate limiter (fase 3):
    POST /orders tem um limite de pedidos por cliente (IP), guardado no Redis (ver rate_limiter.py).
    Acima do limite, o Gateway responde 429 Too Many Requests sem chamar o Order.
    Só fica ativo se REDIS_URL estiver definida (no Kubernetes está; no Docker Compose não).

Vários processos:
    No contentor, o uvicorn arranca vários processos (WEB_CONCURRENCY) para usar vários cores.
    As métricas de todos os processos são guardadas em PROMETHEUS_MULTIPROC_DIR e somadas no /metrics.
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
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
    multiprocess,
)

from rate_limiter import Decision, RateLimiter

# ---------------------------------------------------------------------------
# Configuração (por variáveis de ambiente, para não mudar código entre ambientes)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [api-gateway] %(message)s",
)
logger = logging.getLogger("api-gateway")

# Endereços dos serviços. No Docker Compose são http://catalog:8001 e http://order:8002 (nomes dos
# serviços na rede interna). Os valores por omissão servem para correr o Gateway diretamente no PC.
CATALOG_URL = os.getenv("CATALOG_URL", "http://localhost:8001").rstrip("/")
ORDER_URL = os.getenv("ORDER_URL", "http://localhost:8002").rstrip("/")

# Tempo máximo à espera de cada serviço. Sem timeout, um serviço bloqueado prenderia o Gateway.
# O do Order é maior porque um POST /orders inclui, lá dentro, a chamada ao Catalog e ao RabbitMQ
# (até 2 s cada): assim, se o Order demorar por causa deles, é o próprio Order a responder com o erro certo.
CATALOG_TIMEOUT_SECONDS = float(os.getenv("CATALOG_TIMEOUT_SECONDS", "2"))
ORDER_TIMEOUT_SECONDS = float(os.getenv("ORDER_TIMEOUT_SECONDS", "5"))

# Número máximo de ligações TCP abertas em simultâneo para cada serviço (reutilizadas entre pedidos).
UPSTREAM_MAX_CONNECTIONS = int(os.getenv("UPSTREAM_MAX_CONNECTIONS", "100"))

# Rate limiter do POST /orders (fase 3). Sem REDIS_URL fica desligado (é o caso do Docker Compose).
# RATE_LIMIT_PER_SECOND: pedidos por segundo que cada cliente pode fazer, em média
# RATE_LIMIT_BURST: pedidos seguidos que um cliente pode fazer de uma vez (capacidade do balde)
REDIS_URL = os.getenv("REDIS_URL", "")
RATE_LIMIT_PER_SECOND = float(os.getenv("RATE_LIMIT_PER_SECOND", "5"))
RATE_LIMIT_BURST = int(os.getenv("RATE_LIMIT_BURST", "5"))


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

# Com vários processos, cada um tem a sua própria memória: um contador em memória só contaria os
# pedidos atendidos por esse processo. Quando PROMETHEUS_MULTIPROC_DIR está definida, o
# prometheus_client guarda os valores de cada processo em ficheiros nessa pasta, e o /metrics
# soma-os todos antes de responder ao Prometheus.
MULTIPROCESS_MODE = bool(os.getenv("PROMETHEUS_MULTIPROC_DIR"))


def collect_metrics() -> bytes:
    if MULTIPROCESS_MODE:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return generate_latest(registry)
    return generate_latest()


class MetricsMiddleware:
    """
    Middleware ASGI que mede todos os pedidos HTTP (o mesmo desenho do Catalog).

    Para cada pedido regista o método, o template da rota (ex.: "/events/{event_id}",
    e não "/events/evt-101", para não criar uma série temporal por id), o código HTTP
    devolvido ao cliente e a duração total, incluindo a espera pelo serviço de destino.
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

    A sessão mantém um pool de ligações TCP abertas (keep-alive) para cada serviço. Criar uma
    ligação nova por pedido (handshake TCP a cada pedido) seria a principal causa de latência sob carga.
    """
    app.state.http = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=0, limit_per_host=UPSTREAM_MAX_CONNECTIONS),
    )
    logger.info(
        "A encaminhar pedidos para o Catalog em %s (timeout %.1fs) e para o Order em %s (timeout %.1fs)",
        CATALOG_URL, CATALOG_TIMEOUT_SECONDS, ORDER_URL, ORDER_TIMEOUT_SECONDS,
    )

    # Rate limiter (fase 3): um cliente Redis por processo, todos a partilhar os mesmos baldes
    app.state.rate_limiter = None
    if REDIS_URL:
        app.state.rate_limiter = RateLimiter(REDIS_URL, RATE_LIMIT_PER_SECOND, RATE_LIMIT_BURST)
        logger.info("Rate limiter ativo em POST /orders: %g pedidos/s por cliente, rajadas até %d (Redis em %s)",
                    RATE_LIMIT_PER_SECOND, RATE_LIMIT_BURST, REDIS_URL)
    else:
        logger.info("Rate limiter desligado (REDIS_URL não definida)")

    yield
    await app.state.http.close()
    if app.state.rate_limiter is not None:
        await app.state.rate_limiter.close()


# Respostas 5xx que o serviço devolve de propósito, a explicar o que falhou: passam tal como vêm
DELIBERATE_5XX = {502, 503, 504}

# Cabeçalhos da resposta do serviço que são passados ao cliente
FORWARDED_HEADERS = ("Location",)


async def forward(request: Request, service: str, url: str, timeout_seconds: float) -> Response:
    """
    Reencaminha o pedido do cliente para um serviço interno e devolve a resposta (pedido síncrono).

    O corpo do pedido e o da resposta passam tal como estão (bytes), sem serem convertidos de/para
    JSON: o Gateway não precisa de perceber o conteúdo, só de o entregar.
    """
    session: aiohttp.ClientSession = request.app.state.http
    method = request.method
    body = await request.body() if method in ("POST", "PUT", "PATCH") else None
    headers = {"Content-Type": request.headers.get("content-type", "application/json")} if body is not None else None

    try:
        async with session.request(
            method, url, data=body, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_seconds)
        ) as upstream:
            content = await upstream.read()

            if upstream.status >= 500 and upstream.status not in DELIBERATE_5XX:
                logger.warning("%s respondeu %d a %s %s", service, upstream.status, method, url)
                return error_response(502, f"O serviço {service} devolveu um erro")

            response = Response(
                content=content,
                status_code=upstream.status,
                media_type=upstream.headers.get("Content-Type", "application/json"),
            )
            for name in FORWARDED_HEADERS:
                if name in upstream.headers:
                    response.headers[name] = upstream.headers[name]
            return response

    except asyncio.TimeoutError:
        logger.warning("Timeout (%.1fs) em %s %s", timeout_seconds, method, url)
        return error_response(504, f"O serviço {service} não respondeu a tempo")

    except aiohttp.ClientError as exc:
        logger.warning("Falha a contactar o %s em %s %s: %s", service, method, url, exc)
        return error_response(502, f"O serviço {service} está indisponível")


def error_response(status_code: int, detail: str) -> JSONResponse:
    # Mesmo formato de erro que os serviços usam: {"detail": "..."}
    return JSONResponse(status_code=status_code, content={"detail": detail})


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

app = FastAPI(
    title="TicketMEI - API Gateway",
    description="Ponto de entrada único da plataforma TicketMEI.",
    version="3.0.0",
    lifespan=lifespan,
)
app.add_middleware(MetricsMiddleware)


def upstream_errors(service: str) -> dict:
    return {
        502: {"description": f"{service} indisponível ou com erro"},
        504: {"description": f"{service} não respondeu a tempo"},
    }


# --- Catalog ----------------------------------------------------------------

@app.get("/events", responses=upstream_errors("Catalog"))
async def list_events(request: Request) -> Response:
    """Lista todos os eventos (encaminhado para o Catalog)."""
    return await forward(request, "Catalog", f"{CATALOG_URL}/events", CATALOG_TIMEOUT_SECONDS)


@app.get("/events/{event_id}", responses={404: {"description": "Evento não encontrado"}, **upstream_errors("Catalog")})
async def get_event(request: Request, event_id: str) -> Response:
    """Detalhes de um evento (encaminhado para o Catalog)."""
    # quote() codifica caracteres especiais do id, para que nunca alterem o caminho do pedido interno
    return await forward(request, "Catalog", f"{CATALOG_URL}/events/{quote(event_id, safe='')}", CATALOG_TIMEOUT_SECONDS)


# --- Order (fase 2) -----------------------------------------------------------

# O corpo do POST /orders não é validado pelo Gateway (passa tal como vem e é o Order que o valida).
# Esta descrição serve só para a documentação em /docs, com um exemplo pronto a usar no "Try it out".
ORDER_REQUEST_DOC = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["customer_id", "customer_email", "event_id", "quantity"],
                    "properties": {
                        "customer_id": {"type": "string"},
                        "customer_email": {"type": "string"},
                        "event_id": {"type": "string"},
                        "quantity": {"type": "integer", "minimum": 1, "maximum": 4},
                    },
                },
                "example": {"customer_id": "cliente-1", "customer_email": "ana@exemplo.pt",
                            "event_id": "evt-101", "quantity": 2},
            }
        },
    }
}


@app.post(
    "/orders",
    status_code=202,
    openapi_extra=ORDER_REQUEST_DOC,
    responses={
        202: {"description": "Compra aceite (PENDING); a confirmação por email é enviada depois"},
        422: {"description": "Pedido inválido ou evento inexistente"},
        503: {"description": "Sistema de mensagens indisponível: a compra ficou FAILED"},
        429: {"description": "Demasiados pedidos deste cliente (rate limiter); ver o cabeçalho Retry-After"},
        **upstream_errors("Order"),
    },
)
async def create_order(request: Request) -> Response:
    """Cria uma compra de bilhetes (encaminhado para o Order), com limite de pedidos por cliente."""
    decision = await check_rate_limit(request)

    if decision is not None and not decision.allowed:
        # Recusado aqui, sem chegar ao Order: é isto que protege o resto do sistema
        response = error_response(429, "Demasiados pedidos. Tente novamente daqui a alguns segundos.")
    else:
        response = await forward(request, "Order", f"{ORDER_URL}/orders", ORDER_TIMEOUT_SECONDS)

    if decision is not None:
        response.headers.update(decision.headers())
    return response


async def check_rate_limit(request: Request) -> Decision | None:
    """
    Verifica o limite do cliente que fez o pedido. Devolve None quando não há verificação:
    rate limiter desligado (sem REDIS_URL) ou Redis indisponível (o pedido passa).

    O cliente é identificado pelo IP de onde vem a ligação. Não se usa o cabeçalho X-Forwarded-For
    porque qualquer cliente o pode inventar para fugir ao limite.
    """
    limiter: RateLimiter | None = request.app.state.rate_limiter
    if limiter is None or request.client is None:
        return None
    return await limiter.check(request.client.host)


@app.get("/orders/{order_id}", responses={404: {"description": "Compra não encontrada"}, **upstream_errors("Order")})
async def get_order(request: Request, order_id: str) -> Response:
    """Detalhes de uma compra (encaminhado para o Order)."""
    return await forward(request, "Order", f"{ORDER_URL}/orders/{quote(order_id, safe='')}", ORDER_TIMEOUT_SECONDS)


# --- Infraestrutura -----------------------------------------------------------

@app.get("/health")
async def health() -> dict:
    """
    Indica se o próprio Gateway está a funcionar.

    Não consulta os outros serviços de propósito: se o Catalog ou o Order falharem, o Gateway
    continua vivo e a responder 502, em vez de ser reiniciado pelo Docker/Kubernetes sem necessidade.
    """
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Expõe as métricas para o Prometheus (somadas de todos os processos, se houver vários)."""
    return Response(content=collect_metrics(), media_type=CONTENT_TYPE_LATEST)
