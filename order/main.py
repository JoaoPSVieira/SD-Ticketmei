"""
TicketMEI - serviço Order

Recebe os pedidos de compra de bilhetes, regista cada compra numa base de dados própria (SQLite)
no estado PENDING e publica a mensagem "OrderCreated" no RabbitMQ, para o serviço Notification
enviar o email de confirmação. Responde ao cliente com 202 Accepted: a compra foi aceite, mas o
resto do processo (a notificação) acontece depois, de forma assíncrona.

Endpoints:
    POST /orders         -> cria uma compra (estado PENDING) e publica OrderCreated -> 202
    GET  /orders/{id}    -> detalhes de uma compra (404 se não existir)
    GET  /health         -> estado do serviço (não depende do RabbitMQ nem do Catalog)
    GET  /metrics        -> métricas no formato Prometheus

Fluxo de POST /orders:
    1. Valida o pedido (campos obrigatórios, email, 1 a 4 bilhetes)        -> 422 se inválido
    2. Pergunta ao Catalog o preço do evento (pedido síncrono)            -> 422 se o evento não existe,
                                                                             502/504 se o Catalog falhar
    3. Grava a compra como PENDING no SQLite
    4. Publica OrderCreated na fila do RabbitMQ (com confirmação)          -> 202 Accepted
       Se a publicação falhar, a compra passa a FAILED                     -> 503 Service Unavailable

Métricas:
    orders_initiated_total            Counter    labels: status (PENDING, FAILED)
    order_requests_total              Counter    labels: method, endpoint, http_status
    order_request_duration_seconds    Histogram  labels: endpoint

Vários processos:
    No contentor, o uvicorn arranca vários processos (WEB_CONCURRENCY). Todos usam o mesmo ficheiro
    SQLite (em modo WAL) e as métricas de todos são somadas no /metrics (PROMETHEUS_MULTIPROC_DIR).
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import aio_pika
import aiohttp
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
    multiprocess,
)
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Configuração (variáveis de ambiente)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [order] %(message)s",
)
logger = logging.getLogger("order")
# A biblioteca do RabbitMQ (aiormq) escreve tracebacks longos quando a ligação cai, mesmo quando volta a
# ligar sozinha. Mostramos só os erros dela; o estado da ligação aparece nas mensagens do aio_pika e do Order.
logging.getLogger("aiormq").setLevel(logging.ERROR)

# Serviço Catalog: de onde vem o preço de cada evento
CATALOG_URL = os.getenv("CATALOG_URL", "http://localhost:8001").rstrip("/")
CATALOG_TIMEOUT_SECONDS = float(os.getenv("CATALOG_TIMEOUT_SECONDS", "2"))

# RabbitMQ: para onde vão as mensagens OrderCreated
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://ticketmei:ticketmei@localhost:5672/")
ORDER_QUEUE = os.getenv("ORDER_QUEUE", "order_created")
PUBLISH_TIMEOUT_SECONDS = float(os.getenv("PUBLISH_TIMEOUT_SECONDS", "2"))
RABBITMQ_RETRY_SECONDS = float(os.getenv("RABBITMQ_RETRY_SECONDS", "2"))

# Base de dados do Order: um ficheiro SQLite (no contentor fica num volume do Docker)
ORDERS_DB_PATH = Path(os.getenv("ORDERS_DB_PATH", Path(__file__).parent / "data" / "orders.db"))

# RF2: limite de bilhetes por compra
MAX_TICKETS_PER_ORDER = int(os.getenv("MAX_TICKETS_PER_ORDER", "4"))


# ---------------------------------------------------------------------------
# Modelo de dados
# ---------------------------------------------------------------------------

class OrderRequest(BaseModel):
    """Corpo de POST /orders. O FastAPI responde 422 automaticamente se não respeitar estas regras."""

    customer_id: str = Field(min_length=1, max_length=100, examples=["cliente-1"])
    customer_email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", max_length=200, examples=["ana@exemplo.pt"])
    event_id: str = Field(min_length=1, max_length=100, examples=["evt-101"])
    quantity: int = Field(ge=1, le=MAX_TICKETS_PER_ORDER, examples=[2])


class Order(BaseModel):
    """Uma compra, tal como é guardada e devolvida pela API."""

    id: str
    customer_id: str
    customer_email: str
    event_id: str
    quantity: int
    total_amount: float
    status: str  # PENDING, CONFIRMED, CANCELLED, FAILED
    created_at: str


# ---------------------------------------------------------------------------
# Base de dados (SQLite)
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id             TEXT PRIMARY KEY,
    customer_id    TEXT NOT NULL,
    customer_email TEXT NOT NULL,
    event_id       TEXT NOT NULL,
    quantity       INTEGER NOT NULL CHECK (quantity > 0),
    total_amount   REAL NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'CANCELLED', 'FAILED')),
    created_at     TEXT NOT NULL
)
"""


def open_database(path: Path) -> sqlite3.Connection:
    """
    Abre (e cria, se ainda não existir) a base de dados.

    - WAL (Write-Ahead Logging): vários processos podem ler enquanto outro escreve, e as escritas
      de processos diferentes ficam em fila em vez de falharem.
    - busy_timeout: se outro processo estiver a escrever, espera até 5 s em vez de dar erro.
    - synchronous=NORMAL: seguro em modo WAL e muito mais rápido do que o valor por omissão.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5, isolation_level=None)  # isolation_level=None: cada comando é gravado logo
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(SCHEMA)
    return conn


def insert_order(conn: sqlite3.Connection, order: Order) -> None:
    conn.execute(
        "INSERT INTO orders (id, customer_id, customer_email, event_id, quantity, total_amount, status, created_at)"
        " VALUES (:id, :customer_id, :customer_email, :event_id, :quantity, :total_amount, :status, :created_at)",
        order.model_dump(),
    )


def update_status(conn: sqlite3.Connection, order_id: str, status: str) -> None:
    conn.execute("UPDATE orders SET status = ? WHERE id = ?", (status, order_id))


def find_order(conn: sqlite3.Connection, order_id: str) -> Order | None:
    row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    return Order(**dict(row)) if row else None


# ---------------------------------------------------------------------------
# Métricas Prometheus
# ---------------------------------------------------------------------------

ORDERS_INITIATED = Counter(
    "orders_initiated_total",
    "Compras iniciadas, por estado em que ficaram (PENDING: aceite e mensagem publicada; FAILED: falhou a publicação)",
    ["status"],
)
# Cria as duas séries a zero logo no arranque, para os gráficos mostrarem 0 em vez de "No data"
for _status in ("PENDING", "FAILED"):
    ORDERS_INITIATED.labels(status=_status)

REQUESTS_TOTAL = Counter(
    "order_requests_total",
    "Total de pedidos HTTP processados pelo Order",
    ["method", "endpoint", "http_status"],
)

REQUEST_DURATION = Histogram(
    "order_request_duration_seconds",
    "Duração dos pedidos HTTP no Order (inclui a chamada ao Catalog, o SQLite e o RabbitMQ), em segundos",
    ["endpoint"],
    buckets=(0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 1.0, 2.5, 5.0),
)

UNINSTRUMENTED_PATHS = {"/metrics", "/health"}

# Com vários processos, cada um guarda as métricas em ficheiros em PROMETHEUS_MULTIPROC_DIR
# e o /metrics soma-as todas (o mesmo mecanismo do Gateway e do Catalog).
MULTIPROCESS_MODE = bool(os.getenv("PROMETHEUS_MULTIPROC_DIR"))


def collect_metrics() -> bytes:
    if MULTIPROCESS_MODE:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return generate_latest(registry)
    return generate_latest()


class MetricsMiddleware:
    """Mede todos os pedidos HTTP (o mesmo desenho do Gateway e do Catalog)."""

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
# RabbitMQ
# ---------------------------------------------------------------------------

async def keep_rabbitmq_connected(app: FastAPI) -> None:
    """
    Liga ao RabbitMQ em segundo plano, tentando de novo até conseguir.

    O serviço arranca e responde mesmo que o RabbitMQ ainda não esteja pronto: enquanto não houver
    ligação, as compras ficam FAILED (503). Depois da primeira ligação, o connect_robust volta a ligar
    sozinho se o RabbitMQ reiniciar.
    """
    while True:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL, reconnect_interval=RABBITMQ_RETRY_SECONDS)
            # publisher_confirms: cada publicação espera pela confirmação do RabbitMQ de que a mensagem
            # ficou guardada na fila. Sem isto, uma mensagem podia perder-se sem o Order saber.
            channel = await connection.channel(publisher_confirms=True)
            # Fila durável: sobrevive a um reinício do RabbitMQ. Declarar é idempotente: se já existir
            # (criada por outro processo ou pelo Notification), não acontece nada.
            await channel.declare_queue(ORDER_QUEUE, durable=True)
            app.state.amqp_connection = connection
            app.state.amqp_channel = channel
            logger.info("Ligado ao RabbitMQ; mensagens OrderCreated vão para a fila '%s'", ORDER_QUEUE)
            return
        except Exception as exc:
            logger.warning(
                "RabbitMQ indisponível (%s); nova tentativa em %.0fs", type(exc).__name__, RABBITMQ_RETRY_SECONDS
            )
            await asyncio.sleep(RABBITMQ_RETRY_SECONDS)


def rabbitmq_is_connected(app: FastAPI) -> bool:
    channel = getattr(app.state, "amqp_channel", None)
    return channel is not None and not channel.is_closed


async def publish_order_created(app: FastAPI, order: Order) -> None:
    """
    Publica a mensagem OrderCreated na fila. Levanta uma exceção se não for possível.

    A mensagem é persistente (o RabbitMQ grava-a em disco) e só se considera publicada depois
    de o RabbitMQ a confirmar.
    """
    if not rabbitmq_is_connected(app):
        raise ConnectionError("Sem ligação ao RabbitMQ")

    payload = {
        "type": "OrderCreated",
        "order_id": order.id,
        "customer_id": order.customer_id,
        "customer_email": order.customer_email,
        "event_id": order.event_id,
        "quantity": order.quantity,
        "total_amount": order.total_amount,
        "created_at": order.created_at,
    }
    message = aio_pika.Message(
        body=json.dumps(payload).encode("utf-8"),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        message_id=order.id,
        type="OrderCreated",
    )
    # Exchange por omissão: entrega a mensagem diretamente à fila com o nome indicado em routing_key
    await app.state.amqp_channel.default_exchange.publish(
        message, routing_key=ORDER_QUEUE, timeout=PUBLISH_TIMEOUT_SECONDS
    )


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

class CatalogError(Exception):
    """Falha ao obter o evento no Catalog, com a resposta HTTP a devolver ao cliente."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail


async def fetch_event_price(app: FastAPI, event_id: str) -> float:
    """
    Pede o evento ao Catalog (pedido síncrono) e devolve o preço por bilhete.

    O preço vem sempre do Catalog e nunca do cliente: assim ninguém consegue comprar
    bilhetes ao preço que quiser.
    """
    url = f"{CATALOG_URL}/events/{quote(event_id, safe='')}"
    try:
        async with app.state.http.get(url) as response:
            if response.status == 404:
                raise CatalogError(422, f"O evento '{event_id}' não existe")
            if response.status != 200:
                logger.warning("Catalog respondeu %d a GET %s", response.status, url)
                raise CatalogError(502, "O serviço Catalog devolveu um erro")
            event = await response.json()
            return float(event["price_eur"])
    except asyncio.TimeoutError:
        raise CatalogError(504, "O serviço Catalog não respondeu a tempo") from None
    except aiohttp.ClientError as exc:
        logger.warning("Falha a contactar o Catalog em GET %s: %s", url, exc)
        raise CatalogError(502, "O serviço Catalog está indisponível") from None


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Base de dados: uma ligação por processo
    app.state.db = open_database(ORDERS_DB_PATH)
    logger.info("Base de dados em %s", ORDERS_DB_PATH)

    # Cliente HTTP para o Catalog, com ligações reutilizadas (como no Gateway)
    app.state.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=CATALOG_TIMEOUT_SECONDS))

    # RabbitMQ: liga em segundo plano, para o serviço não ficar bloqueado se o RabbitMQ demorar
    app.state.amqp_channel = None
    app.state.amqp_connection = None
    connect_task = asyncio.create_task(keep_rabbitmq_connected(app))

    yield

    connect_task.cancel()
    with suppress(asyncio.CancelledError):
        await connect_task
    if app.state.amqp_connection is not None:
        await app.state.amqp_connection.close()
    await app.state.http.close()
    app.state.db.close()


app = FastAPI(
    title="TicketMEI - Order",
    description="Compras de bilhetes. Cada compra fica PENDING e gera uma mensagem OrderCreated no RabbitMQ.",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(MetricsMiddleware)


def error(status_code: int, detail: str, **extra) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail, **extra})


@app.post(
    "/orders",
    status_code=202,
    response_model=Order,
    responses={
        422: {"description": "Pedido inválido (campos, email, mais de 4 bilhetes) ou evento inexistente"},
        502: {"description": "Catalog indisponível ou com erro"},
        503: {"description": "RabbitMQ indisponível: a compra ficou FAILED"},
        504: {"description": "Catalog não respondeu a tempo"},
    },
)
async def create_order(request: OrderRequest) -> Response:
    """Cria uma compra no estado PENDING e publica a mensagem OrderCreated."""
    try:
        price = await fetch_event_price(app, request.event_id)
    except CatalogError as exc:
        return error(exc.status_code, exc.detail)

    order = Order(
        id=str(uuid.uuid4()),
        customer_id=request.customer_id,
        customer_email=request.customer_email,
        event_id=request.event_id,
        quantity=request.quantity,
        total_amount=round(price * request.quantity, 2),
        status="PENDING",
        created_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )

    # 1.º grava, 2.º publica: se a publicação falhar, a compra fica registada como FAILED
    insert_order(app.state.db, order)
    try:
        await publish_order_created(app, order)
    except Exception as exc:
        logger.warning("Não foi possível publicar OrderCreated da compra %s: %s", order.id, type(exc).__name__)
        update_status(app.state.db, order.id, "FAILED")
        ORDERS_INITIATED.labels(status="FAILED").inc()
        order.status = "FAILED"
        return error(503, "Não foi possível registar a compra: o sistema de mensagens está indisponível",
                     order=order.model_dump())

    ORDERS_INITIATED.labels(status="PENDING").inc()
    return JSONResponse(status_code=202, content=order.model_dump(), headers={"Location": f"/orders/{order.id}"})


@app.get("/orders/{order_id}", response_model=Order, responses={404: {"description": "Compra não encontrada"}})
async def get_order(order_id: str) -> Response:
    """Devolve os detalhes de uma compra."""
    order = find_order(app.state.db, order_id)
    if order is None:
        return error(404, f"Compra '{order_id}' não encontrada")
    return JSONResponse(content=order.model_dump())


@app.get("/health")
async def health() -> dict:
    """
    Indica se o serviço está a funcionar. Não falha se o RabbitMQ estiver em baixo (o Order continua
    a responder, com as compras a ficarem FAILED); o campo "rabbitmq" mostra o estado da ligação.
    """
    return {"status": "ok", "rabbitmq": "connected" if rabbitmq_is_connected(app) else "disconnected"}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Expõe as métricas para o Prometheus (somadas de todos os processos, se houver vários)."""
    return Response(content=collect_metrics(), media_type=CONTENT_TYPE_LATEST)
