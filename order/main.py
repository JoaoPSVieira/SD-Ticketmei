"""
TicketMEI - serviço Order

Recebe os pedidos de compra de bilhetes, regista cada compra numa base de dados própria (SQLite),
cobra a compra no serviço Payment e, se o pagamento correr bem, confirma-a e publica a mensagem
"OrderCreated" no RabbitMQ, para o serviço Notification enviar o email de confirmação.

As chamadas ao Payment passam por um circuit breaker com retry e jitter (circuit_breaker.py): se o
Payment estiver a falhar ou lento, o circuito abre e o Order responde logo 503, sem chamar o Payment.

Endpoints:
    POST /orders         -> cria e cobra uma compra -> 201 CONFIRMED, 502 ou 503 (FAILED)
    GET  /orders/{id}    -> detalhes de uma compra (404 se não existir)
    GET  /health         -> estado do serviço (não depende do RabbitMQ, do Catalog nem do Payment)
    GET  /metrics        -> métricas no formato Prometheus

Fluxo de POST /orders:
    1. Valida o pedido (campos obrigatórios, email, 1 a 4 bilhetes)        -> 422 se inválido
    2. Pergunta ao Catalog o preço do evento (pedido síncrono)            -> 422 se o evento não existe,
                                                                             502/504 se o Catalog falhar
    3. Grava a compra como PENDING no SQLite
    4. Cobra no Payment (POST /payments), pelo circuit breaker e com retry:
         - pagamento aceite                -> CONFIRMED, segue para o passo 5
         - circuito aberto                 -> FAILED, 503 {"error": "Payment system unavailable, try again later"}
         - falhou em todas as tentativas   -> FAILED, 502 {"error": "Payment failed", ...}
    5. Publica OrderCreated na exchange orders.exchange do RabbitMQ         -> 201 Created
       Se a publicação falhar, a compra continua CONFIRMED (já foi paga): o email não é enviado e a
       falha fica registada no log e na métrica order_publish_failures_total.

    Enquanto o Payment não existir no cluster, PAYMENT_URL fica vazio e o passo 4 não acontece: a
    compra é confirmada logo a seguir a ser gravada.

Métricas:
    orders_initiated_total            Counter    labels: status (CONFIRMED, FAILED)
    circuit_breaker_state             Gauge      0 = CLOSED, 1 = OPEN, 2 = HALF_OPEN
    circuit_breaker_fallbacks_total   Counter    tentativas recusadas pelo circuito (sem chamar o Payment)
    payment_client_requests_total     Counter    labels: status (2xx, 500, timeout, error, ...)
    order_publish_failures_total      Counter    compras confirmadas cuja mensagem não foi publicada
    order_requests_total              Counter    labels: method, endpoint, http_status
    order_request_duration_seconds    Histogram  labels: endpoint

Processos:
    O Order corre com um só processo (WEB_CONCURRENCY=1): cada processo teria o seu circuit breaker,
    e o estado do circuito ficaria diferente de processo para processo. A base de dados (SQLite em
    modo WAL) e as métricas (PROMETHEUS_MULTIPROC_DIR) continuam a funcionar com vários processos.
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
    Gauge,
    Histogram,
    generate_latest,
    multiprocess,
)
from pydantic import BaseModel, Field

from circuit_breaker import CircuitBreaker, State, full_jitter_delay

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

# Serviço Payment (Parte II). Vazio: sem passo de pagamento (enquanto o Payment não existir no cluster).
PAYMENT_URL = os.getenv("PAYMENT_URL", "").rstrip("/")
# Cada tentativa espera no máximo isto pelo Payment. Mais lento do que isto conta como falha.
PAYMENT_TIMEOUT_SECONDS = float(os.getenv("PAYMENT_TIMEOUT_SECONDS", "1"))
# Tentativas no total (1 = sem retry) e base do intervalo aleatório entre tentativas
PAYMENT_RETRY_ATTEMPTS = int(os.getenv("PAYMENT_RETRY_ATTEMPTS", "3"))
PAYMENT_RETRY_BASE_SECONDS = float(os.getenv("PAYMENT_RETRY_BASE_SECONDS", "0.1"))

# Circuit breaker: abre com N falhas seguidas, fica aberto X segundos, depois deixa passar M chamadas de teste
CB_FAILURE_THRESHOLD = int(os.getenv("CB_FAILURE_THRESHOLD", "5"))
CB_OPEN_SECONDS = float(os.getenv("CB_OPEN_SECONDS", "15"))
CB_HALF_OPEN_MAX_CALLS = int(os.getenv("CB_HALF_OPEN_MAX_CALLS", "1"))

# RabbitMQ: para onde vão as mensagens OrderCreated.
# No cluster local usa-se RABBITMQ_URL. No cluster global (RabbitMQ partilhado da UMa) as credenciais
# vêm de um Secret, por isso aceitam-se também as partes em separado (ver rabbitmq_url()).
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://ticketmei:ticketmei@localhost:5672/")
ORDERS_EXCHANGE = os.getenv("ORDERS_EXCHANGE", "orders.exchange")
ORDER_ROUTING_KEY = os.getenv("ORDER_ROUTING_KEY", "order.created")
ORDER_QUEUE = os.getenv("ORDER_QUEUE", "order_created")
PUBLISH_TIMEOUT_SECONDS = float(os.getenv("PUBLISH_TIMEOUT_SECONDS", "2"))
RABBITMQ_RETRY_SECONDS = float(os.getenv("RABBITMQ_RETRY_SECONDS", "2"))

# Base de dados do Order: um ficheiro SQLite (no contentor fica num volume do Docker)
ORDERS_DB_PATH = Path(os.getenv("ORDERS_DB_PATH", Path(__file__).parent / "data" / "orders.db"))

# RF2: limite de bilhetes por compra
MAX_TICKETS_PER_ORDER = int(os.getenv("MAX_TICKETS_PER_ORDER", "4"))

# Resposta do enunciado quando o circuito está aberto
PAYMENT_UNAVAILABLE = {"error": "Payment system unavailable, try again later"}


def rabbitmq_url() -> str:
    """
    URL de ligação ao RabbitMQ.

    Se RABBITMQ_HOST estiver definido (cluster global), junta RABBITMQ_USER, RABBITMQ_PASSWORD,
    RABBITMQ_HOST, RABBITMQ_PORT e RABBITMQ_VHOST. O vhost vai codificado no URL: "/grupo01" passa a
    "%2Fgrupo01". Senão, usa RABBITMQ_URL (cluster local e docker compose).
    """
    host = os.getenv("RABBITMQ_HOST")
    if not host:
        return RABBITMQ_URL
    user = quote(os.getenv("RABBITMQ_USER", "guest"), safe="")
    password = quote(os.getenv("RABBITMQ_PASSWORD", "guest"), safe="")
    port = os.getenv("RABBITMQ_PORT", "5672")
    vhost = quote(os.getenv("RABBITMQ_VHOST", "/"), safe="")
    return f"amqp://{user}:{password}@{host}:{port}/{vhost}"


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
    "Compras iniciadas, por estado em que ficaram (CONFIRMED: paga e confirmada; FAILED: pagamento falhou)",
    ["status"],
)
# Cria as séries a zero logo no arranque, para os gráficos mostrarem 0 em vez de "No data"
for _status in ("CONFIRMED", "FAILED"):
    ORDERS_INITIATED.labels(status=_status)

CIRCUIT_BREAKER_STATE = Gauge(
    "circuit_breaker_state",
    "Estado do circuit breaker das chamadas ao Payment: 0 = CLOSED, 1 = OPEN, 2 = HALF_OPEN",
    # Com vários processos (PROMETHEUS_MULTIPROC_DIR), mostra o valor mais alto dos processos vivos
    multiprocess_mode="livemax",
)

CIRCUIT_BREAKER_FALLBACKS = Counter(
    "circuit_breaker_fallbacks_total",
    "Tentativas de pagamento recusadas pelo circuit breaker (circuito aberto): o Payment não foi chamado",
)

PAYMENT_CLIENT_REQUESTS = Counter(
    "payment_client_requests_total",
    "Pedidos do Order ao Payment, pelo resultado (2xx, código de erro, timeout ou error de ligação). "
    "Não conta as tentativas recusadas pelo circuito, porque aí não há pedido",
    ["status"],
)
for _status in ("2xx", "500", "timeout"):
    PAYMENT_CLIENT_REQUESTS.labels(status=_status)

PUBLISH_FAILURES = Counter(
    "order_publish_failures_total",
    "Compras confirmadas cuja mensagem OrderCreated não foi publicada (o email não foi enviado)",
)

REQUESTS_TOTAL = Counter(
    "order_requests_total",
    "Total de pedidos HTTP processados pelo Order",
    ["method", "endpoint", "http_status"],
)

REQUEST_DURATION = Histogram(
    "order_request_duration_seconds",
    "Duração dos pedidos HTTP no Order (inclui o Catalog, o Payment, o SQLite e o RabbitMQ), em segundos",
    ["endpoint"],
    buckets=(0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 1.0, 2.5, 5.0, 10.0),
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
    ligação, as compras confirmadas não geram email (order_publish_failures_total). Depois da primeira
    ligação, o connect_robust volta a ligar sozinho se o RabbitMQ reiniciar.
    """
    while True:
        try:
            connection = await aio_pika.connect_robust(rabbitmq_url(), reconnect_interval=RABBITMQ_RETRY_SECONDS)
            # publisher_confirms: cada publicação espera pela confirmação do RabbitMQ de que a mensagem
            # ficou guardada na fila. Sem isto, uma mensagem podia perder-se sem o Order saber.
            channel = await connection.channel(publisher_confirms=True)
            # Exchange e fila duráveis (sobrevivem a um reinício do RabbitMQ), ligadas pela routing key.
            # Declarar é idempotente: se já existirem (criadas por outro processo ou pelo Notification),
            # não acontece nada. O Notification consome a fila order_created, como na Parte I.
            exchange = await channel.declare_exchange(ORDERS_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True)
            queue = await channel.declare_queue(ORDER_QUEUE, durable=True)
            await queue.bind(exchange, routing_key=ORDER_ROUTING_KEY)
            app.state.amqp_connection = connection
            app.state.amqp_channel = channel
            app.state.amqp_exchange = exchange
            logger.info(
                "Ligado ao RabbitMQ; mensagens OrderCreated vão para a exchange '%s' (routing key '%s', fila '%s')",
                ORDERS_EXCHANGE, ORDER_ROUTING_KEY, ORDER_QUEUE,
            )
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
    Publica a mensagem OrderCreated na exchange. Levanta uma exceção se não for possível.

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
        "status": order.status,
        "created_at": order.created_at,
    }
    message = aio_pika.Message(
        body=json.dumps(payload).encode("utf-8"),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        message_id=order.id,
        type="OrderCreated",
    )
    await app.state.amqp_exchange.publish(message, routing_key=ORDER_ROUTING_KEY, timeout=PUBLISH_TIMEOUT_SECONDS)


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
# Payment (circuit breaker e retry com jitter)
# ---------------------------------------------------------------------------

class CircuitOpenError(Exception):
    """O circuito está aberto: o pagamento nem foi tentado (fail-fast)."""


class PaymentFailedError(Exception):
    """O Payment recusou ou falhou em todas as tentativas."""


class PaymentAttemptError(Exception):
    """
    Uma tentativa de pagamento falhou.

    counts_for_breaker: True para 5xx, timeout e erro de ligação (o Payment está com problemas);
    False para 4xx (o pedido é que está errado: repetir não adianta e não é culpa do Payment).
    """

    def __init__(self, reason: str, counts_for_breaker: bool = True):
        super().__init__(reason)
        self.reason = reason
        self.counts_for_breaker = counts_for_breaker


def new_circuit_breaker() -> CircuitBreaker:
    def on_state_change(old: State, new: State) -> None:
        CIRCUIT_BREAKER_STATE.set(int(new))
        logger.warning("Circuit breaker do Payment: %s -> %s", old.name, new.name)

    breaker = CircuitBreaker(
        failure_threshold=CB_FAILURE_THRESHOLD,
        open_seconds=CB_OPEN_SECONDS,
        half_open_max_calls=CB_HALF_OPEN_MAX_CALLS,
        on_state_change=on_state_change,
    )
    CIRCUIT_BREAKER_STATE.set(int(breaker.state))
    return breaker


async def request_payment(app: FastAPI, order: Order) -> dict:
    """Uma tentativa: POST /payments. Devolve o pagamento ou levanta PaymentAttemptError."""
    url = f"{PAYMENT_URL}/payments"
    body = {"order_id": order.id, "amount": order.total_amount}
    try:
        async with app.state.payment_http.post(url, json=body) as response:
            if response.status in (200, 201):
                payment = await response.json(content_type=None)
                PAYMENT_CLIENT_REQUESTS.labels(status="2xx").inc()
                return payment
            PAYMENT_CLIENT_REQUESTS.labels(status=str(response.status)).inc()
            raise PaymentAttemptError(f"HTTP {response.status}", counts_for_breaker=response.status >= 500)
    except asyncio.TimeoutError:
        PAYMENT_CLIENT_REQUESTS.labels(status="timeout").inc()
        raise PaymentAttemptError(f"timeout ({PAYMENT_TIMEOUT_SECONDS:g}s)") from None
    except (aiohttp.ClientError, ValueError) as exc:
        PAYMENT_CLIENT_REQUESTS.labels(status="error").inc()
        raise PaymentAttemptError(f"erro de ligação ({type(exc).__name__})") from None


async def charge_payment(app: FastAPI, order: Order) -> dict:
    """
    Cobra a compra no Payment: até PAYMENT_RETRY_ATTEMPTS tentativas, todas pelo circuit breaker.

    - Antes de cada tentativa pergunta ao circuito se pode chamar. Se o circuito estiver aberto,
      não chama o Payment (conta um fallback) e levanta CircuitOpenError.
    - Entre tentativas espera um tempo aleatório (full jitter). Se o circuito abrir com a falha
      anterior, deixa de tentar logo, sem esperar.
    - Um 4xx não se repete nem conta para o circuito: o pedido é que está errado.
    """
    breaker: CircuitBreaker = app.state.breaker
    last_error = "sem tentativas"
    for attempt in range(1, PAYMENT_RETRY_ATTEMPTS + 1):
        if not breaker.allow_request():
            CIRCUIT_BREAKER_FALLBACKS.inc()
            raise CircuitOpenError()
        try:
            payment = await request_payment(app, order)
        except PaymentAttemptError as exc:
            last_error = exc.reason
            logger.warning("Pagamento da compra %s falhou (tentativa %d de %d): %s",
                           order.id, attempt, PAYMENT_RETRY_ATTEMPTS, exc.reason)
            if not exc.counts_for_breaker:
                # O Payment respondeu (está a funcionar): para o circuito, conta como sucesso
                breaker.record_success()
                break
            breaker.record_failure()
            if attempt < PAYMENT_RETRY_ATTEMPTS:
                if breaker.state is State.OPEN:
                    # O circuito abriu com esta falha: os retries param imediatamente
                    CIRCUIT_BREAKER_FALLBACKS.inc()
                    raise CircuitOpenError() from None
                await asyncio.sleep(full_jitter_delay(attempt, PAYMENT_RETRY_BASE_SECONDS))
            continue
        breaker.record_success()
        return payment
    raise PaymentFailedError(last_error)


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Base de dados: uma ligação por processo
    app.state.db = open_database(ORDERS_DB_PATH)
    logger.info("Base de dados em %s", ORDERS_DB_PATH)

    # Clientes HTTP para o Catalog e para o Payment, com ligações reutilizadas (como no Gateway)
    app.state.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=CATALOG_TIMEOUT_SECONDS))
    app.state.payment_http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=PAYMENT_TIMEOUT_SECONDS))

    # Circuit breaker das chamadas ao Payment (um por processo)
    app.state.breaker = new_circuit_breaker()
    if PAYMENT_URL:
        logger.info(
            "Pagamentos em %s (timeout %gs, %d tentativas; circuito abre com %d falhas seguidas, fica aberto %gs)",
            PAYMENT_URL, PAYMENT_TIMEOUT_SECONDS, PAYMENT_RETRY_ATTEMPTS, CB_FAILURE_THRESHOLD, CB_OPEN_SECONDS,
        )
    else:
        logger.warning("PAYMENT_URL vazio: as compras são confirmadas sem passar pelo Payment")

    # RabbitMQ: liga em segundo plano, para o serviço não ficar bloqueado se o RabbitMQ demorar
    app.state.amqp_channel = None
    app.state.amqp_connection = None
    app.state.amqp_exchange = None
    connect_task = asyncio.create_task(keep_rabbitmq_connected(app))

    yield

    connect_task.cancel()
    with suppress(asyncio.CancelledError):
        await connect_task
    if app.state.amqp_connection is not None:
        await app.state.amqp_connection.close()
    await app.state.payment_http.close()
    await app.state.http.close()
    app.state.db.close()


app = FastAPI(
    title="TicketMEI - Order",
    description="Compras de bilhetes. Cada compra é cobrada no Payment (com circuit breaker) e, se for paga, "
                "fica CONFIRMED e gera uma mensagem OrderCreated no RabbitMQ.",
    version="2.0.0",
    lifespan=lifespan,
)
app.add_middleware(MetricsMiddleware)


def error(status_code: int, detail: str, **extra) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail, **extra})


def fail_order(order: Order) -> None:
    update_status(app.state.db, order.id, "FAILED")
    order.status = "FAILED"
    ORDERS_INITIATED.labels(status="FAILED").inc()


@app.post(
    "/orders",
    status_code=201,
    response_model=Order,
    responses={
        422: {"description": "Pedido inválido (campos, email, mais de 4 bilhetes) ou evento inexistente"},
        502: {"description": "Catalog indisponível, ou pagamento falhou em todas as tentativas (compra FAILED)"},
        503: {"description": "Circuito do Payment aberto (compra FAILED): "
                             "{\"error\": \"Payment system unavailable, try again later\"}"},
        504: {"description": "Catalog não respondeu a tempo"},
    },
)
async def create_order(request: OrderRequest) -> Response:
    """Cria uma compra, cobra-a no Payment e, se for paga, confirma-a e publica OrderCreated."""
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
    # A compra fica gravada antes de cobrar: se o pagamento falhar, fica registada como FAILED
    insert_order(app.state.db, order)

    if PAYMENT_URL:
        try:
            await charge_payment(app, order)
        except CircuitOpenError:
            fail_order(order)
            return JSONResponse(status_code=503, content=PAYMENT_UNAVAILABLE)
        except PaymentFailedError as exc:
            fail_order(order)
            return JSONResponse(status_code=502, content={"error": "Payment failed", "order_id": order.id,
                                                          "reason": str(exc)})

    update_status(app.state.db, order.id, "CONFIRMED")
    order.status = "CONFIRMED"
    ORDERS_INITIATED.labels(status="CONFIRMED").inc()

    # A compra já está paga e confirmada: se a mensagem não puder ser publicada, a compra não muda,
    # só o email de confirmação é que não é enviado (fica no log e na métrica)
    try:
        await publish_order_created(app, order)
    except Exception as exc:
        PUBLISH_FAILURES.inc()
        logger.warning("Compra %s confirmada, mas o OrderCreated não foi publicado: %s", order.id, type(exc).__name__)

    return JSONResponse(status_code=201, content=order.model_dump(), headers={"Location": f"/orders/{order.id}"})


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
    Indica se o serviço está a funcionar. Não falha se o RabbitMQ ou o Payment estiverem em baixo (o
    Order continua a responder); mostra o estado da ligação ao RabbitMQ e do circuito do Payment.
    """
    return {
        "status": "ok",
        "rabbitmq": "connected" if rabbitmq_is_connected(app) else "disconnected",
        "payment": app.state.breaker.state.name if PAYMENT_URL else "disabled",
    }


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Expõe as métricas para o Prometheus (somadas de todos os processos, se houver vários)."""
    return Response(content=collect_metrics(), media_type=CONTENT_TYPE_LATEST)
