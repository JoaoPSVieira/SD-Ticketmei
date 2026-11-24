"""
TicketMEI - serviço Notification.

Consome as mensagens OrderCreated da fila order_created do RabbitMQ (publicadas pelo Order),
simula o envio de um email de confirmação (100 ms) e regista cada entrega num log.

RabbitMQ (Parte II):
    - Exchange orders.exchange (direct, durável) e fila order_created (durável), ligadas pela routing
      key order.created. O Order publica aí o OrderCreated depois de a compra ficar CONFIRMED.
    - A fila continua a receber o que chega pela exchange por omissão (o Order da Parte I).
    - Endereço: RABBITMQ_URL no cluster local; RABBITMQ_HOST, RABBITMQ_VHOST, RABBITMQ_USER e
      RABBITMQ_PASSWORD no cluster global (RabbitMQ partilhado, um vhost por grupo).

Não tem endpoints de negócio: o trabalho todo vem da fila. Expõe apenas:
    GET /metrics   -> métricas no formato Prometheus
    GET /health    -> estado do serviço (healthcheck do Docker)

Modelo de dados (uma linha JSON por notificação no log de entregas):
    order_id, email, status (SENT ou FAILED), reason (motivo, se FAILED), timestamp

Métricas:
    notifications_processed_total               Counter    labels: status (SENT, FAILED)
    notification_processing_duration_seconds    Histogram

Garantias:
    - Cada mensagem só é confirmada ao RabbitMQ (ack) DEPOIS de processada. Se o serviço for abaixo
      a meio, a mensagem volta para a fila e é entregue de novo: nenhuma notificação se perde.
    - O "prefetch" limita quantas mensagens o serviço processa em paralelo (por omissão, 10).
"""

import asyncio
import json
import logging
import os
import random
import re
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import aio_pika
from fastapi import FastAPI
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuração (variáveis de ambiente)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [notification] %(message)s",
)
logger = logging.getLogger("notification")
# Mostra só os erros da biblioteca do RabbitMQ (sem tracebacks longos quando a ligação cai e volta)
logging.getLogger("aiormq").setLevel(logging.ERROR)

def rabbitmq_url_from_env(env: Mapping[str, str]) -> str:
    """
    Endereço do RabbitMQ (regra combinada com o Order, secção 8 do plano da Parte II):

    - cluster local: RABBITMQ_URL (o RabbitMQ do próprio projeto);
    - cluster global: RABBITMQ_HOST, RABBITMQ_VHOST, RABBITMQ_USER e RABBITMQ_PASSWORD (o RabbitMQ
      partilhado da UMa, com um vhost por grupo). Se RABBITMQ_HOST estiver definido, ganha.

    O vhost vai codificado no caminho do URL ("/grupoXX" -> "%2FgrupoXX"), e o utilizador e a
    password também, para uma password com "@", ":" ou "/" não estragar o URL.
    """
    host = env.get("RABBITMQ_HOST", "").strip()
    if not host:
        return env.get("RABBITMQ_URL", "amqp://ticketmei:ticketmei@localhost:5672/")

    user, password = env.get("RABBITMQ_USER"), env.get("RABBITMQ_PASSWORD")
    if not user or not password:
        raise ValueError("RABBITMQ_HOST está definido, mas faltam RABBITMQ_USER e/ou RABBITMQ_PASSWORD")
    port = env.get("RABBITMQ_PORT", "5672")
    vhost = env.get("RABBITMQ_VHOST", "/")
    return f"amqp://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}/{quote(vhost, safe='')}"


# Um valor inválido (ex.: RABBITMQ_HOST sem credenciais) impede o serviço de arrancar, com o erro no log
RABBITMQ_URL = rabbitmq_url_from_env(os.environ)
ORDER_QUEUE = os.getenv("ORDER_QUEUE", "order_created")
# Parte II: o Order publica o OrderCreated nesta exchange (direct, durável) com esta routing key
ORDERS_EXCHANGE = os.getenv("ORDERS_EXCHANGE", "orders.exchange")
ORDER_ROUTING_KEY = os.getenv("ORDER_ROUTING_KEY", "order.created")
RABBITMQ_RETRY_SECONDS = float(os.getenv("RABBITMQ_RETRY_SECONDS", "2"))

# Quantas mensagens são processadas em paralelo. Com 100 ms por email, 10 dá ~100 notificações/s.
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "10"))

# Simulação do envio do email
EMAIL_DELAY_SECONDS = float(os.getenv("EMAIL_DELAY_SECONDS", "0.1"))
# Percentagem de envios que falham de propósito (0 a 1), para demonstrar o estado FAILED nos dashboards
NOTIFICATION_FAILURE_RATE = float(os.getenv("NOTIFICATION_FAILURE_RATE", "0"))

# Log de entregas: uma linha JSON por notificação (no contentor fica num volume do Docker)
NOTIFICATIONS_LOG_PATH = Path(
    os.getenv("NOTIFICATIONS_LOG_PATH", Path(__file__).parent / "logs" / "notifications.log")
)

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ---------------------------------------------------------------------------
# Modelo de dados
# ---------------------------------------------------------------------------

class Notification(BaseModel):
    """Registo de uma notificação, tal como fica no log de entregas."""

    order_id: str | None
    email: str | None
    status: str  # SENT ou FAILED
    reason: str | None = None
    timestamp: str


# ---------------------------------------------------------------------------
# Métricas Prometheus
# ---------------------------------------------------------------------------

NOTIFICATIONS_PROCESSED = Counter(
    "notifications_processed_total",
    "Notificações processadas, por resultado (SENT ou FAILED)",
    ["status"],
)
# Cria as duas séries a zero logo no arranque, para os gráficos mostrarem 0 em vez de "No data"
for _status in ("SENT", "FAILED"):
    NOTIFICATIONS_PROCESSED.labels(status=_status)

PROCESSING_DURATION = Histogram(
    "notification_processing_duration_seconds",
    "Tempo a processar cada mensagem OrderCreated (inclui o envio simulado do email), em segundos",
    # Concentrados à volta dos 100 ms do envio simulado
    buckets=(0.05, 0.09, 0.1, 0.105, 0.11, 0.125, 0.15, 0.2, 0.3, 0.5, 1.0),
)


# ---------------------------------------------------------------------------
# Log de entregas
# ---------------------------------------------------------------------------

def setup_delivery_log(path: Path) -> logging.Logger:
    """Logger que escreve uma linha JSON por notificação no ficheiro de entregas."""
    path.parent.mkdir(parents=True, exist_ok=True)
    delivery_log = logging.getLogger("notification.deliveries")
    delivery_log.setLevel(logging.INFO)
    delivery_log.propagate = False  # não repete a linha no log normal
    for handler in list(delivery_log.handlers):
        delivery_log.removeHandler(handler)
        handler.close()
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    delivery_log.addHandler(handler)
    return delivery_log


# ---------------------------------------------------------------------------
# Processamento de uma mensagem
# ---------------------------------------------------------------------------

async def send_email(email: str, order: dict) -> None:
    """
    Simula o envio do email de confirmação (o enunciado pede um intervalo de 100 ms).
    Num sistema real, aqui ficaria a chamada a um servidor de email (SMTP) ou a um serviço externo.
    """
    await asyncio.sleep(EMAIL_DELAY_SECONDS)
    if random.random() < NOTIFICATION_FAILURE_RATE:
        raise RuntimeError("falha simulada no envio do email")


async def process_notification(body: bytes) -> Notification:
    """
    Trata uma mensagem OrderCreated: valida-a, "envia" o email e regista o resultado.

    Mensagens inválidas e envios falhados ficam registados como FAILED (não voltam para a fila:
    repetir não as tornaria válidas).
    """
    start = time.perf_counter()
    order_id = email = None
    try:
        order = json.loads(body)
        order_id = order.get("order_id")
        email = order.get("customer_email")
        if not order_id or not email:
            raise ValueError("mensagem sem order_id ou customer_email")
        if not EMAIL_PATTERN.match(email):
            raise ValueError("email inválido")
        await send_email(email, order)
        status, reason = "SENT", None
    except (ValueError, AttributeError) as exc:  # JSON inválido ou campos em falta/errados
        status, reason = "FAILED", f"mensagem inválida: {exc}"
    except RuntimeError as exc:
        status, reason = "FAILED", str(exc)

    notification = Notification(
        order_id=order_id,
        email=email,
        status=status,
        reason=reason,
        timestamp=datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    )

    app.state.delivery_log.info(notification.model_dump_json())
    if status == "SENT":
        logger.info("Email de confirmação enviado para %s (compra %s)", email, order_id)
    else:
        logger.warning("Notificação FAILED (compra %s): %s", order_id, reason)

    NOTIFICATIONS_PROCESSED.labels(status=status).inc()
    PROCESSING_DURATION.observe(time.perf_counter() - start)
    return notification


async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
    """
    Chamada pelo aio-pika para cada mensagem da fila.

    message.process() confirma a mensagem (ack) quando o bloco termina sem erros. Se rebentar uma
    exceção inesperada, a mensagem é devolvida à fila (requeue) para ser tentada de novo. Se o
    serviço morrer a meio, o RabbitMQ também a volta a entregar, porque nunca recebeu o ack.
    """
    async with message.process(requeue=True):
        await process_notification(message.body)


# ---------------------------------------------------------------------------
# RabbitMQ
# ---------------------------------------------------------------------------

async def keep_consuming(app: FastAPI) -> None:
    """
    Liga ao RabbitMQ em segundo plano (tentando de novo até conseguir) e começa a consumir a fila.
    Depois da primeira ligação, o connect_robust volta a ligar e a consumir sozinho se o RabbitMQ reiniciar.
    """
    while True:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL, reconnect_interval=RABBITMQ_RETRY_SECONDS)
            channel = await connection.channel()
            # No máximo PREFETCH_COUNT mensagens entregues e ainda sem ack de cada vez
            await channel.set_qos(prefetch_count=PREFETCH_COUNT)
            # As mesmas declarações do Order (exchange direct e fila duráveis): não importa qual dos
            # dois arranca primeiro. Declarar o que já existe não faz nada.
            exchange = await channel.declare_exchange(ORDERS_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True)
            queue = await channel.declare_queue(ORDER_QUEUE, durable=True)
            # A fila recebe o que o Order publica em orders.exchange com a routing key order.created.
            # Continua também a receber o que chega pela exchange por omissão (o Order da Parte I),
            # porque todas as filas estão sempre ligadas a essa exchange pelo próprio nome.
            await queue.bind(exchange, routing_key=ORDER_ROUTING_KEY)
            await queue.consume(on_message)  # no_ack=False por omissão: ack manual
            app.state.amqp_connection = connection
            app.state.amqp_channel = channel
            logger.info(
                "A consumir a fila '%s', ligada à exchange '%s' com a routing key '%s' (até %d mensagens em paralelo)",
                ORDER_QUEUE, ORDERS_EXCHANGE, ORDER_ROUTING_KEY, PREFETCH_COUNT,
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


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.delivery_log = setup_delivery_log(NOTIFICATIONS_LOG_PATH)
    logger.info("Log de entregas em %s", NOTIFICATIONS_LOG_PATH)

    app.state.amqp_connection = None
    app.state.amqp_channel = None
    consumer_task = asyncio.create_task(keep_consuming(app))

    yield

    consumer_task.cancel()
    with suppress(asyncio.CancelledError):
        await consumer_task
    if app.state.amqp_connection is not None:
        await app.state.amqp_connection.close()
    for handler in list(app.state.delivery_log.handlers):
        handler.close()


app = FastAPI(
    title="TicketMEI - Notification",
    description="Consome OrderCreated do RabbitMQ e envia (simula) o email de confirmação.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    """Estado do serviço. Responde 200 mesmo sem RabbitMQ (o campo "rabbitmq" mostra a ligação)."""
    return {"status": "ok", "rabbitmq": "connected" if rabbitmq_is_connected(app) else "disconnected"}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Expõe as métricas para o Prometheus."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
