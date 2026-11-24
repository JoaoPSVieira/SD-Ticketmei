"""
TicketMEI - serviço Payment

Simula o sistema de pagamentos (externo) que cobra as compras. O Order chama-o de forma síncrona
(POST /payments) dentro de um circuit breaker.

Endpoints:
    POST /payments                   -> processa o pagamento de uma compra: 201 SUCCESS, 500 FAILED
                                        (falha injetada), 200 se a compra já estava paga (idempotente)
    POST /payments/configure-faults  -> muda a taxa de erros e/ou a latência -> 200 (422 fora dos limites)
    GET  /payments/configure-faults  -> configuração atual das falhas
    GET  /payments/stats             -> {"SUCCESS": n, "FAILED": n} (validação do teste das sagas)
    GET  /payments/{id}              -> um pagamento (404 se não existir)
    GET  /health                     -> estado do serviço
    GET  /metrics                    -> métricas no formato Prometheus

Falhas injetadas (para testar o circuit breaker do Order):
    Cada POST /payments espera primeiro a latência configurada (asyncio.sleep) e só depois sorteia
    a falha com a taxa configurada. Os valores iniciais vêm de PAYMENT_ERROR_RATE e
    PAYMENT_LATENCY_SECONDS e mudam-se em tempo real com POST /payments/configure-faults.

Modelo de dados (tabela payments, SQLite):
    id, order_id, amount, status (SUCCESS ou FAILED), processed_at

Métricas (só de POST /payments):
    payment_requests_total                Counter    labels: http_status
    payment_processing_duration_seconds   Histogram  (inclui a latência injetada; intervalos até 15 s)
    payment_fault_error_rate              Gauge      taxa de erros configurada
    payment_fault_latency_seconds         Gauge      latência configurada

Um só processo:
    A configuração das falhas vive na memória do processo. Com vários processos, o configure-faults
    só mudava o processo que recebeu o pedido; por isso o serviço recusa arrancar com WEB_CONCURRENCY
    diferente de 1. Para escalar usam-se réplicas.
"""

import asyncio
import logging
import os
import random
import sqlite3
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field, model_validator

# ---------------------------------------------------------------------------
# Configuração (variáveis de ambiente)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [payment] %(message)s",
)
logger = logging.getLogger("payment")

# Base de dados do Payment: um ficheiro SQLite (no contentor fica num volume)
PAYMENTS_DB_PATH = Path(os.getenv("PAYMENTS_DB_PATH", Path(__file__).parent / "data" / "payments.db"))

# Modo do diário do SQLite: WAL no cluster local; DELETE no global, porque o WAL não funciona bem
# em discos de rede (o nfs-csi do cluster da UMa)
SQLITE_JOURNAL_MODE = os.getenv("SQLITE_JOURNAL_MODE", "WAL")
JOURNAL_MODES = {"WAL", "DELETE", "TRUNCATE", "PERSIST"}


def check_single_process(env: Mapping[str, str]) -> None:
    """Recusa arrancar com vários processos (ver "Um só processo" no início do ficheiro)."""
    if env.get("WEB_CONCURRENCY", "1").strip() != "1":
        raise RuntimeError(
            "O Payment tem de correr com um só processo (WEB_CONCURRENCY=1): a configuração das falhas "
            "fica na memória do processo. Para escalar, usar mais réplicas."
        )


# ---------------------------------------------------------------------------
# Modelo de dados
# ---------------------------------------------------------------------------

class PaymentRequest(BaseModel):
    """Corpo de POST /payments. O FastAPI responde 422 automaticamente se não respeitar estas regras."""

    order_id: str = Field(min_length=1, max_length=100, examples=["3f1c..."])
    amount: float = Field(gt=0, allow_inf_nan=False, examples=[170.0])


class Payment(BaseModel):
    """Um pagamento, tal como é guardado e devolvido pela API."""

    id: int
    order_id: str
    amount: float
    status: str  # SUCCESS ou FAILED
    processed_at: str


class FaultConfig(BaseModel):
    """Falhas injetadas em cada POST /payments."""

    # Probabilidade de um pagamento falhar (0 = nunca, 1 = sempre)
    error_rate: float = Field(ge=0, le=1, allow_inf_nan=False)
    # Espera antes de responder, em segundos. allow_inf_nan=False: o JSON do Python aceita
    # "Infinity", e um asyncio.sleep infinito deixaria os pedidos pendurados para sempre.
    latency_seconds: float = Field(ge=0, allow_inf_nan=False)


class FaultConfigUpdate(BaseModel):
    """
    Corpo de POST /payments/configure-faults. Só muda os campos enviados: o k6 envia
    {"error_rate": 0.8} para injetar erros sem mexer na latência, e {0, 0} para repor tudo.
    """

    error_rate: float | None = Field(default=None, ge=0, le=1, allow_inf_nan=False, examples=[0.8])
    latency_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False, examples=[10])

    @model_validator(mode="after")
    def at_least_one_field(self):
        if self.error_rate is None and self.latency_seconds is None:
            raise ValueError("Indique error_rate (0 a 1) e/ou latency_seconds (>= 0)")
        return self


def faults_from_env(env: Mapping[str, str]) -> FaultConfig:
    """
    Configuração inicial das falhas (PAYMENT_ERROR_RATE e PAYMENT_LATENCY_SECONDS, 0 por omissão).
    Um valor inválido levanta ValueError e o serviço não arranca, em vez de arrancar com falhas
    diferentes das pedidas.
    """
    return FaultConfig(
        error_rate=float(env.get("PAYMENT_ERROR_RATE", "0")),
        latency_seconds=float(env.get("PAYMENT_LATENCY_SECONDS", "0")),
    )


# ---------------------------------------------------------------------------
# Base de dados (SQLite)
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id     TEXT NOT NULL,
    amount       REAL NOT NULL CHECK (amount > 0),
    status       TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    processed_at TEXT NOT NULL
)
"""

# Idempotência garantida pela própria base de dados: no máximo um pagamento SUCCESS por compra.
# As tentativas FAILED podem repetir-se (cada retry do Order é uma tentativa nova).
ONE_SUCCESS_PER_ORDER = """
CREATE UNIQUE INDEX IF NOT EXISTS payments_one_success_per_order
    ON payments (order_id) WHERE status = 'SUCCESS'
"""


def open_database(path: Path, journal_mode: str = "WAL") -> sqlite3.Connection:
    """
    Abre (e cria, se ainda não existir) a base de dados.

    - journal_mode: WAL (local) ou DELETE (cluster global, disco NFS). É validado contra uma lista
      fechada antes de entrar no PRAGMA, que não aceita parâmetros.
    - busy_timeout: se o ficheiro estiver ocupado, espera até 5 s em vez de dar erro.
    """
    mode = journal_mode.strip().upper()
    if mode not in JOURNAL_MODES:
        raise ValueError(f"SQLITE_JOURNAL_MODE inválido: {journal_mode!r} (usar um de {sorted(JOURNAL_MODES)})")
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5, isolation_level=None)  # isolation_level=None: cada comando é gravado logo
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA journal_mode={mode}")
    if mode == "WAL":
        conn.execute("PRAGMA synchronous=NORMAL")  # seguro em WAL e muito mais rápido
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(SCHEMA)
    conn.execute(ONE_SUCCESS_PER_ORDER)
    return conn


def insert_payment(conn: sqlite3.Connection, order_id: str, amount: float, status: str) -> Payment:
    processed_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    cursor = conn.execute(
        "INSERT INTO payments (order_id, amount, status, processed_at) VALUES (?, ?, ?, ?)",
        (order_id, amount, status, processed_at),
    )
    return Payment(id=cursor.lastrowid, order_id=order_id, amount=amount, status=status, processed_at=processed_at)


def find_payment(conn: sqlite3.Connection, payment_id: int) -> Payment | None:
    row = conn.execute("SELECT * FROM payments WHERE id = ?", (payment_id,)).fetchone()
    return Payment(**dict(row)) if row else None


def find_successful_payment(conn: sqlite3.Connection, order_id: str) -> Payment | None:
    row = conn.execute("SELECT * FROM payments WHERE order_id = ? AND status = 'SUCCESS'", (order_id,)).fetchone()
    return Payment(**dict(row)) if row else None


def payment_stats(conn: sqlite3.Connection) -> dict[str, int]:
    counts = {"SUCCESS": 0, "FAILED": 0}
    for row in conn.execute("SELECT status, COUNT(*) AS n FROM payments GROUP BY status"):
        counts[row["status"]] = row["n"]
    return counts


# ---------------------------------------------------------------------------
# Métricas Prometheus
# ---------------------------------------------------------------------------

PAYMENT_REQUESTS = Counter(
    "payment_requests_total",
    "Pedidos POST /payments, por código HTTP (201 SUCCESS, 500 falha injetada, 200 compra já paga, 422 inválido)",
    ["http_status"],
)
# Cria as séries principais a zero logo no arranque, para os gráficos mostrarem 0 em vez de "No data"
for _status in ("201", "500"):
    PAYMENT_REQUESTS.labels(http_status=_status)

PROCESSING_DURATION = Histogram(
    "payment_processing_duration_seconds",
    "Duração de cada POST /payments, incluindo a latência injetada, em segundos",
    # Até 15 s, para o atraso de 10 s do teste do circuit breaker cair num intervalo próprio
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 7.5, 10.0, 12.5, 15.0),
)

# Mostram no dashboard quando a falha foi injetada e quando foi reposta
FAULT_ERROR_RATE = Gauge("payment_fault_error_rate", "Taxa de erros injetada nos pagamentos (0 a 1)")
FAULT_LATENCY = Gauge("payment_fault_latency_seconds", "Latência injetada em cada pagamento, em segundos")


def set_faults(faults: FaultConfig) -> None:
    app.state.faults = faults
    FAULT_ERROR_RATE.set(faults.error_rate)
    FAULT_LATENCY.set(faults.latency_seconds)
    logger.info("Falhas: error_rate=%s, latency_seconds=%s", faults.error_rate, faults.latency_seconds)


class MetricsMiddleware:
    """
    Mede os pedidos POST /payments (o mesmo desenho dos outros serviços). Os restantes endpoints
    (configure-faults, stats, consultas) não contam, para o dashboard mostrar só pagamentos.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST" or scope["path"] != "/payments":
            await self.app(scope, receive, send)
            return

        status_code = 500  # se a aplicação rebentar antes de responder

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        start = time.perf_counter()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            PAYMENT_REQUESTS.labels(http_status=str(status_code)).inc()
            PROCESSING_DURATION.observe(time.perf_counter() - start)


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    check_single_process(os.environ)

    app.state.db = open_database(PAYMENTS_DB_PATH, SQLITE_JOURNAL_MODE)
    logger.info("Base de dados em %s (journal_mode=%s)", PAYMENTS_DB_PATH, SQLITE_JOURNAL_MODE)

    # Configuração inicial das falhas, que fica na memória deste processo
    set_faults(faults_from_env(os.environ))
    yield
    app.state.db.close()


app = FastAPI(
    title="TicketMEI - Payment",
    description="Simula o processamento dos pagamentos das compras, com falhas e atrasos configuráveis.",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(MetricsMiddleware)


def error(status_code: int, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail})


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    """
    422 igual ao do FastAPI, mas sem o campo "input" (o valor recebido). O JSON do Python aceita
    Infinity e NaN, que o pydantic rejeita mas que não se conseguem escrever na resposta: com o
    "input", um {"amount": Infinity} dava 500 em vez de 422.
    """
    errors = [{key: value for key, value in err.items() if key != "input"} for err in exc.errors()]
    return JSONResponse(status_code=422, content={"detail": jsonable_encoder(errors)})


@app.post(
    "/payments",
    status_code=201,
    response_model=Payment,
    responses={
        422: {"description": "Pedido inválido (order_id em falta, amount <= 0)"},
        500: {"description": "Falha injetada: o pagamento fica FAILED"},
    },
)
async def create_payment(request: PaymentRequest) -> Response:
    """
    Processa (simula) o pagamento de uma compra, com as falhas configuradas.

    Idempotente pelo order_id: se a compra já foi paga, devolve esse pagamento (200) sem cobrar
    outra vez. É o que impede que um retry do Order, depois de um timeout, cobre duas vezes.
    """
    faults = app.state.faults

    # 1.º a latência, depois o sorteio da falha: um pagamento lento pode também falhar
    if faults.latency_seconds > 0:
        await asyncio.sleep(faults.latency_seconds)

    # Verifica-se depois da espera: um retry pode ter chegado enquanto este pedido esperava.
    # Daqui até ao INSERT não há nenhum await, por isso nenhum outro pedido corre no meio.
    paid = find_successful_payment(app.state.db, request.order_id)
    if paid is not None:
        return JSONResponse(status_code=200, content=paid.model_dump())

    if random.random() < faults.error_rate:
        insert_payment(app.state.db, request.order_id, request.amount, "FAILED")
        return JSONResponse(status_code=500, content={"error": "Payment failed", "status": "FAILED"})

    try:
        payment = insert_payment(app.state.db, request.order_id, request.amount, "SUCCESS")
    except sqlite3.IntegrityError:
        # Só acontece se outro processo pagou a mesma compra entretanto (o índice único recusou)
        paid = find_successful_payment(app.state.db, request.order_id)
        return JSONResponse(status_code=200, content=paid.model_dump())
    return JSONResponse(status_code=201, content=payment.model_dump(),
                        headers={"Location": f"/payments/{payment.id}"})


@app.post("/payments/configure-faults", response_model=FaultConfig,
          responses={422: {"description": "error_rate fora de 0..1, latency_seconds negativa, ou nenhum campo"}})
async def configure_faults(update: FaultConfigUpdate) -> FaultConfig:
    """Muda as falhas injetadas (só os campos enviados) e devolve a configuração atual."""
    set_faults(app.state.faults.model_copy(update=update.model_dump(exclude_none=True)))
    return app.state.faults


@app.get("/payments/configure-faults", response_model=FaultConfig)
async def get_faults() -> FaultConfig:
    """Configuração atual das falhas injetadas."""
    return app.state.faults


# Declarado antes de /payments/{payment_id}, senão "stats" era lido como um id
@app.get("/payments/stats")
async def stats() -> dict[str, int]:
    """Número de pagamentos por estado: {"SUCCESS": n, "FAILED": n}."""
    return payment_stats(app.state.db)


@app.get("/payments/{payment_id}", response_model=Payment, responses={404: {"description": "Pagamento não encontrado"}})
async def get_payment(payment_id: int) -> Response:
    """Devolve um pagamento."""
    payment = find_payment(app.state.db, payment_id)
    if payment is None:
        return error(404, f"Pagamento {payment_id} não encontrado")
    return JSONResponse(content=payment.model_dump())


@app.get("/health")
async def health() -> dict:
    """Indica se o serviço está pronto a responder."""
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Expõe as métricas para o Prometheus."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
