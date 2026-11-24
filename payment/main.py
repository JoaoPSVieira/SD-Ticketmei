"""
TicketMEI - serviço Payment

Simula o sistema de pagamentos (externo) que cobra as compras. O Order chama-o de forma síncrona
(POST /payments) dentro de um circuit breaker.

Endpoints:
    POST /payments                   -> processa o pagamento de uma compra
                                        201 SUCCESS, ou 500 FAILED quando a falha é injetada
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
"""

import asyncio
import logging
import os
import random
import sqlite3
from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
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


def open_database(path: Path) -> sqlite3.Connection:
    """Abre (e cria, se ainda não existir) a base de dados."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5, isolation_level=None)  # isolation_level=None: cada comando é gravado logo
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(SCHEMA)
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


def payment_stats(conn: sqlite3.Connection) -> dict[str, int]:
    counts = {"SUCCESS": 0, "FAILED": 0}
    for row in conn.execute("SELECT status, COUNT(*) AS n FROM payments GROUP BY status"):
        counts[row["status"]] = row["n"]
    return counts


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = open_database(PAYMENTS_DB_PATH)
    logger.info("Base de dados em %s", PAYMENTS_DB_PATH)

    # A configuração das falhas vive na memória deste processo (por isso o serviço corre com um só)
    app.state.faults = faults_from_env(os.environ)
    logger.info("Falhas iniciais: error_rate=%s, latency_seconds=%s",
                app.state.faults.error_rate, app.state.faults.latency_seconds)
    yield
    app.state.db.close()


app = FastAPI(
    title="TicketMEI - Payment",
    description="Simula o processamento dos pagamentos das compras, com falhas e atrasos configuráveis.",
    version="1.0.0",
    lifespan=lifespan,
)


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
    """Processa (simula) o pagamento de uma compra, com as falhas configuradas."""
    faults = app.state.faults

    # 1.º a latência, depois o sorteio da falha: um pagamento lento pode também falhar
    if faults.latency_seconds > 0:
        await asyncio.sleep(faults.latency_seconds)

    if random.random() < faults.error_rate:
        insert_payment(app.state.db, request.order_id, request.amount, "FAILED")
        return JSONResponse(status_code=500, content={"error": "Payment failed", "status": "FAILED"})

    payment = insert_payment(app.state.db, request.order_id, request.amount, "SUCCESS")
    return JSONResponse(status_code=201, content=payment.model_dump(),
                        headers={"Location": f"/payments/{payment.id}"})


@app.post("/payments/configure-faults", response_model=FaultConfig,
          responses={422: {"description": "error_rate fora de 0..1, latency_seconds negativa, ou nenhum campo"}})
async def configure_faults(update: FaultConfigUpdate) -> FaultConfig:
    """Muda as falhas injetadas (só os campos enviados) e devolve a configuração atual."""
    app.state.faults = app.state.faults.model_copy(update=update.model_dump(exclude_none=True))
    logger.info("Falhas configuradas: error_rate=%s, latency_seconds=%s",
                app.state.faults.error_rate, app.state.faults.latency_seconds)
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
