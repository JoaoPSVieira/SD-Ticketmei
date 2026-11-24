"""
TicketMEI - serviço Payment

Simula o sistema de pagamentos (externo) que cobra as compras. O Order chama-o de forma síncrona
(POST /payments) dentro de um circuit breaker.

Endpoints:
    POST /payments       -> processa o pagamento de uma compra -> 201 SUCCESS
    GET  /payments/{id}  -> um pagamento (404 se não existir)
    GET  /health         -> estado do serviço
    GET  /metrics        -> métricas no formato Prometheus

Modelo de dados (tabela payments, SQLite):
    id, order_id, amount, status (SUCCESS ou FAILED), processed_at
"""

import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

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


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = open_database(PAYMENTS_DB_PATH)
    logger.info("Base de dados em %s", PAYMENTS_DB_PATH)
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


@app.post("/payments", status_code=201, response_model=Payment)
async def create_payment(request: PaymentRequest) -> Response:
    """Processa (simula) o pagamento de uma compra."""
    payment = insert_payment(app.state.db, request.order_id, request.amount, "SUCCESS")
    return JSONResponse(status_code=201, content=payment.model_dump(),
                        headers={"Location": f"/payments/{payment.id}"})


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
