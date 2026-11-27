"""
TicketMEI - serviço Inventory

Guarda o stock de bilhetes de cada evento. O Order reserva os bilhetes aqui antes de cobrar (passo 1
da Saga) e liberta-os se o pagamento falhar (compensação). É o único serviço com acesso ao stock.

Endpoints:
    GET  /inventory/{event_id}   -> stock de um evento (404 se não tiver stock definido)
    POST /inventory/stock        -> define o total de um evento e repõe o disponível (prepara os testes)
    GET  /health                 -> estado do serviço
    GET  /metrics                -> métricas no formato Prometheus

Modelo de dados (SQLite):
    stock (event_id, total_tickets, available_tickets)

Stock inicial: no arranque, se a tabela estiver vazia, carrega data/stock.json (a lotação de cada
evento no Catalog). Depois de um reinício vale o que está na base de dados.
"""

import logging
import os
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field, TypeAdapter

# ---------------------------------------------------------------------------
# Configuração (variáveis de ambiente)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [inventory] %(message)s",
)
logger = logging.getLogger("inventory")

# Base de dados do Inventory: um ficheiro SQLite (no contentor fica num volume)
INVENTORY_DB_PATH = Path(os.getenv("INVENTORY_DB_PATH", Path(__file__).parent / "data" / "inventory.db"))
# Stock inicial, carregado só quando a base de dados ainda não tem stock
STOCK_FILE = Path(os.getenv("STOCK_FILE", Path(__file__).parent / "data" / "stock.json"))


# ---------------------------------------------------------------------------
# Modelo de dados
# ---------------------------------------------------------------------------

class StockRequest(BaseModel):
    """Corpo de POST /inventory/stock, e cada entrada do data/stock.json."""

    event_id: str = Field(min_length=1, max_length=100, examples=["evt-101"])
    total_tickets: int = Field(ge=0, examples=[20])


class Stock(BaseModel):
    event_id: str
    total_tickets: int
    available_tickets: int


def load_stock_file(path: Path) -> list[StockRequest]:
    """Lê e valida o stock inicial. Falha no arranque se tiver campos em falta ou eventos repetidos."""
    entries = TypeAdapter(list[StockRequest]).validate_json(path.read_bytes())
    ids = [entry.event_id for entry in entries]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Existem eventos repetidos em {path}")
    return entries


# ---------------------------------------------------------------------------
# Base de dados (SQLite)
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock (
    event_id          TEXT PRIMARY KEY,
    total_tickets     INTEGER NOT NULL CHECK (total_tickets >= 0),
    -- Nunca negativo nem acima do total: a última defesa contra o overbooking
    available_tickets INTEGER NOT NULL CHECK (available_tickets BETWEEN 0 AND total_tickets)
)
"""


def open_database(path: Path) -> sqlite3.Connection:
    """Abre (e cria, se ainda não existir) a base de dados."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5, isolation_level=None)  # sem transações implícitas: usa-se transaction()
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(SCHEMA)
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection):
    """Os comandos dentro do bloco ficam todos gravados, ou nenhum (se houver um erro ou o processo morrer)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


def find_stock(conn: sqlite3.Connection, event_id: str) -> Stock | None:
    row = conn.execute("SELECT * FROM stock WHERE event_id = ?", (event_id,)).fetchone()
    return Stock(**dict(row)) if row else None


def set_stock(conn: sqlite3.Connection, event_id: str, total_tickets: int) -> Stock:
    """Define o total de um evento (cria-o se não existir) e repõe o disponível."""
    conn.execute(
        "INSERT INTO stock (event_id, total_tickets, available_tickets) VALUES (:e, :t, :t)"
        " ON CONFLICT (event_id) DO UPDATE SET total_tickets = :t, available_tickets = :t",
        {"e": event_id, "t": total_tickets},
    )
    return Stock(event_id=event_id, total_tickets=total_tickets, available_tickets=total_tickets)


def load_initial_stock(conn: sqlite3.Connection, path: Path) -> int:
    """Carrega o stock inicial se a tabela estiver vazia. Devolve quantos eventos carregou."""
    if conn.execute("SELECT 1 FROM stock LIMIT 1").fetchone():
        return 0
    entries = load_stock_file(path)
    with transaction(conn):
        for entry in entries:
            set_stock(conn, entry.event_id, entry.total_tickets)
    return len(entries)


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = open_database(INVENTORY_DB_PATH)
    logger.info("Base de dados em %s", INVENTORY_DB_PATH)
    loaded = load_initial_stock(app.state.db, STOCK_FILE)
    if loaded:
        logger.info("Stock inicial: %d eventos carregados de %s", loaded, STOCK_FILE)
    yield
    app.state.db.close()


app = FastAPI(
    title="TicketMEI - Inventory",
    description="Stock de bilhetes de cada evento: reservas atómicas e libertação (compensação da Saga).",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    """
    422 igual ao do FastAPI, mas sem o campo "input" (o valor recebido). O JSON do Python aceita
    Infinity e NaN, que não se conseguem escrever na resposta: com o "input" dava 500 em vez de 422.
    """
    errors = [{key: value for key, value in err.items() if key != "input"} for err in exc.errors()]
    return JSONResponse(status_code=422, content={"detail": jsonable_encoder(errors)})


@app.get("/inventory/{event_id}", response_model=Stock, responses={404: {"description": "Evento sem stock definido"}})
async def get_stock(event_id: str) -> Response:
    """Stock de um evento."""
    stock = find_stock(app.state.db, event_id)
    if stock is None:
        return JSONResponse(status_code=404, content={"detail": f"Evento '{event_id}' sem stock definido"})
    return JSONResponse(content=stock.model_dump())


@app.post("/inventory/stock", response_model=Stock)
async def define_stock(request: StockRequest) -> Stock:
    """Define o total de bilhetes de um evento e repõe o disponível (serve para preparar os testes)."""
    stock = set_stock(app.state.db, request.event_id, request.total_tickets)
    logger.info("Stock de %s definido: %d bilhetes", request.event_id, request.total_tickets)
    return stock


@app.get("/health")
async def health() -> dict:
    """Indica se o serviço está pronto a responder."""
    return {"status": "ok"}
