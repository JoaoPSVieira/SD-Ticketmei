"""
TicketMEI - serviço Inventory

Guarda o stock de bilhetes de cada evento. O Order reserva os bilhetes aqui antes de cobrar (passo 1
da Saga) e liberta-os se o pagamento falhar (compensação). É o único serviço com acesso ao stock.

Endpoints:
    POST /inventory/reserve      -> reserva bilhetes para uma compra: 200, 409 sem bilhetes, 404 evento sem stock
    POST /inventory/release      -> liberta os bilhetes de uma compra (compensação): {"released": true/false}
    GET  /inventory/{event_id}   -> stock de um evento (404 se não tiver stock definido)
    POST /inventory/stock        -> define o total de um evento, repõe o disponível e apaga as reservas desse
                                    evento (prepara os testes)
    GET  /health                 -> estado do serviço
    GET  /metrics                -> métricas no formato Prometheus

Modelo de dados (SQLite):
    stock        (event_id, total_tickets, available_tickets)
    reservations (order_id, event_id, quantity, status RESERVED ou RELEASED, created_at)

Reserva atómica (o centro do RF3): numa só transação,
    UPDATE stock SET available_tickets = available_tickets - :q WHERE event_id = :e AND available_tickets >= :q
Se não mudar nenhuma linha, não há bilhetes (409). Com um só processo a escrever e a transação, nunca
há overbooking, nem com 1000 pedidos ao mesmo tempo. A base de dados também recusa stock negativo.
A reserva é idempotente pelo order_id: repetir a mesma compra não desconta outra vez.

Libertação (compensação da Saga), também idempotente: só a primeira repõe os bilhetes.
Libertação antes da reserva: quando a reserva demora mais do que o timeout do Order (1 s), o Order
desiste e liberta "por segurança". Se a libertação chegar primeiro, fica registada (RELEASED, sem
bilhetes), e a reserva atrasada, quando chegar, é recusada com 409 {"error": "Order already released"}
sem descontar. Sem esta regra, os bilhetes ficavam presos numa compra que o Order já deu como FAILED.

Stock inicial: no arranque, se a tabela estiver vazia, carrega data/stock.json (a lotação de cada
evento no Catalog). Depois de um reinício vale o que está na base de dados.

Métricas:
    inventory_reservations_total          Counter    labels: result (reserved, insufficient, unknown_event,
                                                               duplicate, already_released)
    inventory_releases_total              Counter    labels: result (released, noop)
    inventory_available_tickets           Gauge      labels: event_id
    inventory_requests_total              Counter    labels: endpoint, http_status
    inventory_request_duration_seconds    Histogram  labels: endpoint

Um só processo (WEB_CONCURRENCY=1): as escritas no SQLite ficam em fila num só processo, o que é
simples e seguro. O serviço recusa arrancar com mais. As 3 réplicas só chegam com o Raft (B4).
"""

import logging
import os
import sqlite3
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
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

# Modo do diário do SQLite: WAL no cluster local; DELETE no global, porque o WAL não funciona bem
# em discos de rede (o nfs-csi do cluster da UMa)
SQLITE_JOURNAL_MODE = os.getenv("SQLITE_JOURNAL_MODE", "WAL")
JOURNAL_MODES = {"WAL", "DELETE", "TRUNCATE", "PERSIST"}


def check_single_process(env: Mapping[str, str]) -> None:
    """Recusa arrancar com vários processos (ver "Um só processo" no início do ficheiro)."""
    if env.get("WEB_CONCURRENCY", "1").strip() != "1":
        raise RuntimeError(
            "O Inventory tem de correr com um só processo (WEB_CONCURRENCY=1): as escritas no SQLite ficam "
            "em fila num só processo. As réplicas só chegam com o Raft."
        )


# ---------------------------------------------------------------------------
# Modelo de dados
# ---------------------------------------------------------------------------

class StockRequest(BaseModel):
    """Corpo de POST /inventory/stock, e cada entrada do data/stock.json."""

    event_id: str = Field(min_length=1, max_length=100, examples=["evt-101"])
    total_tickets: int = Field(ge=0, examples=[20])


class ReserveRequest(BaseModel):
    """Corpo de POST /inventory/reserve, enviado pelo Order no passo 1 da Saga."""

    order_id: str = Field(min_length=1, max_length=100)
    event_id: str = Field(min_length=1, max_length=100, examples=["evt-101"])
    quantity: int = Field(ge=1, examples=[2])


class ReleaseRequest(BaseModel):
    """Corpo de POST /inventory/release, enviado pelo Order na compensação da Saga."""

    order_id: str = Field(min_length=1, max_length=100)


class Reservation(BaseModel):
    """Resposta de uma reserva feita: os bilhetes da compra e quantos ficaram disponíveis no evento."""

    order_id: str
    event_id: str
    quantity: int
    available_tickets: int


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

RESERVATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS reservations (
    order_id   TEXT PRIMARY KEY,
    -- event_id NULL e quantity 0: libertação que chegou antes da reserva (ver release)
    event_id   TEXT,
    quantity   INTEGER NOT NULL CHECK (quantity >= 0),
    status     TEXT NOT NULL CHECK (status IN ('RESERVED', 'RELEASED')),
    created_at TEXT NOT NULL
)
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
    conn = sqlite3.connect(path, timeout=5, isolation_level=None)  # sem transações implícitas: usa-se transaction()
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA journal_mode={mode}")
    if mode == "WAL":
        conn.execute("PRAGMA synchronous=NORMAL")  # seguro em WAL e muito mais rápido
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(SCHEMA)
    conn.execute(RESERVATIONS_SCHEMA)
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
    """
    Define o total de um evento (cria-o se não existir), repõe o disponível e apaga as reservas desse
    evento, para uma libertação antiga não pôr o disponível acima do total. Chamar dentro de transaction().
    """
    conn.execute("DELETE FROM reservations WHERE event_id = ?", (event_id,))
    conn.execute(
        "INSERT INTO stock (event_id, total_tickets, available_tickets) VALUES (:e, :t, :t)"
        " ON CONFLICT (event_id) DO UPDATE SET total_tickets = :t, available_tickets = :t",
        {"e": event_id, "t": total_tickets},
    )
    return Stock(event_id=event_id, total_tickets=total_tickets, available_tickets=total_tickets)


def reserve(conn: sqlite3.Connection, order_id: str, event_id: str, quantity: int) -> tuple[str, Reservation | None]:
    """
    Reserva bilhetes para uma compra. Devolve (resultado, reserva):
        reserved          reservou agora
        duplicate         a compra já tinha reserva: devolve-a sem descontar outra vez (retry do Order)
        already_released  a compra já foi libertada (a libertação chegou antes desta reserva): nada reservado
        insufficient      não há bilhetes suficientes (nada reservado)
        unknown_event     o evento não tem stock definido (nada reservado)

    Tudo numa transação: o desconto no stock e o registo da reserva ficam gravados juntos ou nada.
    Sem isso, um processo que morresse entre os dois deixava bilhetes descontados sem dono.
    """
    with transaction(conn):
        previous = conn.execute("SELECT * FROM reservations WHERE order_id = ?", (order_id,)).fetchone()
        if previous is not None and previous["status"] == "RELEASED":
            return "already_released", None
        if previous is not None:
            left = find_stock(conn, previous["event_id"]).available_tickets
            return "duplicate", Reservation(order_id=order_id, event_id=previous["event_id"],
                                            quantity=previous["quantity"], available_tickets=left)

        taken = conn.execute(
            "UPDATE stock SET available_tickets = available_tickets - :q"
            " WHERE event_id = :e AND available_tickets >= :q",
            {"q": quantity, "e": event_id},
        ).rowcount
        if not taken:
            return ("insufficient" if find_stock(conn, event_id) else "unknown_event"), None

        conn.execute(
            "INSERT INTO reservations (order_id, event_id, quantity, status, created_at)"
            " VALUES (?, ?, ?, 'RESERVED', ?)",
            (order_id, event_id, quantity, datetime.now(timezone.utc).isoformat(timespec="milliseconds")),
        )
        left = find_stock(conn, event_id).available_tickets
    return "reserved", Reservation(order_id=order_id, event_id=event_id, quantity=quantity, available_tickets=left)


def release(conn: sqlite3.Connection, order_id: str) -> str | None:
    """
    Liberta os bilhetes de uma compra. Devolve o evento onde repôs os bilhetes, ou None se não havia
    nada para repor (a compra já tinha sido libertada, ou nunca reservou).

    Se a compra ainda não tem reserva, fica registada como RELEASED (sem evento nem bilhetes), para a
    reserva dessa compra ser recusada se chegar depois.
    """
    with transaction(conn):
        reservation = conn.execute("SELECT * FROM reservations WHERE order_id = ?", (order_id,)).fetchone()
        if reservation is None:
            conn.execute(
                "INSERT INTO reservations (order_id, event_id, quantity, status, created_at)"
                " VALUES (?, NULL, 0, 'RELEASED', ?)",
                (order_id, datetime.now(timezone.utc).isoformat(timespec="milliseconds")),
            )
            return None
        if reservation["status"] == "RELEASED":
            return None
        conn.execute("UPDATE reservations SET status = 'RELEASED' WHERE order_id = ?", (order_id,))
        conn.execute(
            "UPDATE stock SET available_tickets = available_tickets + ? WHERE event_id = ?",
            (reservation["quantity"], reservation["event_id"]),
        )
    return reservation["event_id"]


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
# Métricas Prometheus
# ---------------------------------------------------------------------------

RESERVATIONS = Counter(
    "inventory_reservations_total",
    "Pedidos de reserva por resultado (duplicate: a mesma compra repetida; already_released: a reserva "
    "chegou depois da libertação da compra)",
    ["result"],
)
RELEASES = Counter(
    "inventory_releases_total",
    "Pedidos de libertação por resultado (released: repôs bilhetes; noop: não havia nada para repor)",
    ["result"],
)
# Cria as séries a zero logo no arranque, para os gráficos mostrarem 0 em vez de "No data"
for _result in ("reserved", "insufficient", "unknown_event", "duplicate", "already_released"):
    RESERVATIONS.labels(result=_result)
for _result in ("released", "noop"):
    RELEASES.labels(result=_result)

AVAILABLE = Gauge("inventory_available_tickets", "Bilhetes disponíveis por evento", ["event_id"])

REQUESTS_TOTAL = Counter(
    "inventory_requests_total",
    "Total de pedidos HTTP processados pelo Inventory",
    ["endpoint", "http_status"],
)
REQUEST_DURATION = Histogram(
    "inventory_request_duration_seconds",
    "Duração dos pedidos HTTP no Inventory, em segundos",
    ["endpoint"],
    # O Order desiste ao fim de 1 s (INVENTORY_TIMEOUT_SECONDS): os intervalos concentram-se abaixo disso
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

UNINSTRUMENTED_PATHS = {"/metrics", "/health"}


class MetricsMiddleware:
    """Mede todos os pedidos HTTP (o mesmo desenho dos outros serviços)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] in UNINSTRUMENTED_PATHS:
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
            # Template da rota ("/inventory/{event_id}"), para cada evento não criar uma série nova
            endpoint = getattr(scope.get("route"), "path", None) or "unmatched"
            REQUESTS_TOTAL.labels(endpoint=endpoint, http_status=str(status_code)).inc()
            REQUEST_DURATION.labels(endpoint=endpoint).observe(time.perf_counter() - start)


# ---------------------------------------------------------------------------
# Aplicação
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    check_single_process(os.environ)

    app.state.db = open_database(INVENTORY_DB_PATH, SQLITE_JOURNAL_MODE)
    logger.info("Base de dados em %s (journal_mode=%s)", INVENTORY_DB_PATH, SQLITE_JOURNAL_MODE)
    loaded = load_initial_stock(app.state.db, STOCK_FILE)
    if loaded:
        logger.info("Stock inicial: %d eventos carregados de %s", loaded, STOCK_FILE)
    for row in app.state.db.execute("SELECT event_id, available_tickets FROM stock"):
        AVAILABLE.labels(event_id=row["event_id"]).set(row["available_tickets"])
    yield
    app.state.db.close()


app = FastAPI(
    title="TicketMEI - Inventory",
    description="Stock de bilhetes de cada evento: reservas atómicas e libertação (compensação da Saga).",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(MetricsMiddleware)


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    """
    422 igual ao do FastAPI, mas sem o campo "input" (o valor recebido). O JSON do Python aceita
    Infinity e NaN, que não se conseguem escrever na resposta: com o "input" dava 500 em vez de 422.
    """
    errors = [{key: value for key, value in err.items() if key != "input"} for err in exc.errors()]
    return JSONResponse(status_code=422, content={"detail": jsonable_encoder(errors)})


@app.post(
    "/inventory/reserve",
    response_model=Reservation,
    responses={
        404: {"description": "O evento não tem stock definido"},
        409: {"description": '{"error": "Not enough tickets"} ou {"error": "Order already released"}'},
    },
)
async def reserve_tickets(request: ReserveRequest) -> Response:
    """Reserva bilhetes para uma compra (passo 1 da Saga). Idempotente pelo order_id."""
    result, reservation = reserve(app.state.db, request.order_id, request.event_id, request.quantity)
    RESERVATIONS.labels(result=result).inc()
    if result == "insufficient":
        return JSONResponse(status_code=409, content={"error": "Not enough tickets"})
    if result == "already_released":
        return JSONResponse(status_code=409, content={"error": "Order already released"})
    if result == "unknown_event":
        return JSONResponse(status_code=404, content={"detail": f"Evento '{request.event_id}' sem stock definido"})
    AVAILABLE.labels(event_id=reservation.event_id).set(reservation.available_tickets)
    return JSONResponse(content=reservation.model_dump())


@app.post("/inventory/release")
async def release_tickets(request: ReleaseRequest) -> dict:
    """Liberta os bilhetes de uma compra (compensação da Saga). Idempotente: responde sempre 200."""
    event_id = release(app.state.db, request.order_id)
    RELEASES.labels(result="released" if event_id else "noop").inc()
    if event_id:
        AVAILABLE.labels(event_id=event_id).set(find_stock(app.state.db, event_id).available_tickets)
    return {"released": event_id is not None}


@app.get("/inventory/{event_id}", response_model=Stock, responses={404: {"description": "Evento sem stock definido"}})
async def get_stock(event_id: str) -> Response:
    """Stock de um evento."""
    stock = find_stock(app.state.db, event_id)
    if stock is None:
        return JSONResponse(status_code=404, content={"detail": f"Evento '{event_id}' sem stock definido"})
    return JSONResponse(content=stock.model_dump())


@app.post("/inventory/stock", response_model=Stock)
async def define_stock(request: StockRequest) -> Stock:
    """Define o total de bilhetes de um evento, repõe o disponível e apaga as reservas desse evento."""
    with transaction(app.state.db):
        stock = set_stock(app.state.db, request.event_id, request.total_tickets)
    AVAILABLE.labels(event_id=stock.event_id).set(stock.available_tickets)
    logger.info("Stock de %s definido: %d bilhetes", request.event_id, request.total_tickets)
    return stock


@app.get("/health")
async def health() -> dict:
    """Indica se o serviço está pronto a responder."""
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Expõe as métricas para o Prometheus."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
