"""
TicketMEI - Saga orquestrada da compra (Parte II, Fase 2)

O Order é o orquestrador: decide a ordem dos passos e, se um passo falhar, desfaz os anteriores
(compensação). Nenhum outro serviço sabe que a saga existe: o Inventory e o Payment só recebem
pedidos normais.

    Passo         Pedido                                   Compensação
    1. Reservar   POST /inventory/reserve  (Inventory)     POST /inventory/release
    2. Cobrar     POST /payments           (Payment)       - (é o último passo que pode falhar)
    3. Confirmar  compra CONFIRMED         (Order)         -

    Sem bilhetes (409) ou evento sem stock (404)  -> compra FAILED, nada a compensar
    Inventory indisponível                       -> compra FAILED; liberta por segurança (ver abaixo)
    Pagamento falhou ou circuito aberto          -> liberta os bilhetes, compra CANCELLED

Registo (tabela `sagas`, no mesmo SQLite das compras): uma linha por compra, gravada antes e depois
de cada passo, com o passo em que vai, se já reservou, se já pagou e o resultado. Serve para:
    - saber onde ficou uma saga se o Order reiniciar a meio (recover_interrupted, no arranque);
    - voltar a tentar as compensações que falharam (retry_failed_compensations, em segundo plano);
    - as contagens de GET /orders/stats, usadas para validar o teste k6 das sagas.

Idempotência: o Inventory e o Payment tratam um pedido repetido com o mesmo order_id como o mesmo
pedido. Por isso o Order pode repetir uma reserva, um pagamento ou uma libertação sem descontar,
cobrar ou repor duas vezes. É isto que torna seguro "libertar por segurança" quando a resposta de uma
reserva se perdeu (timeout): se a reserva não chegou a acontecer, o release responde released=false.

Se INVENTORY_URL estiver vazio (Inventory ainda não existe), o passo 1 não acontece e a saga fica só
com o pagamento e a confirmação, como na Fase 1: um pagamento falhado deixa a compra FAILED.
"""

import asyncio
import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Awaitable, Callable

import aiohttp
from prometheus_client import Counter, Histogram

from circuit_breaker import full_jitter_delay

logger = logging.getLogger("order")


# ---------------------------------------------------------------------------
# Registo das sagas (SQLite)
# ---------------------------------------------------------------------------

SAGA_SCHEMA = """
CREATE TABLE IF NOT EXISTS sagas (
    order_id    TEXT PRIMARY KEY,
    status      TEXT NOT NULL CHECK (status IN
                    ('RUNNING', 'SUCCEEDED', 'ABORTED', 'COMPENSATED', 'COMPENSATION_FAILED')),
    step        TEXT NOT NULL CHECK (step IN ('RESERVE', 'PAY', 'CONFIRM', 'RELEASE', 'DONE')),
    reserved    INTEGER NOT NULL DEFAULT 0,   -- 1: o Inventory pode ter bilhetes reservados para a compra
    paid        INTEGER NOT NULL DEFAULT 0,   -- 1: o Payment aceitou o pagamento
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    error       TEXT
)
"""

SAGA_STATUSES = ("RUNNING", "SUCCEEDED", "ABORTED", "COMPENSATED", "COMPENSATION_FAILED")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def start_saga(conn: sqlite3.Connection, order_id: str, first_step: str) -> None:
    conn.execute(
        "INSERT INTO sagas (order_id, status, step, started_at) VALUES (?, 'RUNNING', ?, ?)",
        (order_id, first_step, now_iso()),
    )


def update_saga(conn: sqlite3.Connection, order_id: str, **fields) -> None:
    """Grava o progresso da saga (passo, reserved, paid, error) antes de avançar."""
    columns = ", ".join(f"{name} = ?" for name in fields)
    conn.execute(f"UPDATE sagas SET {columns} WHERE order_id = ?", (*fields.values(), order_id))


def finish_saga(conn: sqlite3.Connection, order_id: str, saga_status: str, order_status: str | None,
                error: str | None = None) -> None:
    """Fecha a saga e muda o estado da compra na mesma transação: os dois ficam sempre de acordo."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE sagas SET status = ?, step = 'DONE', finished_at = ?, error = COALESCE(?, error) "
            "WHERE order_id = ?",
            (saga_status, now_iso(), error, order_id),
        )
        if order_status is not None:
            conn.execute("UPDATE orders SET status = ? WHERE id = ?", (order_status, order_id))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def find_saga(conn: sqlite3.Connection, order_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM sagas WHERE order_id = ?", (order_id,)).fetchone()
    return dict(row) if row else None


def count_sagas(conn: sqlite3.Connection) -> dict[str, int]:
    counts = dict.fromkeys(SAGA_STATUSES, 0)
    for row in conn.execute("SELECT status, COUNT(*) AS n FROM sagas GROUP BY status"):
        counts[row["status"]] = row["n"]
    return counts


# ---------------------------------------------------------------------------
# Métricas (nomes do enunciado)
# ---------------------------------------------------------------------------

SAGA_INITIATED = Counter("saga_transactions_initiated_total", "Sagas de compra iniciadas")
SAGA_SUCCESS = Counter("saga_transactions_success_total", "Sagas que terminaram com a compra CONFIRMED")
SAGA_ABORTED = Counter(
    "saga_transactions_aborted_total",
    "Sagas que não terminaram em CONFIRMED (sem bilhetes, Inventory indisponível, pagamento falhado ou circuito aberto)",
)
SAGA_COMPENSATIONS = Counter(
    "saga_compensations_executed_total",
    "Compensações (libertar os bilhetes reservados) por resultado: success, ou failed se o Inventory não "
    "respondeu em nenhuma tentativa (o Order volta a tentar em segundo plano e, se conseguir, conta success)",
    ["outcome"],
)
for _outcome in ("success", "failed"):
    SAGA_COMPENSATIONS.labels(outcome=_outcome)

SAGA_DURATION = Histogram(
    "saga_duration_seconds",
    "Duração das sagas, por resultado: success (CONFIRMED), compensated (houve libertação dos bilhetes) "
    "ou aborted (falhou sem nada a compensar)",
    ["outcome"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
for _outcome in ("success", "compensated", "aborted"):
    SAGA_DURATION.labels(outcome=_outcome)


# ---------------------------------------------------------------------------
# Inventory (cliente HTTP)
# ---------------------------------------------------------------------------

class InventoryError(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class NotEnoughTicketsError(InventoryError):
    """409: o evento não tem bilhetes suficientes. Não ficou nada reservado."""


class UnknownEventError(InventoryError):
    """404: o Inventory não tem stock definido para este evento. Não ficou nada reservado."""


class InventoryUnavailableError(InventoryError):
    """5xx, timeout, erro de ligação ou resposta inesperada: não se sabe se a reserva ficou feita."""


class InventoryClient:
    """Os dois pedidos que a saga faz ao Inventory. Ambos são idempotentes pelo order_id."""

    def __init__(self, session: aiohttp.ClientSession, base_url: str):
        self.session = session
        self.base_url = base_url.rstrip("/")

    async def _post(self, path: str, body: dict) -> tuple[int, dict]:
        try:
            async with self.session.post(f"{self.base_url}{path}", json=body) as response:
                try:
                    payload = await response.json(content_type=None)
                except ValueError:
                    payload = {}
                return response.status, payload if isinstance(payload, dict) else {}
        except asyncio.TimeoutError:
            raise InventoryUnavailableError("timeout") from None
        except aiohttp.ClientError as exc:
            raise InventoryUnavailableError(f"erro de ligação ({type(exc).__name__})") from None

    async def reserve(self, order_id: str, event_id: str, quantity: int) -> dict:
        status, payload = await self._post(
            "/inventory/reserve", {"order_id": order_id, "event_id": event_id, "quantity": quantity}
        )
        if status == 200:
            return payload
        if status == 409:
            raise NotEnoughTicketsError("sem bilhetes suficientes")
        if status == 404:
            raise UnknownEventError(f"o evento '{event_id}' não tem stock no Inventory")
        raise InventoryUnavailableError(f"HTTP {status}")

    async def release(self, order_id: str) -> bool:
        """Liberta os bilhetes da compra. Devolve True se havia uma reserva para libertar."""
        status, payload = await self._post("/inventory/release", {"order_id": order_id})
        if status == 200:
            return bool(payload.get("released", False))
        raise InventoryUnavailableError(f"HTTP {status}")



# ---------------------------------------------------------------------------
# Passo do pagamento: o Order (main.py) liga aqui o circuit breaker
# ---------------------------------------------------------------------------

class PaymentStepError(Exception):
    """O passo de pagamento falhou."""


class PaymentUnavailableError(PaymentStepError):
    """O pagamento nem foi tentado (no Order: o circuit breaker está aberto)."""


class PaymentFailedError(PaymentStepError):
    """O Payment recusou ou falhou em todas as tentativas."""


Charge = Callable[[object], Awaitable[object]]


# ---------------------------------------------------------------------------
# O orquestrador
# ---------------------------------------------------------------------------

class SagaOutcome(str, Enum):
    """Como terminou a saga. O Order traduz isto para a resposta HTTP."""

    CONFIRMED = "CONFIRMED"
    NOT_ENOUGH_TICKETS = "NOT_ENOUGH_TICKETS"
    UNKNOWN_EVENT = "UNKNOWN_EVENT"
    INVENTORY_UNAVAILABLE = "INVENTORY_UNAVAILABLE"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    PAYMENT_UNAVAILABLE = "PAYMENT_UNAVAILABLE"


@dataclass
class SagaResult:
    outcome: SagaOutcome
    order_status: str  # CONFIRMED, FAILED ou CANCELLED
    reason: str = ""


class PurchaseSaga:
    """
    Orquestra a compra: reservar -> cobrar -> confirmar, e libertar os bilhetes se o pagamento falhar.

    conn        ligação ao SQLite do Order (tabelas orders e sagas)
    inventory   cliente do Inventory, ou None se o Inventory não existir (sem passo de reserva)
    charge      função que cobra a compra (no Order: o circuit breaker), ou None sem Payment
    """

    def __init__(self, conn: sqlite3.Connection, inventory: InventoryClient | None, charge: Charge | None,
                 compensation_attempts: int = 3, compensation_base_seconds: float = 0.1):
        self.conn = conn
        self.inventory = inventory
        self.charge = charge
        self.compensation_attempts = compensation_attempts
        self.compensation_base_seconds = compensation_base_seconds

    async def run(self, order) -> SagaResult:
        """Corre a saga de uma compra já gravada como PENDING. Nunca levanta exceções de negócio."""
        started = time.perf_counter()
        start_saga(self.conn, order.id, "RESERVE" if self.inventory else "PAY")
        SAGA_INITIATED.inc()

        # Passo 1: reservar os bilhetes
        if self.inventory is not None:
            try:
                await self.inventory.reserve(order.id, order.event_id, order.quantity)
            except NotEnoughTicketsError as exc:
                return self._abort(order, started, SagaOutcome.NOT_ENOUGH_TICKETS, exc.reason)
            except UnknownEventError as exc:
                return self._abort(order, started, SagaOutcome.UNKNOWN_EVENT, exc.reason)
            except InventoryUnavailableError as exc:
                # Não se sabe se a reserva ficou feita (a resposta pode ter-se perdido). Como o release é
                # idempotente, liberta-se por segurança: se não havia reserva, não muda nada.
                logger.warning("Saga %s: Inventory indisponível na reserva (%s); a libertar por segurança",
                               order.id, exc.reason)
                update_saga(self.conn, order.id, reserved=1, step="RELEASE", error=f"reserva: {exc.reason}")
                return await self._compensate_and_finish(
                    order, started, SagaOutcome.INVENTORY_UNAVAILABLE, "FAILED", exc.reason)
            update_saga(self.conn, order.id, reserved=1, step="PAY")

        # Passo 2: cobrar
        if self.charge is not None:
            try:
                await self.charge(order)
            except PaymentStepError as exc:
                outcome = (SagaOutcome.PAYMENT_UNAVAILABLE if isinstance(exc, PaymentUnavailableError)
                           else SagaOutcome.PAYMENT_FAILED)
                reason = str(exc) or "circuito aberto"
                if self.inventory is None:
                    # Sem Inventory não há nada reservado: a saga aborta sem compensação (como na Fase 1)
                    return self._abort(order, started, outcome, reason)
                update_saga(self.conn, order.id, step="RELEASE", error=f"pagamento: {reason}")
                return await self._compensate_and_finish(order, started, outcome, "CANCELLED", reason)
            update_saga(self.conn, order.id, paid=1, step="CONFIRM")

        # Passo 3: confirmar
        finish_saga(self.conn, order.id, "SUCCEEDED", "CONFIRMED")
        SAGA_SUCCESS.inc()
        SAGA_DURATION.labels(outcome="success").observe(time.perf_counter() - started)
        return SagaResult(SagaOutcome.CONFIRMED, "CONFIRMED")

    def _abort(self, order, started: float, outcome: SagaOutcome, reason: str) -> SagaResult:
        """A saga falha sem nada para desfazer."""
        finish_saga(self.conn, order.id, "ABORTED", "FAILED", error=reason)
        SAGA_ABORTED.inc()
        SAGA_DURATION.labels(outcome="aborted").observe(time.perf_counter() - started)
        logger.info("Saga %s abortada (%s): %s", order.id, outcome.value, reason)
        return SagaResult(outcome, "FAILED", reason)

    async def _compensate_and_finish(self, order, started: float, outcome: SagaOutcome, order_status: str,
                                     reason: str) -> SagaResult:
        """A saga falha depois da reserva: liberta os bilhetes e fecha a saga."""
        released = await self.release_tickets(order.id, self.compensation_attempts)
        saga_status = "COMPENSATED" if released is not None else "COMPENSATION_FAILED"
        finish_saga(self.conn, order.id, saga_status, order_status)
        SAGA_COMPENSATIONS.labels(outcome="success" if released is not None else "failed").inc()
        SAGA_ABORTED.inc()
        SAGA_DURATION.labels(outcome="compensated").observe(time.perf_counter() - started)
        return SagaResult(outcome, order_status, reason)

    async def release_tickets(self, order_id: str, attempts: int) -> bool | None:
        """
        Compensação: POST /inventory/release, com retry e backoff com jitter.

        Devolve True/False (havia ou não uma reserva para libertar) ou None se o Inventory não
        respondeu em nenhuma tentativa. Nesse caso a saga fica COMPENSATION_FAILED e o Order volta a
        tentar em segundo plano (retry_failed_compensations).
        """
        for attempt in range(1, attempts + 1):
            try:
                released = await self.inventory.release(order_id)
            except InventoryUnavailableError as exc:
                logger.warning("Saga %s: compensação falhou (tentativa %d de %d): %s",
                               order_id, attempt, attempts, exc.reason)
                if attempt < attempts:
                    await asyncio.sleep(full_jitter_delay(attempt, self.compensation_base_seconds))
                continue
            logger.info("Saga %s compensada: bilhetes libertados no Inventory (released=%s)", order_id, released)
            return released
        logger.error("Saga %s: compensação falhou em todas as tentativas; fica COMPENSATION_FAILED e o Order "
                     "volta a tentar em segundo plano", order_id)
        return None

    def recover_interrupted(self) -> int:
        """
        No arranque (antes de aceitar pedidos): fecha as sagas que ficaram a meio porque o Order parou.

        - Já pagou (paid=1): só faltava confirmar, por isso termina para a frente (CONFIRMED).
        - Ainda não pagou: desfaz para trás. Com Inventory, fica COMPENSATION_FAILED para a libertação
          ser feita em segundo plano; sem Inventory, não há nada a libertar (ABORTED).
        Devolve quantas sagas fechou.
        """
        rows = self.conn.execute("SELECT * FROM sagas WHERE status = 'RUNNING'").fetchall()
        for row in rows:
            order_id = row["order_id"]
            if row["paid"]:
                finish_saga(self.conn, order_id, "SUCCEEDED", "CONFIRMED", error="terminada no arranque")
                logger.warning("Saga %s interrompida depois do pagamento: compra CONFIRMED no arranque", order_id)
            elif self.inventory is not None:
                order_status = "CANCELLED" if row["reserved"] else "FAILED"
                update_saga(self.conn, order_id, reserved=1, step="RELEASE")
                finish_saga(self.conn, order_id, "COMPENSATION_FAILED", order_status,
                            error="interrompida: o Order reiniciou a meio")
                logger.warning("Saga %s interrompida antes do pagamento: compra %s, bilhetes a libertar",
                               order_id, order_status)
            else:
                finish_saga(self.conn, order_id, "ABORTED", "FAILED", error="interrompida: o Order reiniciou a meio")
                logger.warning("Saga %s interrompida antes do pagamento: compra FAILED", order_id)
        # Compras PENDING sem saga (o Order parou entre gravar a compra e começar a saga)
        orphans = self.conn.execute(
            "UPDATE orders SET status = 'FAILED' WHERE status = 'PENDING' "
            "AND id NOT IN (SELECT order_id FROM sagas)"
        ).rowcount
        return len(rows) + orphans

    async def retry_failed_compensations(self) -> int:
        """Volta a tentar (uma vez cada) as libertações que falharam. Devolve quantas conseguiu."""
        if self.inventory is None:
            return 0
        rows = self.conn.execute("SELECT order_id FROM sagas WHERE status = 'COMPENSATION_FAILED'").fetchall()
        done = 0
        for row in rows:
            released = await self.release_tickets(row["order_id"], attempts=1)
            if released is None:
                continue
            finish_saga(self.conn, row["order_id"], "COMPENSATED", None)
            SAGA_COMPENSATIONS.labels(outcome="success").inc()
            done += 1
        return done
