"""
Circuit breaker e retry com jitter, usados pelo Order nas chamadas ao Payment (Parte II, fase 1).

Circuit breaker (disjuntor):
    CLOSED     as chamadas passam; contam-se as falhas seguidas. Ao chegar a `failure_threshold`
               falhas seguidas, o circuito abre.
    OPEN       nenhuma chamada passa: o Order falha logo ("fail-fast"), sem esperar pelo Payment.
               Passados `open_seconds`, o circuito fica semiaberto.
    HALF_OPEN  deixa passar até `half_open_max_calls` chamadas de teste. Se uma correr bem, o circuito
               fecha; se falhar, volta a abrir (e espera outra vez `open_seconds`).

    Uma falha é uma resposta 5xx do Payment, um timeout ou um erro de ligação. Um pedido mais lento do
    que o timeout conta como falha: é assim que o limite de latência abre o circuito.

Retry com jitter:
    Entre tentativas espera-se um tempo aleatório entre 0 e base * 2^(tentativa - 1) ("full jitter").
    O aleatório evita que muitos pedidos que falharam ao mesmo tempo voltem a tentar todos ao mesmo
    tempo. Quem usa o retry deixa de tentar assim que o circuito abre.

Este módulo não sabe nada de HTTP nem de métricas: o main.py chama allow_request() antes de cada
tentativa e record_success()/record_failure() depois, e recebe as mudanças de estado em
on_state_change (para atualizar a métrica circuit_breaker_state).
"""

import random
import time
from enum import IntEnum
from typing import Callable


class State(IntEnum):
    """Estados do circuito. Os valores são os da métrica circuit_breaker_state."""

    CLOSED = 0
    OPEN = 1
    HALF_OPEN = 2


class CircuitBreaker:
    """
    Circuit breaker para um único processo.

    Não precisa de locks: o Order corre em asyncio (uma só thread por processo), por isso nunca há
    dois pedidos a mudar o estado ao mesmo tempo. Cada processo tem o seu circuito: é por isso que o
    Order corre com um só processo (WEB_CONCURRENCY=1).
    """

    def __init__(
        self,
        failure_threshold: int,
        open_seconds: float,
        half_open_max_calls: int = 1,
        clock: Callable[[], float] = time.monotonic,
        on_state_change: Callable[[State, State], None] | None = None,
    ):
        if failure_threshold < 1:
            raise ValueError("failure_threshold tem de ser pelo menos 1")
        if open_seconds < 0:
            raise ValueError("open_seconds não pode ser negativo")
        if half_open_max_calls < 1:
            raise ValueError("half_open_max_calls tem de ser pelo menos 1")

        self.failure_threshold = failure_threshold
        self.open_seconds = open_seconds
        self.half_open_max_calls = half_open_max_calls
        self._clock = clock
        self._on_state_change = on_state_change

        self._state = State.CLOSED
        self._failures = 0           # falhas seguidas no estado CLOSED
        self._opened_at = 0.0        # quando abriu (relógio monotónico)
        self._half_open_calls = 0    # chamadas de teste já deixadas passar no estado HALF_OPEN

    @property
    def state(self) -> State:
        """Estado atual. Passa de OPEN a HALF_OPEN quando já passaram `open_seconds`."""
        if self._state is State.OPEN and self._clock() - self._opened_at >= self.open_seconds:
            self._change_to(State.HALF_OPEN)
        return self._state

    @property
    def consecutive_failures(self) -> int:
        return self._failures

    def allow_request(self) -> bool:
        """Diz se uma chamada pode ser feita agora. Se devolver False, não se chama o Payment."""
        state = self.state
        if state is State.CLOSED:
            return True
        if state is State.HALF_OPEN and self._half_open_calls < self.half_open_max_calls:
            self._half_open_calls += 1
            return True
        return False

    def record_success(self) -> None:
        """Regista uma chamada que correu bem."""
        if self._state is State.HALF_OPEN:
            self._change_to(State.CLOSED)
        self._failures = 0

    def record_failure(self) -> None:
        """Regista uma chamada que falhou (5xx, timeout ou erro de ligação)."""
        if self._state is State.HALF_OPEN:
            # A chamada de teste falhou: o Payment ainda não recuperou
            self._open()
            return
        if self._state is State.CLOSED:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._open()
        # Em OPEN não há nada a fazer: é uma resposta atrasada de uma chamada feita antes de abrir

    def _open(self) -> None:
        self._opened_at = self._clock()
        self._change_to(State.OPEN)

    def _change_to(self, new_state: State) -> None:
        old_state = self._state
        if old_state is new_state:
            return
        self._state = new_state
        self._failures = 0
        self._half_open_calls = 0
        if self._on_state_change is not None:
            self._on_state_change(old_state, new_state)


def full_jitter_delay(attempt: int, base_seconds: float, cap_seconds: float = 2.0) -> float:
    """
    Tempo a esperar antes da tentativa seguinte, depois de falhar a tentativa número `attempt` (1, 2, ...).

    Um valor aleatório entre 0 e min(cap_seconds, base_seconds * 2^(attempt - 1)): o intervalo máximo
    duplica a cada tentativa (backoff exponencial) e o aleatório espalha os retries no tempo (jitter).
    """
    if attempt < 1:
        raise ValueError("attempt começa em 1")
    return random.uniform(0, min(cap_seconds, base_seconds * 2 ** (attempt - 1)))
