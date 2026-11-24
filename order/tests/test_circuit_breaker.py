"""Testes da classe CircuitBreaker e do intervalo com jitter (sem HTTP: um relógio falso controla o tempo)."""

import pytest

import circuit_breaker
from circuit_breaker import CircuitBreaker, State, full_jitter_delay


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def changes():
    return []


@pytest.fixture
def breaker(clock, changes):
    return CircuitBreaker(failure_threshold=3, open_seconds=10, half_open_max_calls=1, clock=clock,
                          on_state_change=lambda old, new: changes.append((old, new)))


def fail(breaker: CircuitBreaker, times: int) -> None:
    for _ in range(times):
        assert breaker.allow_request()
        breaker.record_failure()


def test_starts_closed_and_lets_calls_through(breaker):
    assert breaker.state is State.CLOSED
    assert breaker.allow_request()


def test_opens_after_threshold_consecutive_failures(breaker, changes):
    fail(breaker, 2)
    assert breaker.state is State.CLOSED

    fail(breaker, 1)

    assert breaker.state is State.OPEN
    assert changes == [(State.CLOSED, State.OPEN)]


def test_a_success_resets_the_failure_count(breaker):
    fail(breaker, 2)
    breaker.record_success()
    fail(breaker, 2)

    assert breaker.state is State.CLOSED


def test_open_rejects_calls_without_calling(breaker):
    fail(breaker, 3)

    assert not breaker.allow_request()
    assert not breaker.allow_request()


def test_becomes_half_open_after_open_seconds(breaker, clock, changes):
    fail(breaker, 3)
    clock.advance(9.9)
    assert breaker.state is State.OPEN

    clock.advance(0.1)

    assert breaker.state is State.HALF_OPEN
    assert changes[-1] == (State.OPEN, State.HALF_OPEN)


def test_half_open_lets_only_the_trial_calls_through(breaker, clock):
    fail(breaker, 3)
    clock.advance(10)

    assert breaker.allow_request()       # a chamada de teste
    assert not breaker.allow_request()   # as outras falham logo enquanto o teste não acaba


def test_half_open_closes_when_the_trial_succeeds(breaker, clock, changes):
    fail(breaker, 3)
    clock.advance(10)
    assert breaker.allow_request()

    breaker.record_success()

    assert breaker.state is State.CLOSED
    assert breaker.allow_request()
    assert changes == [(State.CLOSED, State.OPEN), (State.OPEN, State.HALF_OPEN), (State.HALF_OPEN, State.CLOSED)]


def test_half_open_reopens_when_the_trial_fails(breaker, clock):
    fail(breaker, 3)
    clock.advance(10)
    assert breaker.allow_request()

    breaker.record_failure()

    assert breaker.state is State.OPEN
    assert not breaker.allow_request()
    clock.advance(10)  # volta a esperar o tempo todo
    assert breaker.state is State.HALF_OPEN


def test_late_failures_while_open_do_not_change_anything(breaker, clock):
    fail(breaker, 3)
    breaker.record_failure()  # resposta atrasada de uma chamada feita antes de abrir
    clock.advance(10)

    assert breaker.state is State.HALF_OPEN


def test_closing_starts_counting_from_zero(breaker, clock):
    fail(breaker, 3)
    clock.advance(10)
    assert breaker.allow_request()
    breaker.record_success()

    fail(breaker, 2)

    assert breaker.state is State.CLOSED


def test_state_values_match_the_metric():
    # circuit_breaker_state: 0 = CLOSED, 1 = OPEN, 2 = HALF_OPEN (enunciado)
    assert (int(State.CLOSED), int(State.OPEN), int(State.HALF_OPEN)) == (0, 1, 2)


@pytest.mark.parametrize("kwargs", [
    {"failure_threshold": 0, "open_seconds": 1},
    {"failure_threshold": 1, "open_seconds": -1},
    {"failure_threshold": 1, "open_seconds": 1, "half_open_max_calls": 0},
])
def test_invalid_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        CircuitBreaker(**kwargs)


# --- Retry com jitter --------------------------------------------------------

def test_jitter_is_random_between_zero_and_the_exponential_limit(monkeypatch):
    limits = []
    monkeypatch.setattr(circuit_breaker.random, "uniform", lambda low, high: limits.append((low, high)) or high)

    delays = [full_jitter_delay(attempt, base_seconds=0.1) for attempt in (1, 2, 3)]

    assert limits == [(0, 0.1), (0, 0.2), (0, 0.4)]
    assert delays == [0.1, 0.2, 0.4]


def test_jitter_never_goes_above_the_cap():
    assert all(0 <= full_jitter_delay(10, base_seconds=1, cap_seconds=2) <= 2 for _ in range(100))


def test_jitter_spreads_the_retries():
    delays = {full_jitter_delay(1, base_seconds=1) for _ in range(20)}

    assert len(delays) > 1  # sem jitter seriam todos iguais


def test_jitter_attempts_start_at_one():
    with pytest.raises(ValueError):
        full_jitter_delay(0, base_seconds=0.1)
