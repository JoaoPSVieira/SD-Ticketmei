"""
Testes do rate limiter do POST /orders (fase 3).

Os da primeira parte usam um limitador falso e não precisam de Redis. Os da segunda parte usam um
Redis verdadeiro em REDIS_TEST_URL e são ignorados se não houver nenhum (no CI há; no PC pode-se
arrancar um com: docker run --rm -p 6379:6379 redis:7.4-alpine).
"""

import asyncio
import os
import time
import uuid
from contextlib import contextmanager

import pytest
import redis
from prometheus_client import REGISTRY

import main
from conftest import FakeServicesHandler
from rate_limiter import Decision, RateLimiter

REDIS_TEST_URL = os.getenv("REDIS_TEST_URL", "redis://127.0.0.1:6379/0")

VALID_ORDER = {"customer_id": "cliente-1", "customer_email": "ana@exemplo.pt", "event_id": "evt-101", "quantity": 2}


def redis_available() -> bool:
    try:
        redis.Redis.from_url(REDIS_TEST_URL, socket_connect_timeout=1).ping()
        return True
    except redis.RedisError:
        return False


needs_redis = pytest.mark.skipif(not redis_available(), reason="Sem Redis acessível em REDIS_TEST_URL")


class FakeLimiter:
    """Limitador falso: devolve sempre a mesma decisão e regista quem foi verificado."""

    def __init__(self, decision: Decision | None):
        self.decision = decision
        self.clients: list[str] = []

    async def check(self, client_id: str) -> Decision | None:
        self.clients.append(client_id)
        return self.decision

    async def close(self) -> None:
        pass


ALLOWED = Decision(allowed=True, limit=5, remaining=3, retry_after=1)
REJECTED = Decision(allowed=False, limit=5, remaining=0, retry_after=1)


@pytest.fixture
def client_with(make_client, fake_catalog_url):
    """TestClient do Gateway com o limitador à escolha (o lifespan cria-o a None, sem REDIS_URL)."""

    @contextmanager
    def _make(limiter):
        with make_client(fake_catalog_url) as client:
            client.app.state.rate_limiter = limiter
            yield client

    return _make


def posts_received() -> int:
    return sum(1 for path, _, _ in FakeServicesHandler.received_posts if path == "/orders")


def requests_count(status: str) -> float:
    value = REGISTRY.get_sample_value(
        "gateway_requests_total", {"method": "POST", "endpoint": "/orders", "http_status": status}
    )
    return value or 0.0


# --- Comportamento do Gateway (limitador falso, sem Redis) -------------------

def test_rejected_request_returns_429_without_calling_order(client_with):
    before = posts_received()
    with client_with(FakeLimiter(REJECTED)) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 429
    assert "Demasiados pedidos" in response.json()["detail"]
    assert posts_received() == before  # o Order nem chegou a ser chamado


def test_429_tells_the_client_when_to_retry(client_with):
    with client_with(FakeLimiter(REJECTED)) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.headers["retry-after"] == "1"
    assert response.headers["x-ratelimit-limit"] == "5"
    assert response.headers["x-ratelimit-remaining"] == "0"


def test_allowed_request_is_forwarded_with_rate_limit_headers(client_with):
    before = posts_received()
    with client_with(FakeLimiter(ALLOWED)) as client:
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 202
    assert response.json()["status"] == "PENDING"
    assert response.headers["location"] == "/orders/ord-1"
    assert response.headers["x-ratelimit-limit"] == "5"
    assert response.headers["x-ratelimit-remaining"] == "3"
    assert "retry-after" not in response.headers
    assert posts_received() == before + 1


def test_client_is_identified_by_its_ip(client_with):
    limiter = FakeLimiter(ALLOWED)
    with client_with(limiter) as client:
        client.post("/orders", json=VALID_ORDER, headers={"X-Forwarded-For": "1.2.3.4"})

    # O TestClient liga-se com o "IP" testclient; o X-Forwarded-For (que o cliente pode inventar) é ignorado
    assert limiter.clients == ["testclient"]


def test_only_post_orders_is_limited(client_with):
    limiter = FakeLimiter(REJECTED)
    with client_with(limiter) as client:
        assert client.get("/events").status_code == 200
        assert client.get("/orders/ord-1").status_code == 200

    assert limiter.clients == []


def test_without_redis_url_there_is_no_limit(make_client, fake_catalog_url):
    with make_client(fake_catalog_url) as client:
        assert client.app.state.rate_limiter is None
        response = client.post("/orders", json=VALID_ORDER)

    assert response.status_code == 202
    assert "x-ratelimit-limit" not in response.headers


def test_429_is_counted_in_the_gateway_metrics(client_with):
    before = requests_count("429")
    with client_with(FakeLimiter(REJECTED)) as client:
        client.post("/orders", json=VALID_ORDER)

    # É esta série que o dashboard "TicketMei - Kubernetes" usa
    assert requests_count("429") == before + 1


def test_redis_down_lets_requests_through(client_with, unused_url):
    errors_before = REGISTRY.get_sample_value("gateway_rate_limiter_errors_total") or 0.0
    port = unused_url.rsplit(":", 1)[1]
    with client_with(None) as client:
        # Limitador verdadeiro, mas a apontar para uma porta onde não há Redis
        client.app.state.rate_limiter = RateLimiter(f"redis://127.0.0.1:{port}/0", rate=5, capacity=5)
        start = time.perf_counter()
        response = client.post("/orders", json=VALID_ORDER)
        elapsed = time.perf_counter() - start

    assert response.status_code == 202  # fail open: a compra passa
    assert "x-ratelimit-limit" not in response.headers
    assert REGISTRY.get_sample_value("gateway_rate_limiter_errors_total") == errors_before + 1
    assert elapsed < 2  # o Redis em baixo não atrasa o pedido


# --- Token bucket num Redis verdadeiro ------------------------------------------

def run_checks(client_ids: list[str], *, capacity: int, rate: float, concurrent: bool = False,
               key_prefix: str | None = None) -> list[Decision]:
    """Faz uma verificação por elemento de client_ids, com um limitador novo ligado ao Redis de teste."""

    async def _run():
        limiter = RateLimiter(REDIS_TEST_URL, rate=rate, capacity=capacity,
                              key_prefix=key_prefix or f"teste-{uuid.uuid4().hex}")
        try:
            if concurrent:
                return await asyncio.gather(*(limiter.check(c) for c in client_ids))
            return [await limiter.check(c) for c in client_ids]
        finally:
            await limiter.close()

    return asyncio.run(_run())


@needs_redis
def test_burst_up_to_capacity_then_429():
    decisions = run_checks(["ana"] * 7, capacity=5, rate=0.01)

    assert [d.allowed for d in decisions] == [True] * 5 + [False] * 2
    assert [d.remaining for d in decisions] == [4, 3, 2, 1, 0, 0, 0]
    assert decisions[-1].retry_after >= 1


@needs_redis
def test_bucket_refills_over_time():
    prefix = f"teste-{uuid.uuid4().hex}"
    first = run_checks(["ana"] * 3, capacity=2, rate=10, key_prefix=prefix)  # 10 fichas por segundo
    time.sleep(0.25)  # ganha ~2 fichas
    later = run_checks(["ana"] * 2, capacity=2, rate=10, key_prefix=prefix)

    assert [d.allowed for d in first] == [True, True, False]
    assert [d.allowed for d in later] == [True, True]


@needs_redis
def test_retry_after_matches_the_refill_rate():
    decisions = run_checks(["ana"] * 2, capacity=1, rate=0.25)  # uma ficha a cada 4 s

    assert decisions[1].allowed is False
    assert decisions[1].retry_after == 4


@needs_redis
def test_each_client_has_its_own_bucket():
    decisions = run_checks(["ana", "ana", "ana", "rui"], capacity=2, rate=0.01)

    assert [d.allowed for d in decisions] == [True, True, False, True]


@needs_redis
def test_concurrent_requests_never_exceed_the_limit():
    # 100 pedidos ao mesmo tempo do mesmo cliente: o script Lua é atómico, por isso passam exatamente 10
    decisions = run_checks(["ana"] * 100, capacity=10, rate=0.001, concurrent=True)

    assert sum(d.allowed for d in decisions) == 10


@needs_redis
def test_bucket_expires_after_the_client_stops():
    prefix = f"teste-{uuid.uuid4().hex}"
    run_checks(["ana"], capacity=5, rate=5, key_prefix=prefix)

    ttl = redis.Redis.from_url(REDIS_TEST_URL).ttl(f"{prefix}:ana")
    assert 0 < ttl <= 2  # cheio outra vez ao fim de 5 / 5 = 1 s, mais 1 s de margem


@needs_redis
def test_end_to_end_with_redis(monkeypatch, make_client, fake_catalog_url):
    monkeypatch.setattr(main, "REDIS_URL", REDIS_TEST_URL)
    monkeypatch.setattr(main, "RATE_LIMIT_PER_SECOND", 0.01)
    monkeypatch.setattr(main, "RATE_LIMIT_BURST", 3)
    redis.Redis.from_url(REDIS_TEST_URL).delete("rate_limit:testclient")  # balde de execuções anteriores

    with make_client(fake_catalog_url) as client:
        assert isinstance(client.app.state.rate_limiter, RateLimiter)
        statuses = [client.post("/orders", json=VALID_ORDER).status_code for _ in range(5)]

    assert statuses == [202, 202, 202, 429, 429]
