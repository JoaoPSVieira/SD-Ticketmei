"""
Rate limiter distribuído do API Gateway (fase 3): algoritmo *token bucket* com o estado no Redis.

Cada cliente (identificado pelo IP) tem um "balde" com até CAPACIDADE fichas. Cada pedido gasta uma
ficha e o balde volta a encher a RITMO fichas por segundo. Sem fichas, o pedido é recusado (429).
Com capacidade 5 e ritmo 5/s: um cliente pode fazer 5 pedidos seguidos (rajada) e depois 5 por segundo.

Porquê no Redis e não na memória do Gateway:
    Cada pod do Gateway tem 4 processos (workers) e pode haver várias réplicas. Com contadores em
    memória, cada processo teria o seu balde e o limite real seria 5 × 4 × réplicas pedidos/s.
    Com o balde no Redis, todos os processos e réplicas veem o mesmo balde.

Porquê um script Lua:
    O Redis executa o script de uma só vez (atómico): nenhum outro pedido mexe no balde entre a
    leitura e a escrita. Com um GET e um SET separados, dois pedidos ao mesmo tempo podiam ler as
    mesmas fichas e passar os dois (race condition).

Se o Redis falhar:
    O pedido passa (fail open) e fica registado na métrica gateway_rate_limiter_errors_total.
    Preferimos não vender durante uns segundos sem limite do que deixar de vender.
"""

import logging
import math
import time
from dataclasses import dataclass

import redis.asyncio as redis
from prometheus_client import Counter
from redis.exceptions import RedisError

logger = logging.getLogger("api-gateway")

RATE_LIMITER_ERRORS = Counter(
    "gateway_rate_limiter_errors_total",
    "Pedidos que passaram sem verificação do limite porque o Redis falhou (fail open)",
)

# KEYS[1]: chave do balde do cliente. ARGV[1]: capacidade. ARGV[2]: fichas por segundo.
# Devolve {1 se o pedido passa / 0 se é recusado, fichas que sobram, segundos até haver uma ficha}.
TOKEN_BUCKET_LUA = """
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])

-- Hora do próprio Redis: todos os processos e réplicas do Gateway usam o mesmo relógio
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000

-- Balde guardado: fichas e hora da última atualização (cliente novo: balde cheio)
local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at')
local tokens = tonumber(bucket[1]) or capacity
local updated_at = tonumber(bucket[2]) or now

-- Volta a encher com as fichas ganhas desde a última atualização, sem passar da capacidade
tokens = math.min(capacity, tokens + math.max(0, now - updated_at) * rate)

local allowed = 0
local retry_after = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_after = (1 - tokens) / rate
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_at', now)
-- O balde apaga-se sozinho depois de o cliente parar (quando já estaria cheio outra vez)
redis.call('EXPIRE', KEYS[1], math.ceil(capacity / rate) + 1)

-- Números com casas decimais vão como texto (o Redis converteria um número Lua num inteiro)
return {allowed, tostring(tokens), tostring(retry_after)}
"""


@dataclass
class Decision:
    """Resultado da verificação do limite para um pedido."""

    allowed: bool
    limit: int  # capacidade do balde
    remaining: int  # fichas que sobram depois deste pedido
    retry_after: int  # segundos até o cliente poder tentar outra vez (só quando é recusado)

    def headers(self) -> dict[str, str]:
        """Cabeçalhos que informam o cliente sobre o limite (a convenção de muitas APIs, por exemplo a do GitHub)."""
        headers = {"X-RateLimit-Limit": str(self.limit), "X-RateLimit-Remaining": str(self.remaining)}
        if not self.allowed:
            headers["Retry-After"] = str(self.retry_after)
        return headers


class RateLimiter:
    def __init__(self, redis_url: str, rate: float, capacity: int, key_prefix: str = "rate_limit",
                 timeout_seconds: float = 0.2):
        self.rate = rate
        self.capacity = capacity
        self.key_prefix = key_prefix
        # Timeouts curtos: se o Redis não responder, o pedido não pode ficar à espera dele
        self.redis = redis.Redis.from_url(
            redis_url,
            socket_connect_timeout=timeout_seconds,
            socket_timeout=timeout_seconds,
            decode_responses=True,
        )
        # O redis-py envia o script uma vez e depois só o identificador (EVALSHA)
        self.script = self.redis.register_script(TOKEN_BUCKET_LUA)
        self._last_warning = 0.0

    async def check(self, client_id: str) -> Decision | None:
        """Gasta uma ficha do balde do cliente. Devolve None se o Redis falhar (o pedido passa)."""
        try:
            allowed, tokens, retry_after = await self.script(
                keys=[f"{self.key_prefix}:{client_id}"], args=[self.capacity, self.rate]
            )
        except (RedisError, OSError) as exc:
            RATE_LIMITER_ERRORS.inc()
            # Um aviso a cada 10 s no máximo, para não encher o log com milhares de linhas iguais
            if time.monotonic() - self._last_warning > 10:
                self._last_warning = time.monotonic()
                logger.warning("Redis indisponível (%s): pedidos a passar sem limite", type(exc).__name__)
            return None

        return Decision(
            allowed=allowed == 1,
            limit=self.capacity,
            remaining=int(float(tokens)),
            retry_after=max(1, math.ceil(float(retry_after))),
        )

    async def close(self) -> None:
        await self.redis.aclose()
