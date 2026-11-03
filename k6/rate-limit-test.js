import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// Demonstração do rate limiter do Gateway (fase 3).
// Um único cliente tenta 20 compras por segundo durante 30 s. Com o limite de 5 por segundo
// (e rajadas de 5), o Gateway aceita ~5 por segundo (~155 no total) e responde 429 às restantes.
//
// No Kubernetes:   .\k8s-tools\k6.ps1 limite
// Não serve para o compose: lá não há Redis, por isso não há limite (e os thresholds falham).

export const options = {
  scenarios: {
    cliente_insistente: {
      executor: 'constant-arrival-rate', // 20 pedidos por segundo, seja qual for o tempo de resposta
      rate: 20,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 20,
    },
  },
  thresholds: {
    // O limite está a funcionar: 5 de rajada + 5/s × 30 s ≈ 155 aceites (com margem para o arranque)
    compras_aceites: ['count>=120', 'count<=190'],
    compras_limitadas_429: ['count>=300'],
    checks: ['rate>0.99'],
    http_req_failed: ['rate<0.01'], // só 202 e 429 são respostas certas
  },
};

http.setResponseCallback(http.expectedStatuses(202, 429));

const BASE_URL = __ENV.BASE_URL || 'http://api-gateway:8000';
const acceptedOrders = new Counter('compras_aceites');
const limitedOrders = new Counter('compras_limitadas_429');

export default function () {
  const order = { customer_id: 'cliente-insistente', customer_email: 'insistente@exemplo.pt', event_id: 'evt-101', quantity: 1 };
  const res = http.post(`${BASE_URL}/orders`, JSON.stringify(order), {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /orders' },
  });

  if (res.status === 202) {
    acceptedOrders.add(1);
  } else if (res.status === 429) {
    limitedOrders.add(1);
  }

  check(res, {
    'aceite (202) ou limitado (429)': (r) => r.status === 202 || r.status === 429,
    'cabeçalhos X-RateLimit presentes': (r) => r.headers['X-Ratelimit-Limit'] !== undefined,
    '429 com Retry-After': (r) => r.status !== 429 || Number(r.headers['Retry-After']) >= 1,
  });
}
