import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// Teste de carga do Circuit Breaker (Parte II - C3):
//   - 0m a 6m: 50 pedidos/s diretamente no Order (POST /orders)
//   - aos 2m:  injeta falhas no Payment (error_rate: 0.8 por omissão)
//   - aos 4m:  repõe o Payment sem falhas (error_rate: 0, latency_seconds: 0)
//
// Correr no Docker Compose:
//   docker compose run --rm k6 run circuit-breaker-test.js
// Correr com latência de 10s em vez de erro 500:
//   docker compose run --rm -e FAULT_ERROR_RATE=0 -e FAULT_LATENCY_SECONDS=10 k6 run circuit-breaker-test.js
// Correr no cluster local (Kubernetes):
//   .\k8s-tools\k6.ps1 cb

// 201 (CONFIRMED), 502 (Payment failed) e 503 (Circuit OPEN) são respostas esperadas durante o ensaio
http.setResponseCallback(http.expectedStatuses(200, 201, 502, 503));

const ordersConfirmed = new Counter('compras_201_confirmed');
const ordersPaymentFailed = new Counter('compras_502_payment_failed');
const ordersCircuitOpen = new Counter('compras_503_circuit_open');

const ORDER_URL = __ENV.ORDER_URL || __ENV.BASE_URL || 'http://order:8002';
const PAYMENT_URL = __ENV.PAYMENT_URL || 'http://payment:8004';
const FAULT_ERROR_RATE = Number(__ENV.FAULT_ERROR_RATE ?? 0.8);
const FAULT_LATENCY_SECONDS = Number(__ENV.FAULT_LATENCY_SECONDS ?? 0);

const EVENTS = ['evt-101', 'evt-102', 'evt-103', 'evt-104'];
const HEADERS = { 'Content-Type': 'application/json' };

export const options = {
  scenarios: {
    carga_orders: {
      executor: 'constant-arrival-rate',
      rate: 50,
      timeUnit: '1s',
      duration: '6m',
      preAllocatedVUs: 50,
      maxVUs: 200,
      exec: 'criarCompra',
    },
    injetar_falha: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      startTime: '2m',
      exec: 'injetarFalha',
    },
    repor_normal: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      startTime: '4m',
      exec: 'reporNormal',
    },
  },
};

export function criarCompra() {
  const order = {
    customer_id: `cliente-${__VU}`,
    customer_email: `cliente${__VU}@exemplo.pt`,
    event_id: EVENTS[__ITER % EVENTS.length],
    quantity: (__ITER % 4) + 1,
  };

  const res = http.post(`${ORDER_URL}/orders`, JSON.stringify(order), {
    headers: HEADERS,
    tags: { name: 'POST /orders' },
  });

  if (res.status === 201) {
    ordersConfirmed.add(1);
  } else if (res.status === 502) {
    ordersPaymentFailed.add(1);
  } else if (res.status === 503) {
    ordersCircuitOpen.add(1);
  }

  check(res, {
    'resposta válida do Order (201, 502 ou 503)': (r) =>
      r.status === 201 || r.status === 502 || r.status === 503,
    '503 tem payload de circuito aberto': (r) =>
      r.status !== 503 || r.json('error') === 'Payment system unavailable, try again later',
  });
}

export function injetarFalha() {
  const payload = JSON.stringify({
    error_rate: FAULT_ERROR_RATE,
    latency_seconds: FAULT_LATENCY_SECONDS,
  });

  const res = http.post(`${PAYMENT_URL}/payments/configure-faults`, payload, {
    headers: HEADERS,
    tags: { name: 'POST /payments/configure-faults (injetar)' },
  });

  check(res, {
    'falha injetada no Payment (200)': (r) => r.status === 200,
  });
}

export function reporNormal() {
  const payload = JSON.stringify({
    error_rate: 0,
    latency_seconds: 0,
  });

  const res = http.post(`${PAYMENT_URL}/payments/configure-faults`, payload, {
    headers: HEADERS,
    tags: { name: 'POST /payments/configure-faults (repor)' },
  });

  check(res, {
    'Payment reposto ao normal (200)': (r) => r.status === 200,
  });
}