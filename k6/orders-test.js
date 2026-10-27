import http from 'k6/http';
import { check, sleep } from 'k6';

// Teste de carga das compras (fase 2).
// Cada utilizador virtual cria uma compra através do Gateway (POST /orders) e consulta-a logo a seguir
// (GET /orders/{id}), como faria um cliente a acompanhar o estado da compra.
//
// Correr (com o docker compose a correr):   docker compose run --rm k6 run orders-test.js
// Com mais carga, para ver a fila do RabbitMQ a crescer:
//   docker compose run --rm k6 run --stage 30s:200,60s:200,10s:0 orders-test.js

export const options = {
  stages: [
    { duration: '1m', target: 50 },   // sobe até 50 utilizadores virtuais
    { duration: '2m', target: 50 },   // mantém
    { duration: '30s', target: 0 },   // desce
  ],
  thresholds: {
    http_req_duration: ['p(95)<200'], // 95% dos pedidos abaixo de 200 ms
    http_req_failed: ['rate<0.01'],   // menos de 1% de pedidos falhados
    checks: ['rate>0.99'],            // mais de 99% das verificações certas
  },
};

// Dentro da rede do Docker Compose o Gateway chama-se api-gateway.
// Para correr o k6 fora do Docker: k6 run -e BASE_URL=http://localhost:8000 orders-test.js
const BASE_URL = __ENV.BASE_URL || 'http://api-gateway:8000';
const EVENTS = ['evt-101', 'evt-102', 'evt-103', 'evt-104'];
const HEADERS = { 'Content-Type': 'application/json' };

export default function () {
  const order = {
    customer_id: `cliente-${__VU}`,
    customer_email: `cliente${__VU}@exemplo.pt`,
    event_id: EVENTS[__ITER % EVENTS.length],
    quantity: (__ITER % 4) + 1, // 1 a 4 bilhetes (o limite por compra)
  };

  const created = http.post(`${BASE_URL}/orders`, JSON.stringify(order), {
    headers: HEADERS,
    tags: { name: 'POST /orders' },
  });
  const accepted = check(created, {
    'compra aceite (202)': (r) => r.status === 202,
    'compra no estado PENDING': (r) => r.status === 202 && r.json('status') === 'PENDING',
  });

  if (accepted) {
    const fetched = http.get(`${BASE_URL}/orders/${created.json('id')}`, { tags: { name: 'GET /orders/{id}' } });
    check(fetched, { 'compra consultável (200)': (r) => r.status === 200 });
  }

  // Tempo de "pensar" entre compras: com 50 utilizadores dá ~100 compras/s,
  // perto do que o Notification consegue enviar (~100 emails/s com PREFETCH_COUNT=10)
  sleep(0.5);
}
