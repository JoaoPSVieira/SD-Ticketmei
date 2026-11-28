# TicketMEI

[![CI](https://github.com/JoaoPSVieira/SD-Ticketmei/actions/workflows/ci.yml/badge.svg)](https://github.com/JoaoPSVieira/SD-Ticketmei/actions/workflows/ci.yml)
[![CD](https://github.com/JoaoPSVieira/SD-Ticketmei/actions/workflows/cd.yml/badge.svg)](https://github.com/JoaoPSVieira/SD-Ticketmei/actions/workflows/cd.yml)

Plataforma eletrónica de venda de bilhetes para eventos, baseada em microsserviços.
Projeto da unidade curricular de **Sistemas Distribuídos** — Mestrado em Engenharia Informática, Universidade da Madeira, 2026/2027.

## Grupo

| Nome                                | Número | GitHub                                             |
| ----------------------------------- | ------- | -------------------------------------------------- |
| João Pedro Sá Vieira              | 2103823 | [JoaoPSVieira](https://github.com/JoaoPSVieira)     |
| Francisco Guilherme Fernandes Silva | 2114821 | [SkyJust4Fun](https://github.com/SkyJust4Fun)       |
| Rodrigo Sousa Soares                | 2075526 | [SoaresRodrigo8](https://github.com/SoaresRodrigo8) |

## Estado das fases

| Fase | Prazo      | Conteúdo                                                         | Estado                          |
| ---- | ---------- | ----------------------------------------------------------------- | ------------------------------- |
| 1    | 21/10/2026 | API Gateway + Catalog, Docker Compose, Prometheus, Grafana        | Concluída (o p95 < 200 ms foi cumprido na fase 2, com vários processos por serviço) |
| 2    | 28/10/2026 | Order + Notification, RabbitMQ                                    | Concluída                      |
| 3    | 04/11/2026 | Kubernetes, GitHub Actions + GHCR, ArgoCD, rate limiter com Redis | Concluída                       |

## Arquitetura (Fases 1 e 2)

```mermaid
flowchart LR
    client["Cliente / k6"] -->|HTTP| gw["API Gateway<br/>:8000"]
    gw -->|"HTTP síncrono"| cat["Catalog<br/>:8001"]
    gw -->|"HTTP síncrono"| ord["Order<br/>:8002"]
    ord -->|"HTTP síncrono<br/>(preço do evento)"| cat
    cat --- events[("events.json")]
    ord --- db[("SQLite<br/>orders.db")]
    ord -->|"HTTP síncrono<br/>(circuit breaker + retry)"| pay["Payment<br/>:8004"]
    ord -->|"OrderCreated"| mq[["RabbitMQ<br/>orders.exchange → fila order_created"]]
    mq -->|"assíncrono"| notif["Notification<br/>:8003"]
    notif --- log[("notifications.log")]

    prom["Prometheus<br/>:9090"] -.-> gw & cat & ord & notif & mq & cadv["cAdvisor<br/>:8080"]
    graf["Grafana<br/>:3000"] -->|PromQL| prom
```

As linhas tracejadas são o Prometheus a recolher métricas (*scrape*) de cada componente.

- Todo o tráfego dos clientes entra pelo **API Gateway**, que encaminha os pedidos de forma **síncrona**: os eventos para o **Catalog** e as compras para o **Order**.
- O **Order** pergunta ao Catalog o preço do evento (síncrono), grava a compra como `PENDING` e cobra-a no **Payment** (síncrono, através de um *circuit breaker* com *retry* e *jitter*). Se o pagamento correr bem, a compra fica `CONFIRMED`, a mensagem `OrderCreated` é publicada na exchange `orders.exchange` do **RabbitMQ** e a resposta é **201 Created**, sem esperar pelo email.
- O **Notification** consome a fila `order_created` de forma **assíncrona** e simula o envio do email de confirmação.
- Cada serviço tem acesso exclusivo aos seus dados: o Catalog ao `events.json`, o Order à sua base de dados SQLite, o Notification ao seu log de entregas.
- O **Prometheus** recolhe as métricas de todos os serviços, do RabbitMQ e do cAdvisor. O **Grafana** arranca com o datasource e os dashboards configurados (provisioning).
- Todos os componentes correm em contentores, orquestrados com Docker Compose numa rede própria (`ticketmei-net`).

## Estrutura do repositório

```
.
├── .github/workflows/ci.yml      # Pipeline de CI (GitHub Actions)
├── .github/workflows/cd.yml      # Pipeline de CD: imagens no GHCR e tags em k8s/ (fase 3)
├── ci/
│   ├── smoke-test.sh             # Smoke test do sistema completo (usado pelo CI)
│   └── cd-plano.sh, cd-atualizar-manifestos.sh   # Passos da CD
├── api-gateway/                  # API Gateway (Python, FastAPI + aiohttp; rate limiter em rate_limiter.py)
├── catalog/                      # Catalog (Python, FastAPI)
│   └── data/events.json          # Dados dos eventos (exclusivos do Catalog)
├── order/                        # Order (Python, FastAPI + SQLite + aio-pika)
├── notification/                 # Notification (Python, aio-pika)
├── payment/                      # Payment (Python, FastAPI + SQLite), com falhas configuráveis (Parte II)
├── inventory/                    # Inventory (Python, FastAPI + SQLite): stock e reservas atómicas (Parte II)
│   └── data/stock.json           # Stock inicial de cada evento
├── prometheus/
│   └── prometheus.yml            # Configuração de scraping
├── grafana/
│   ├── provisioning/             # Datasource e provider de dashboards
│   └── dashboards/               # dashboard1.json, catalog.json, order-notification.json, rabbitmq-overview.json, kubernetes.json
├── k6/
│   ├── load-test.js              # Teste de carga dos eventos (fornecido pela docente)
│   ├── orders-test.js            # Teste de carga das compras
│   └── rate-limit-test.js        # Demonstração do rate limiter (429)
├── k8s/                          # Manifestos Kubernetes, sincronizados pelo Argo CD (fase 3)
│   └── monitoring/               # Prometheus, Grafana e cAdvisor no cluster
├── argocd/application.yaml       # Application do Argo CD (aplicada uma vez)
├── k8s-tools/                    # Imagens locais, gerador de ConfigMaps e Jobs do k6 no cluster
├── docs/img/                     # Capturas de ecrã usadas neste README
├── docker-compose.yml
└── README.md
```

## Como correr

Requisitos: [Docker Desktop](https://www.docker.com/products/docker-desktop/) com Docker Compose v2.

```bash
# Construir e arrancar todos os serviços
docker compose up --build -d

# Ver o estado dos contentores (catalog e api-gateway devem ficar "healthy")
docker compose ps

# Ver logs de um serviço
docker compose logs -f api-gateway

# Parar tudo
docker compose down
```

O API Gateway só arranca depois de o healthcheck do Catalog passar, e o k6 só começa depois de o Gateway estar pronto.

| Serviço                 | URL                                                                |
| ------------------------ | ------------------------------------------------------------------ |
| API Gateway              | http://localhost:8000 (documentação: http://localhost:8000/docs) |
| Catalog                  | http://localhost:8001 (documentação: http://localhost:8001/docs) |
| Order                    | http://localhost:8002 (documentação: http://localhost:8002/docs) |
| Notification             | http://localhost:8003/metrics                                      |
| Payment                  | http://localhost:8004 (documentação: http://localhost:8004/docs) |
| Inventory                | http://localhost:8005 (documentação: http://localhost:8005/docs) |
| RabbitMQ (interface web) | http://localhost:15672 (ticketmei / ticketmei)                     |
| RabbitMQ (métricas)     | http://localhost:15692/metrics                                     |
| Prometheus               | http://localhost:9090                                              |
| Grafana                  | http://localhost:3000 (admin / admin)                              |
| cAdvisor                 | http://localhost:8080                                              |

![Contentores em execução](docs/img/docker-compose-ps.png)

## Endpoints

### API Gateway (porta 8000)

| Método | Endpoint         | Descrição                                                    |
| ------- | ---------------- | -------------------------------------------------------------- |
| GET     | `/events`      | Lista todos os eventos (encaminha para o Catalog)              |
| GET     | `/events/{id}` | Detalhes de um evento (encaminha para o Catalog)               |
| POST    | `/orders`      | Cria uma compra de bilhetes (encaminha para o Order)           |
| GET     | `/orders/{id}` | Detalhes de uma compra (encaminha para o Order)                |
| GET     | `/health`      | Estado do próprio Gateway (não depende dos outros serviços) |
| GET     | `/metrics`     | Métricas no formato Prometheus                                |
| GET     | `/docs`        | Documentação interativa da API (Swagger UI)                  |

Comportamento quando um serviço falha:

| Situação                                                           | Resposta do Gateway                                                                                                  |
| -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| O serviço responde 2xx ou 4xx (ex.: 201, 404, 422)                  | A mesma resposta, sem alterações                                                                                   |
| O serviço responde 502, 503 ou 504                                  | A mesma resposta: é o próprio serviço a explicar o que falhou (ex.: o Order responde 503 com a compra `FAILED`) |
| O serviço responde outro 5xx (ex.: 500, um erro inesperado)         | `502 Bad Gateway`                                                                                                  |
| O serviço está inacessível (contentor parado, ligação recusada) | `502 Bad Gateway`                                                                                                  |
| O serviço não responde a tempo                                     | `504 Gateway Timeout`                                                                                              |

Configuração (variáveis de ambiente):

| Variável                    | Por omissão                               | Descrição                                                                                                              |
| ---------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `CATALOG_URL`              | `http://localhost:8001`                  | Endereço do Catalog (no Compose: `http://catalog:8001`)                                                               |
| `ORDER_URL`                | `http://localhost:8002`                  | Endereço do Order (no Compose: `http://order:8002`)                                                                   |
| `CATALOG_TIMEOUT_SECONDS`  | `2`                                      | Tempo máximo à espera do Catalog                                                                                       |
| `ORDER_TIMEOUT_SECONDS`    | `5`                                      | Tempo máximo à espera do Order. É maior do que o do Catalog porque o Order, lá dentro, chama o Catalog e o RabbitMQ. |
| `UPSTREAM_MAX_CONNECTIONS` | `100`                                    | Ligações TCP reutilizáveis para cada serviço, por processo                                                           |
| `WEB_CONCURRENCY`          | `4` (no contentor)                       | Número de processos do Gateway                                                                                          |
| `PROMETHEUS_MULTIPROC_DIR` | `/tmp/prometheus-metrics` (no contentor) | Pasta onde cada processo guarda as suas métricas, somadas no `/metrics`                                               |

### Order (porta 8002)

| Método | Endpoint         | Descrição                                                                             |
| ------- | ---------------- | --------------------------------------------------------------------------------------- |
| POST    | `/orders`      | Cria uma compra (Saga: reservar, cobrar, confirmar); se for paga, fica `CONFIRMED` e é publicada a mensagem `OrderCreated` no RabbitMQ |
| GET     | `/orders/stats` | Contagens das compras e das sagas por estado (para validar os testes k6)              |
| GET     | `/orders/{id}` | Detalhes de uma compra (404 se não existir)                                            |
| GET     | `/health`      | Estado do serviço, da ligação ao RabbitMQ, do circuito do Payment (`CLOSED`, `OPEN`, `HALF_OPEN` ou `disabled`) e se o Inventory está ligado (`enabled` ou `disabled`) |
| GET     | `/metrics`     | Métricas no formato Prometheus                                                         |
| GET     | `/docs`        | Documentação interativa da API (Swagger UI)                                           |

Pedido `POST /orders`:

```json
{"customer_id": "cliente-1", "customer_email": "ana@exemplo.pt", "event_id": "evt-101", "quantity": 2}
```

| Resposta          | Quando                                                                                                                                                                              |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `201 Created`   | Compra reservada, paga e `CONFIRMED`. O corpo tem a compra (`id`, `total_amount`, `status`, `created_at`, ...) e o cabeçalho `Location: /orders/{id}`. |
| `409`           | Sem bilhetes suficientes: `{"error": "Not enough tickets", "order_id": ...}`, compra `FAILED`, sem cobrar                                                                     |
| `422`           | Campos em falta ou inválidos, email inválido, `quantity` fora de 1–4 (RF2), evento inexistente, ou evento sem stock no Inventory                                               |
| `502`           | O Catalog falhou (não foi possível obter o preço), ou o pagamento falhou em todas as tentativas: `{"error": "Payment failed", ...}`, bilhetes libertados, compra `CANCELLED` |
| `503`           | O circuito do Payment está aberto: `{"error": "Payment system unavailable, try again later"}`, bilhetes libertados, compra `CANCELLED`, sem chamar o Payment; ou o Inventory está indisponível: `{"error": "Inventory unavailable, try again later", ...}`, compra `FAILED` |
| `504`           | O Catalog não respondeu a tempo                                                                                                                                                  |

Enquanto o Inventory não estiver no sistema (`INVENTORY_URL` vazio), não há reserva nem nada a libertar: um pagamento falhado deixa a compra `FAILED` (como na Fase 1).

Se o RabbitMQ estiver em baixo, a compra paga continua `CONFIRMED` (201): só o email é que não é enviado, e a falha fica no log e na métrica `order_publish_failures_total`.

#### Pagamento: circuit breaker e retry com jitter

Cada tentativa de pagamento passa pelo *circuit breaker* (`order/circuit_breaker.py`). Com o circuito **fechado** as chamadas passam; ao fim de `CB_FAILURE_THRESHOLD` falhas seguidas (5xx, timeout ou erro de ligação) o circuito **abre** e o Order responde logo 503, sem chamar o Payment (*fail-fast*). Passados `CB_OPEN_SECONDS`, fica **semiaberto** e deixa passar uma chamada de teste: se correr bem fecha, se falhar volta a abrir. Entre tentativas o Order espera um tempo aleatório (*full jitter*), e deixa de tentar assim que o circuito abre. O Order corre com um só processo, porque cada processo teria o seu circuito.

| Variável                     | Valor     | Descrição                                                                 |
| ---------------------------- | --------- | ------------------------------------------------------------------------- |
| `PAYMENT_URL`                | `http://payment:8004` (compose e `k8s/`) | Endereço do Payment. Vazio: compras confirmadas sem pagamento |
| `PAYMENT_TIMEOUT_SECONDS`    | `1`       | Tempo máximo de cada tentativa. Mais lento do que isto conta como falha   |
| `PAYMENT_RETRY_ATTEMPTS`     | `3`       | Tentativas no total (1 = sem retry)                                       |
| `PAYMENT_RETRY_BASE_SECONDS` | `0.1`     | Base do intervalo aleatório entre tentativas (0,1 s, 0,2 s, 0,4 s, ...)   |
| `CB_FAILURE_THRESHOLD`       | `5`       | Falhas seguidas que abrem o circuito                                       |
| `CB_OPEN_SECONDS`            | `15`      | Tempo com o circuito aberto até à chamada de teste                         |
| `CB_HALF_OPEN_MAX_CALLS`     | `1`       | Chamadas de teste no estado semiaberto                                     |

#### Saga orquestrada (reservar → cobrar → confirmar)

Cada compra é uma **Saga orquestrada pelo Order** (`order/saga.py`): o Order decide a ordem dos passos e, se um falhar, desfaz os anteriores. O Inventory e o Payment só recebem pedidos normais e não sabem que a saga existe.

| Passo | Pedido | Se falhar |
| --- | --- | --- |
| 1. Reservar | `POST /inventory/reserve` | Sem bilhetes (409) ou evento sem stock (404): compra `FAILED`, nada a desfazer. Inventory indisponível: compra `FAILED` e liberta por segurança (a resposta da reserva pode ter-se perdido) |
| 2. Cobrar | `POST /payments`, pelo circuit breaker | **Compensação:** `POST /inventory/release`, com até `COMPENSATION_RETRY_ATTEMPTS` tentativas e espera aleatória entre elas; compra `CANCELLED` |
| 3. Confirmar | — (no Order) | — |

- **Registo:** cada saga tem uma linha na tabela `sagas` (no mesmo SQLite das compras), gravada antes e depois de cada passo: o passo em que vai, se já reservou, se já pagou, o resultado e o erro.
- **Compensação que falha:** se o Inventory não responder a nenhuma tentativa, a compra fica `CANCELLED` (não foi cobrada), a saga fica `COMPENSATION_FAILED` e o Order volta a tentar a libertação em segundo plano, de `SAGA_RECOVERY_SECONDS` em `SAGA_RECOVERY_SECONDS`.
- **Recuperação no arranque:** se o Order parar a meio de uma saga, no arranque seguinte termina-a para a frente se já tinha pago (`CONFIRMED`), ou desfaz para trás se ainda não (liberta os bilhetes).
- **Idempotência:** o Inventory e o Payment tratam um pedido repetido com o mesmo `order_id` como o mesmo pedido, por isso repetir uma reserva, um pagamento ou uma libertação nunca desconta, cobra ou repõe duas vezes. Uma libertação de uma compra sem reserva fica registada no Inventory, para uma reserva atrasada dessa compra ser recusada.

| Variável                          | Valor  | Descrição                                                                 |
| --------------------------------- | ------ | ------------------------------------------------------------------------- |
| `INVENTORY_URL`                   | vazio (até haver Inventory) | Endereço do Inventory (`http://inventory:8005`). Vazio: sem passo de reserva |
| `INVENTORY_TIMEOUT_SECONDS`       | `1`    | Tempo máximo de cada pedido ao Inventory                                  |
| `COMPENSATION_RETRY_ATTEMPTS`     | `3`    | Tentativas da libertação dos bilhetes durante o pedido                    |
| `COMPENSATION_RETRY_BASE_SECONDS` | `0.1`  | Base do intervalo aleatório entre essas tentativas                        |
| `SAGA_RECOVERY_SECONDS`           | `10`   | De quanto em quanto tempo o Order volta a tentar as libertações que falharam (0 desliga) |
| `SQLITE_JOURNAL_MODE`             | `WAL`  | `DELETE` no cluster global, porque o disco é NFS                          |

O preço vem sempre do Catalog (`total_amount = price_eur × quantity`) e nunca do cliente. As compras ficam numa base de dados SQLite própria do Order, guardada num volume do Docker.

### Notification (porta 8003)

Não tem endpoints de negócio: consome as mensagens `OrderCreated` da fila `order_created`, simula o envio do email (100 ms) e regista cada entrega no log `/app/logs/notifications.log`, uma linha JSON por notificação, com `order_id`, `email`, `status` (`SENT` ou `FAILED`), `reason` e `timestamp`.

| Método | Endpoint     | Descrição                                   |
| ------- | ------------ | --------------------------------------------- |
| GET     | `/health`  | Estado do serviço e da ligação ao RabbitMQ |
| GET     | `/metrics` | Métricas no formato Prometheus               |

| Variável                     | Por omissão | Descrição                                                                 |
| ----------------------------- | ------------ | --------------------------------------------------------------------------- |
| `PREFETCH_COUNT`            | `10`       | Mensagens processadas em paralelo (~100 notificações/s)                   |
| `NOTIFICATION_FAILURE_RATE` | `0`        | Percentagem de envios que falham de propósito (0 a 1), para demonstração |
| `RABBITMQ_URL`              | `amqp://ticketmei:ticketmei@localhost:5672/` | Endereço do RabbitMQ no cluster local e no compose |
| `RABBITMQ_HOST`, `RABBITMQ_VHOST`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | — | Cluster global (RabbitMQ partilhado, um vhost por grupo). Se `RABBITMQ_HOST` estiver definido, o endereço é `amqp://USER:PASSWORD@HOST:5672/<vhost codificado>` (`/grupoXX` → `%2FgrupoXX`) e o `RABBITMQ_URL` é ignorado. O utilizador e a password vêm do Secret do cluster. |

### Payment (porta 8004)

Simula o sistema de pagamentos que cobra as compras (Parte II). Não é exposto pelo Gateway: só o Order o chama, dentro do circuit breaker. Tem falhas e latência configuráveis em tempo real, para testar o circuit breaker.

| Método | Endpoint                       | Descrição                                                                                                  |
| ------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| POST    | `/payments`                  | Processa o pagamento de uma compra: `{"order_id": "...", "amount": 170.0}`                                  |
| POST    | `/payments/configure-faults` | Muda as falhas injetadas: `{"error_rate": 0.8}`, `{"latency_seconds": 10}` ou os dois. Só muda os campos enviados |
| GET     | `/payments/configure-faults` | Configuração atual das falhas                                                                               |
| GET     | `/payments/stats`            | Pagamentos por estado: `{"SUCCESS": n, "FAILED": n}` (usado na validação do teste das sagas)              |
| GET     | `/payments/{id}`             | Um pagamento (404 se não existir)                                                                          |
| GET     | `/health`, `/metrics`      | Estado do serviço e métricas no formato Prometheus                                                        |
| GET     | `/docs`                      | Documentação interativa da API (Swagger UI)                                                                |

| Resposta de `POST /payments` | Quando                                                                                                             |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `201 Created`                | Pagamento `SUCCESS`. Corpo: `id`, `order_id`, `amount`, `status`, `processed_at`; cabeçalho `Location`          |
| `200 OK`                     | A compra já estava paga: devolve o mesmo pagamento sem cobrar outra vez (idempotência pelo `order_id`)          |
| `500`                        | Falha injetada: `{"error": "Payment failed", "status": "FAILED"}`. A tentativa fica gravada como `FAILED`        |
| `422`                        | `order_id` em falta, `amount` <= 0, ou valores fora dos limites no `configure-faults` (`error_rate` 0–1, `latency_seconds` >= 0) |

- Cada pedido espera primeiro a latência configurada e só depois sorteia a falha com a taxa configurada.
- **Idempotência:** um retry do Order depois de um timeout não cobra duas vezes. A base de dados também o garante, com um índice único que só deixa existir um pagamento `SUCCESS` por `order_id`.
- **Um só processo:** a configuração das falhas vive na memória do processo. Com vários processos, o `configure-faults` só mudava o que recebeu o pedido, por isso o serviço recusa arrancar com `WEB_CONCURRENCY` diferente de 1. Para escalar usam-se réplicas.

| Variável                    | Por omissão | Descrição                                                                    |
| ---------------------------- | ------------ | ------------------------------------------------------------------------------ |
| `PAYMENT_ERROR_RATE`       | `0`        | Taxa de erros no arranque (0 a 1)                                               |
| `PAYMENT_LATENCY_SECONDS`  | `0`        | Latência no arranque, em segundos                                             |
| `SQLITE_JOURNAL_MODE`      | `WAL`      | `WAL` no cluster local; `DELETE` no global (disco NFS)                       |
| `WEB_CONCURRENCY`          | `1`        | Tem de ser 1                                                                    |

Injetar e repor falhas (no PowerShell):

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8004/payments/configure-faults -ContentType "application/json" -Body '{"error_rate": 0.8}'
Invoke-RestMethod -Method Post -Uri http://localhost:8004/payments/configure-faults -ContentType "application/json" -Body '{"error_rate": 0, "latency_seconds": 0}'
```

No cluster local: `kubectl port-forward svc/payment -n ticketmei 8004:8004`.

### Inventory (porta 8005)

Guarda o stock de bilhetes de cada evento (Parte II, Fase 2). O Order reserva aqui os bilhetes antes de cobrar (passo 1 da Saga) e liberta-os se o pagamento falhar (compensação). Não é exposto pelo Gateway.

| Método | Endpoint                 | Descrição                                                                                                   |
| ------- | ------------------------ | ------------------------------------------------------------------------------------------------------------- |
| POST    | `/inventory/reserve`   | Reserva bilhetes: `{"order_id", "event_id", "quantity"}` → `{order_id, event_id, quantity, available_tickets}` |
| POST    | `/inventory/release`   | Liberta os bilhetes de uma compra: `{"order_id"}` → `{"released": true}` ou `{"released": false}`         |
| GET     | `/inventory/{event_id}` | `{event_id, total_tickets, available_tickets}` (404 se o evento não tiver stock)                            |
| POST    | `/inventory/stock`     | Define o total de um evento, repõe o disponível e apaga as reservas desse evento (prepara os testes)      |
| GET     | `/health`, `/metrics`  | Estado do serviço e métricas no formato Prometheus                                                          |

| Resposta de `POST /inventory/reserve` | Quando                                                                                                        |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `200`                                  | Bilhetes reservados. Repetir a mesma compra devolve a mesma reserva sem descontar outra vez (idempotente)     |
| `409` `{"error": "Not enough tickets"}` | Não há bilhetes suficientes; nada fica reservado                                                             |
| `409` `{"error": "Order already released"}` | A compra já foi libertada: a libertação chegou antes desta reserva (ver abaixo); nada fica reservado    |
| `404`                                  | O evento não tem stock definido                                                                                |

- **Reserva atómica, sem overbooking:** numa só transação, `UPDATE stock SET available_tickets = available_tickets - :q WHERE event_id = :e AND available_tickets >= :q`. Se não mudar nenhuma linha, não há bilhetes. O serviço corre com um só processo (as escritas ficam em fila) e a base de dados recusa stock negativo. 200 reservas em simultâneo com 20 bilhetes dão exatamente 20.
- **Libertação idempotente:** só a primeira repõe os bilhetes; as seguintes respondem `released: false`.
- **Libertação antes da reserva:** se a reserva demora mais do que o timeout do Order (1 s), o Order desiste e liberta "por segurança". Se essa libertação chegar primeiro, fica registada, e a reserva atrasada é recusada com 409, para os bilhetes não ficarem presos numa compra que o Order já deu como `FAILED`.
- **Stock inicial:** se a base de dados estiver vazia, carrega `inventory/data/stock.json` (a lotação de cada evento no Catalog). Depois vale o que está na base de dados; para voltar ao início, `docker compose down -v`.

| Variável               | Por omissão | Descrição                                            |
| ----------------------- | ------------ | ------------------------------------------------------ |
| `SQLITE_JOURNAL_MODE` | `WAL`      | `WAL` no cluster local; `DELETE` no global (disco NFS) |
| `WEB_CONCURRENCY`     | `1`        | Tem de ser 1                                           |

No cluster local: `kubectl port-forward svc/inventory -n ticketmei 8005:8005`.

### Verificação rápida

```bash
curl http://localhost:8000/events
curl http://localhost:8000/events/evt-101
curl -i http://localhost:8000/events/evt-999     # 404
curl http://localhost:8000/metrics
```

No Windows, no PowerShell, usar `curl.exe` em vez de `curl`.

Compras (no PowerShell):

```powershell
$compra = Invoke-RestMethod -Method Post -Uri http://localhost:8000/orders -ContentType "application/json" `
  -Body '{"customer_id":"cliente-1","customer_email":"ana@exemplo.pt","event_id":"evt-101","quantity":2}'
$compra
Invoke-RestMethod http://localhost:8000/orders/$($compra.id)
docker compose logs notification --tail 5     # "Email de confirmação enviado para ana@exemplo.pt"
```

Ou em http://localhost:8000/docs → `POST /orders` → **Try it out**.

O smoke test do CI verifica tudo isto de uma vez (no Windows, no Git Bash): `bash ci/smoke-test.sh`.

## Comunicação assíncrona (Order → RabbitMQ → Notification)

O Order e o Notification comunicam só através do RabbitMQ: o Order publica na exchange `orders.exchange` e o Notification consome a fila `order_created`, ligada a essa exchange. Nenhum dos dois chama o outro diretamente.

|              |                                                                                                                                        |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| Exchange     | `orders.exchange`, do tipo *direct*, durável. O Order e o Notification declaram-na e ligam-lhe a fila com a routing key `order.created` (as mesmas declarações nos dois: não importa qual arranca primeiro) |
| Fila         | `order_created`, durável. Os dois serviços declaram-na; declarar uma fila que já existe não faz nada.                            |
| Publicação | Exchange `orders.exchange`, `routing_key="order.created"`, mensagens persistentes, `type="OrderCreated"`, `message_id` = id da compra; só depois de a compra ficar `CONFIRMED` |
| Corpo        | JSON: `type`, `order_id`, `customer_id`, `customer_email`, `event_id`, `quantity`, `total_amount`, `status`, `created_at`        |

Garantias:

| Mecanismo                               | Onde                 | O que garante                                                                                                                                                           |
| --------------------------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Fila durável + mensagens persistentes  | RabbitMQ             | As mensagens sobrevivem a um reinício do RabbitMQ                                                                                                                      |
| *Publisher confirms*                  | Order                | O Order só considera a mensagem publicada depois de o RabbitMQ confirmar que a guardou. Se a publicação falhar, a compra (já paga) continua `CONFIRMED` e a falha conta em `order_publish_failures_total`. |
| *Ack* manual, só depois de processar | Notification         | Se o Notification for abaixo a meio, as mensagens não confirmadas voltam para a fila e são entregues de novo                                                          |
| *Prefetch*                            | Notification         | No máximo `PREFETCH_COUNT` mensagens em processamento de cada vez                                                                                                    |
| Religação automática                 | Order e Notification | Se o RabbitMQ reiniciar, os dois voltam a ligar sozinhos                                                                                                                |

A entrega é **at-least-once**: nenhuma notificação se perde, mas, se o Notification falhar no instante entre enviar o email e confirmar a mensagem, esse email pode repetir-se. Quando o Order produz compras mais depressa do que o Notification as consegue tratar, a fila cresce e depois esvazia. É a fila a absorver picos sem afetar o tempo de resposta das compras.

![Fila order_created no RabbitMQ](docs/img/rabbitmq-fila.png)

## Observabilidade

### Métricas expostas

| Serviço     | Métrica                                                                                         | Tipo      | Labels                                    |
| ------------ | ------------------------------------------------------------------------------------------------ | --------- | ----------------------------------------- |
| API Gateway  | `gateway_requests_total`                                                                       | Counter   | `method`, `endpoint`, `http_status` |
| API Gateway  | `gateway_request_duration_seconds`                                                             | Histogram | `endpoint`                              |
| Catalog      | `catalog_requests_total`                                                                       | Counter   | `endpoint`, `http_status`             |
| Catalog      | `catalog_request_duration_seconds`                                                             | Histogram | —                                        |
| cAdvisor     | `container_cpu_usage_seconds_total`, `container_memory_usage_bytes`, ...                     | —        | `name` (nome do contentor), ...         |
| Order        | `orders_initiated_total`                                                                       | Counter   | `status` (`CONFIRMED`, `CANCELLED`, `FAILED`)    |
| Order        | `circuit_breaker_state` (0 = CLOSED, 1 = OPEN, 2 = HALF_OPEN)                                   | Gauge     | —                                        |
| Order        | `circuit_breaker_fallbacks_total`                                                              | Counter   | —                                        |
| Order        | `payment_client_requests_total`                                                                | Counter   | `status` (`2xx`, `500`, `timeout`, `error`, ...) |
| Order        | `order_publish_failures_total`                                                                 | Counter   | —                                        |
| Order        | `saga_transactions_initiated_total`, `saga_transactions_success_total`, `saga_transactions_aborted_total` | Counter   | —                                        |
| Order        | `saga_compensations_executed_total`                                                            | Counter   | `outcome` (`success`, `failed`)          |
| Order        | `saga_duration_seconds`                                                                        | Histogram | `outcome` (`success`, `compensated`, `aborted`) |
| Order        | `order_requests_total`                                                                         | Counter   | `method`, `endpoint`, `http_status` |
| Order        | `order_request_duration_seconds`                                                               | Histogram | `endpoint`                              |
| Notification | `notifications_processed_total`                                                                | Counter   | `status` (`SENT`, `FAILED`)         |
| Notification | `notification_processing_duration_seconds`                                                     | Histogram | —                                        |
| Payment      | `payment_requests_total` (só `POST /payments`)                                               | Counter   | `http_status`                           |
| Payment      | `payment_processing_duration_seconds` (inclui a latência injetada; intervalos até 15 s)     | Histogram | —                                        |
| Payment      | `payment_fault_error_rate`, `payment_fault_latency_seconds` (falhas configuradas)           | Gauge     | —                                        |
| Inventory    | `inventory_reservations_total`                                                                 | Counter   | `result` (`reserved`, `insufficient`, `unknown_event`, `duplicate`, `already_released`) |
| Inventory    | `inventory_releases_total`                                                                     | Counter   | `result` (`released`, `noop`)       |
| Inventory    | `inventory_available_tickets`                                                                  | Gauge     | `event_id`                              |
| Inventory    | `inventory_requests_total`, `inventory_request_duration_seconds`                             | Counter, Histogram | `endpoint`, `http_status`      |
| RabbitMQ     | `rabbitmq_detailed_queue_messages_ready`, `..._unacked` (tamanho da fila) e métricas gerais | —        | `queue`, ...                            |

- O label `endpoint` guarda o template da rota (`/events`, `/events/{event_id}`) e não o caminho real, para que cada id não crie uma série temporal nova (cardinalidade controlada). Caminhos inexistentes ficam agrupados em `unmatched`.
- Os pedidos a `/metrics` e `/health` não são contabilizados, para que as métricas reflitam apenas tráfego de clientes.
- Os buckets do histograma do Gateway estão concentrados à volta dos 200 ms (o threshold do teste de carga), para que o p95/p99 calculado seja preciso nessa zona. Os do Catalog estão abaixo de 100 ms, porque o Catalog responde a partir de memória.

### Prometheus

O Prometheus faz *scrape* (pull) de 5 em 5 segundos dos targets definidos em `prometheus/prometheus.yml`: `api-gateway:8000`, `catalog:8001`, `order:8002`, `notification:8003`, `cadvisor:8080` e o RabbitMQ (`rabbitmq:15692`) em dois jobs: `rabbitmq` (métricas gerais, em `/metrics`) e `rabbitmq-queues` (métricas por fila, em `/metrics/detailed`). O plugin de métricas do RabbitMQ vem ativo na imagem oficial.

Em http://localhost:9090/targets os três targets devem aparecer como **UP**:

![Targets do Prometheus](docs/img/prometheus-targets.png)

### Grafana

O Grafana é configurado por ficheiros (*provisioning*), montados no contentor pelo `docker-compose.yml`:

| Ficheiro                                            | Função                                                                                                    |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `grafana/provisioning/datasources/prometheus.yml` | Cria o datasource Prometheus (`http://prometheus:9090`) com o `uid` usado pelos painéis dos dashboards |
| `grafana/provisioning/dashboards/dashboards.yml`  | Carrega automaticamente todos os dashboards de `grafana/dashboards/` para a pasta **TicketMEI**     |

Dashboards:

**TicketMei - P1** (`dashboard1.json`, fornecido pela docente): monitoriza o Gateway segundo o método RED (*Rate, Errors, Duration*) e os recursos dos contentores.

| Painel                          | Query                                                                                        | O que mostra                                       |
| ------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| Gateway Throughput (RPS)        | `sum(rate(gateway_requests_total[1m]))`                                                    | Pedidos por segundo recebidos pelo Gateway         |
| API Gateway Latency (p95 & p99) | `histogram_quantile(0.95, sum(rate(gateway_request_duration_seconds_bucket[1m])) by (le))` | Tempo abaixo do qual ficam 95% (e 99%) dos pedidos |
| Gateway HTTP Error Rate (%)     | pedidos `5xx` / total × 100                                                               | Percentagem de pedidos com erro do servidor        |
| CPU Usage by Container          | `rate(container_cpu_usage_seconds_total{name=~"api-gateway\|catalog"}[1m]) * 100`           | CPU usado por cada contentor (100% = um core)      |
| Memory Consumption              | `container_memory_usage_bytes{name=~"api-gateway\|catalog"}`                                | Memória usada por cada contentor                  |

**TicketMei - Catalog** (`catalog.json`): métricas específicas do Catalog.

| Painel                                | O que mostra                                                                               |
| ------------------------------------- | ------------------------------------------------------------------------------------------ |
| Catalog Throughput (RPS) por endpoint | Pedidos por segundo por rota                                                               |
| Catalog respostas por código HTTP    | Pedidos por segundo por código (200, 404, ...)                                            |
| Catalog Latency (p95 & p99)           | Tempo de resposta medido dentro do Catalog                                                 |
| Latência p95: Gateway vs Catalog     | A diferença entre as duas linhas é o tempo gasto no Gateway e na rede entre os serviços |

**TicketMei - Order & Notification** (`order-notification.json`): compras, fila e notificações.

| Painel                                                                                        | O que mostra                                                                                                      |
| --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Compras aceites / Notificações enviadas / Mensagens à espera                               | Totais no intervalo do dashboard e o tamanho atual da fila                                                        |
| Fila order_created: entrada vs saída                                                         | Compras publicadas pelo Order vs notificações processadas. Se a entrada estiver acima da saída, a fila cresce. |
| Mensagens na fila order_created                                                               | Mensagens em espera (*ready*) e em processamento (*unacked*), medidas pelo RabbitMQ                           |
| Compras iniciadas por estado                                                                  | `orders_initiated_total` por `CONFIRMED` / `FAILED`                                                         |
| Notificações processadas por estado                                                         | `notifications_processed_total` por `SENT` / `FAILED`                                                       |
| Latência de POST /orders (p95 & p99) e tempo de processamento das notificações (p50 & p95) |                                                                                                                   |
| Respostas HTTP do Order por código e CPU por contentor                                       |                                                                                                                   |

**RabbitMQ-Overview** (`rabbitmq-overview.json`): o dashboard oficial do RabbitMQ (grafana.com, ID 10991), na versão do RabbitMQ 3.13.7, ligado ao datasource do projeto.

![Dashboard Order & Notification](docs/img/dashboard-order-notification.png)

![Dashboard RabbitMQ-Overview](docs/img/rabbitmq-overview.png)

Todas as queries usam uma janela de 1 minuto (`[1m]`), por isso os gráficos reagem a mudanças com um atraso de até um minuto.

## Teste de carga (k6)

O k6 está no profile `testing`, por isso não arranca com `docker compose up`:

```bash
docker compose run --rm k6
```

O teste (`k6/load-test.js`) sobe até 500 utilizadores virtuais em 3 minutos, mantém durante 2 minutos e desce em 30 segundos. Cada utilizador faz `GET /events` ao Gateway e espera 100 ms, em ciclo.
Thresholds: p95 da latência < 200 ms e menos de 1% de pedidos falhados.

### Resultados

Primeira execução (fase 1), ainda com um só processo no Gateway e no Catalog: o p95 ficou acima do threshold. A correção (vários processos por contentor) está nas *Decisões técnicas*.

| Métrica         | Resultado                       |
| ---------------- | ------------------------------- |
| Pedidos totais   | 512747                          |
| Débito médio   | 1553.59 pedidos/s               |
| Latência p90    | 233.61 ms                       |
| Latência p95    | 526.3 ms (threshold: < 200 ms)  |
| Pedidos falhados | 0.00 % (threshold: < 1%)        |
| Máquina         | Laptop Ryzen 7 7735HS, 16 Cores |

![Resumo do k6](docs/img/k6-resumo.png)

![Dashboard durante o teste de carga](docs/img/dashboard.png)

![Dashboard do Catalog durante o teste de carga](docs/img/dashboard-catalog.png)

### Compras (`k6/orders-test.js`)

```bash
docker compose run --rm k6 run orders-test.js
```

Cada utilizador virtual cria uma compra através do Gateway (`POST /orders`) e consulta-a logo a seguir (`GET /orders/{id}`). Sobe até 50 utilizadores em 1 minuto, mantém durante 2 minutos e desce em 30 segundos, com 0,5 s entre compras (~100 compras/s, perto da capacidade do Notification). Thresholds: p95 < 200 ms, menos de 1% de pedidos falhados e mais de 99% das verificações certas.

Para mais carga, e para ver a fila do RabbitMQ a crescer e a esvaziar no dashboard:

```bash
docker compose run --rm k6 run --stage 30s:200,60s:200,10s:0 orders-test.js
```

| Métrica                                            | Resultado                          |
| --------------------------------------------------- | ---------------------------------- |
| Pedidos totais (POST + GET)                         | 62084                              |
| Compras criadas                                     | 31042                              |
| Latência p95 (threshold: < 200 ms)                 | 24.92 ms                           |
| Pedidos falhados (threshold: < 1%)                  | 0.00 %                             |
| Compras na base de dados = notificações enviadas? | Sim (47 Mil (Realizados 2 Testes)) |

![Resumo do k6 das compras](docs/img/k6-compras.png)

## Demonstração de falha

Com tráfego a passar pelo Gateway (por exemplo, durante o teste k6):

```bash
docker stop catalog     # o Gateway passa a responder 502 e a taxa de erros sobe no dashboard
docker start catalog    # o Gateway volta a responder 200 sem ser reiniciado
```

O Gateway continua `healthy` enquanto o Catalog está em baixo, porque o seu `/health` não depende do Catalog. Isto evita falhas em cascata (o Gateway ser reiniciado sem estar avariado).

![Taxa de erros com o Catalog parado](docs/img/dashboard-falha.png)

Fase 2:

| Ação                                       | Resultado                                                                                                                      |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `docker stop notification` e fazer compras | As compras continuam a ser aceites (201). As mensagens ficam à espera na fila e o painel "Mensagens na fila" sobe.            |
| `docker start notification`                | A fila esvazia e todas as compras recebem o email. Nenhuma se perde.                                                           |
| `docker stop rabbitmq` e fazer uma compra  | `201`, compra `CONFIRMED` sem email (`order_publish_failures_total` sobe). O Order e o Notification continuam `healthy`, com `"rabbitmq": "disconnected"` no `/health`. |
| `docker start rabbitmq`                    | O Order e o Notification voltam a ligar sozinhos e as compras voltam a gerar email                                             |
| `docker stop order` e fazer uma compra     | `502` do Gateway. Os eventos (`GET /events`) continuam a funcionar.                                                        |
| `docker compose restart order`             | As compras anteriores continuam disponíveis (base de dados num volume)                                                        |

## Decisões técnicas

- **Python com FastAPI e uvicorn** nos dois serviços.
- **`aiohttp` como cliente HTTP no Gateway.** Num teste com o script k6 e 500 utilizadores virtuais, o `httpx` ficou em 261 pedidos/s com p95 de 4,6 s; o `aiohttp` chegou aos 2 118 pedidos/s com p95 de 174 ms, na mesma máquina.
- **Uma única sessão HTTP com pool de ligações (keep-alive)** entre o Gateway e o Catalog, criada no arranque do Gateway, em vez de uma ligação nova por pedido.
- **Timeout de 2 s** nas chamadas ao Catalog. Um Catalog bloqueado nunca prende o Gateway indefinidamente.
- **Vários processos por contentor** (`WEB_CONCURRENCY`: 4 no Gateway, 2 no Catalog; o Order passou a 1 na Parte II, por causa do circuit breaker). Um processo Python só usa um core de cada vez, e com um só processo o Gateway não cumpria o p95 < 200 ms. Como as métricas do `prometheus_client` vivem na memória de cada processo, cada um escreve-as em ficheiros (`PROMETHEUS_MULTIPROC_DIR`) e o `/metrics` soma-as todas.
- **O Catalog lê e valida o `events.json` uma vez no arranque** e guarda as respostas já serializadas em JSON. Os dados são estáticos e cada pedido só devolve bytes prontos.
- **Healthchecks e `depends_on: condition: service_healthy`** para uma ordem de arranque correta.
- **Contentores com utilizador sem privilégios** e imagens `python:3.13-slim`.
- **Fase 2: SQLite para as compras**, num volume do Docker. Não precisa de mais um contentor e aguenta os 2 processos do Order em simultâneo (modo WAL). Limitação: o Order não pode ter várias réplicas.
- **Fase 2: o preço vem do Catalog** (pedido síncrono do Order ao Catalog), nunca do cliente.
- **Fase 2: `aio-pika` para o RabbitMQ**, com religação automática, *publisher confirms* no Order e *ack* manual com *prefetch* no Notification.
- **Fase 2: a compra é gravada antes de publicar a mensagem.** Na Parte I, se a publicação falhasse, ficava `FAILED` (503); na Parte II a compra já está paga quando é publicada, por isso continua `CONFIRMED` e só o email se perde. Limitação conhecida: se o Order morrer exatamente entre gravar e publicar, a compra fica `PENDING` sem mensagem (*dual write*). A solução seria o padrão *Transactional Outbox*.
- **Parte II: circuit breaker escrito à mão** (`order/circuit_breaker.py`, cerca de 80 linhas) em vez de uma biblioteca: é fácil de testar e de explicar, e dá controlo direto sobre as métricas. Retry com *full jitter* e timeout de 1 s por tentativa, para o pior caso (~3,3 s) ficar abaixo dos 5 s que o Gateway espera pelo Order.
- **Fase 2: o Gateway passa as respostas 502/503/504 dos serviços tal como vêm**, porque são o serviço a explicar o que falhou. Só um 500 (erro inesperado) é convertido em 502.
- **Pipeline de CI (GitHub Actions)**: testes de todos os serviços (com um RabbitMQ verdadeiro), build das imagens, arranque do Compose, smoke test e testes k6 com carga reduzida.

## Integração contínua (GitHub Actions)

O pipeline `.github/workflows/ci.yml` corre em cada push para o `main`, em cada Pull Request e à mão (separador **Actions** → **CI** → **Run workflow**):

1. **Testes**: um job por serviço (Catalog, API Gateway, Order, Notification), com um RabbitMQ e um Redis verdadeiros para os testes de integração.
2. **Docker Compose e k6**, só se os testes passarem:
   - valida o `docker-compose.yml` e constrói as imagens;
   - arranca os serviços e espera que fiquem `healthy`;
   - corre o smoke test (`ci/smoke-test.sh`): eventos, compras, notificação e targets do Prometheus;
   - corre os dois testes k6 com carga reduzida (as máquinas do GitHub Actions são pequenas e partilhadas). Os thresholds dos scripts continuam a aplicar-se.
   - se algum passo falhar, mostra os logs dos serviços.
3. **Manifestos Kubernetes** (fase 3): valida `k8s/`, `k8s-tools/` e `argocd/` com o `kubeconform` (esquemas oficiais do Kubernetes e da `Application` do Argo CD) e confirma que os ConfigMaps da monitorização estão em dia.

A entrega contínua (imagens no GHCR e tags em `k8s/`) está no `cd.yml`, descrita na secção da fase 3.

![Pipeline de CI](docs/img/github-actions.png)

## Desenvolvimento local (sem Docker)

Cada serviço Python pode ser corrido e testado diretamente. Exemplo para o Catalog, no Windows:

```bat
cd catalog
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements-dev.txt

rem Correr os testes
python -m pytest

rem Arrancar em modo de desenvolvimento (recarrega ao gravar)
uvicorn main:app --port 8001 --reload
```

Em Linux/macOS, a ativação do ambiente é `source .venv/bin/activate`.

O API Gateway segue os mesmos passos, na pasta `api-gateway/` e na porta 8000. Por omissão procura o Catalog em `http://localhost:8001`, por isso basta ter o Catalog a correr ao mesmo tempo. Os testes do Gateway não precisam do Catalog: usam um Catalog simulado.

## Resolução de problemas

| Sintoma                                                  | Causa provável e solução                                                                                                                                 |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Painéis do Grafana com "datasource not found"           | O provisioning não foi carregado. Ver `docker compose logs grafana` e confirmar que as pastas `grafana/provisioning` e `grafana/dashboards` existem. |
| Painéis do Gateway sem dados                            | Ver http://localhost:9090/targets. Se o `api-gateway` estiver DOWN, ver `docker compose logs api-gateway`.                                              |
| Painel de erros mostra "NaN" ou "No data"                | Normal quando não há tráfego: a query divide pelo total de pedidos, que é zero.                                                                         |
| Painéis de CPU e memória vazios                        | O cAdvisor não está a identificar os contentores. Ver http://localhost:8080 e `docker compose logs cadvisor`.                                           |
| `api-gateway` fica em `Created`                      | Está à espera de o Catalog ficar `healthy`. Ver `docker compose logs catalog`.                                                                        |
| `port is already allocated`                            | Outro processo está a usar a porta (por exemplo, um `uvicorn` local). Pará-lo antes de `docker compose up`.                                           |
| `order` ou `notification` ficam em `Created`       | Estão à espera de o `rabbitmq` ficar `healthy` (até 30 s no primeiro arranque)                                                                       |
| Todas as compras dão `503`                            | O Order não está ligado ao RabbitMQ: ver `curl http://localhost:8002/health` e `docker compose logs order`                                            |
| A fila `order_created` não esvazia                    | Ver se o Notification está ligado (`/health`) e se a fila tem 1 consumidor na interface do RabbitMQ                                                      |
| `PermissionError` nos logs do Order ou do Notification | O volume foi criado com permissões erradas: `docker compose down -v` (**apaga as compras e o log de entregas**) e `docker compose up --build -d` |
| Pipeline de CI vermelho                                  | Abrir o job que falhou no separador **Actions**. No job "Docker Compose e k6", o último passo mostra os logs de todos os serviços.                  |

## Fase 3: Kubernetes e Argo CD

A partir da Fase 3, todos os componentes correm no Kubernetes do Docker Desktop (RNF1). O `docker-compose.yml` continua no repositório para o desenvolvimento local e para o pipeline de CI.

### Recursos no cluster (namespace `ticketmei`)

| Ficheiro                  | Recursos                                             | Notas                                                                   |
| ------------------------- | ---------------------------------------------------- | ----------------------------------------------------------------------- |
| `k8s/00-base.yaml`      | Namespace, Secret `rabbitmq-credentials`           | Credenciais do RabbitMQ partilhadas pelo RabbitMQ, Order e Notification |
| `k8s/rabbitmq.yaml`     | PVC, Deployment (`Recreate`), Service LoadBalancer | Filas e mensagens persistentes sobrevivem a reinícios                  |
| `k8s/redis.yaml`        | Deployment, Service                                  | Contadores do rate limiter do Gateway (sem disco)                       |
| `k8s/catalog.yaml`      | Deployment, Service                                  |                                                                         |
| `k8s/order.yaml`        | PVC, Deployment, Service                             | Base de dados SQLite no disco persistente                               |
| `k8s/notification.yaml` | PVC, Deployment, Service                             | Log de entregas no disco persistente                                    |
| `k8s/api-gateway.yaml`  | Deployment, Service LoadBalancer                     | Única entrada dos clientes: http://localhost:8000                      |

Todos os serviços têm *readiness* e *liveness probes* no `/health`, pedidos e limites de memória, e *rolling updates* sem interrupção (`maxUnavailable: 0`).

### GitOps com Argo CD

O Argo CD vigia a pasta `k8s/` do ramo `main` (`argocd/application.yaml`) e aplica cada alteração automaticamente (`automated`), apaga o que for removido do Git (`prune`) e desfaz alterações feitas à mão no cluster (`selfHeal`). Não se faz `kubectl apply` no cluster da demonstração: muda-se o Git.

Instalação (uma vez):

```powershell
kubectl create namespace argocd
kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl apply -f argocd/application.yaml
kubectl port-forward svc/argocd-server -n argocd 8443:443   # interface em https://localhost:8443
```

### Testes de carga no cluster

```powershell
.\k8s-tools\k6.ps1 eventos   # GET /events, até 500 utilizadores virtuais
.\k8s-tools\k6.ps1 compras   # POST e GET /orders, 50 utilizadores virtuais
```

O k6 corre num Job dentro do cluster e usa o endereço interno `http://api-gateway:8000`, tal como na rede do compose.

### Correr sem o Argo CD (desenvolvimento)

```powershell
docker compose down
.\k8s-tools\build-local-images.ps1
kubectl apply -f k8s/ --recursive
kubectl get pods -n ticketmei
```

### Monitorização no Kubernetes (Prometheus, Grafana e cAdvisor)

O Prometheus, o Grafana e o cAdvisor também correm no cluster (`k8s/monitoring/`), com as mesmas versões e **os mesmos ficheiros de configuração** do compose:

| Componente      | Recursos                            | No PC                                                          |
| --------------- | ----------------------------------- | -------------------------------------------------------------- |
| Prometheus 2.50 | Deployment, Service LoadBalancer    | http://localhost:9090 (*Status* → *Targets*)              |
| Grafana 10.3    | Deployment, Service LoadBalancer    | http://localhost:3000 (admin/admin), pasta **TicketMEI** |
| cAdvisor        | DaemonSet (um pod por nó), Service | só dentro do cluster (`cadvisor:8080`)                      |

Os ficheiros `prometheus/prometheus.yml`, `grafana/provisioning/` e `grafana/dashboards/` chegam aos pods por ConfigMaps, gerados com:

```powershell
python k8s-tools/gerar-configmaps.py
```

Para mudar um dashboard ou um alvo do Prometheus, altera-se o ficheiro original, corre-se o script e faz-se commit dos dois. O `k8s/monitoring/configmaps.yaml` nunca se edita à mão, e o CI falha se não estiver em dia. O script escreve também um resumo (hash) da configuração nos Deployments do Prometheus e do Grafana, para que uma configuração nova reinicie o pod.

**cAdvisor no Kubernetes.** No Kubernetes do Docker Desktop, os pods são contentores Docker com nomes do tipo `k8s_catalog_catalog-7c8b..._ticketmei_...` e com as labels `io.kubernetes.*`. Uma regra de *relabeling* no job `cadvisor` do Prometheus põe na label `name` só o nome do contentor (`catalog`). Assim, os painéis de CPU e memória das fases 1 e 2 funcionam no compose e no Kubernetes sem alterações.

**Dashboard TicketMei - Kubernetes** (novo): alvos do Prometheus em cima e em baixo, pods a correr por serviço (num *rolling update* vê-se o serviço com 2 pods durante alguns segundos), CPU e memória por pod, e pedidos de compra aceites e rejeitados pelo rate limiter (429).

### Entrega contínua (GitHub Actions → GHCR → Argo CD)

```
push no main ─► CD (.github/workflows/cd.yml)
                 1. plano: que serviços têm código novo?
                 2. imagens: docker build + push para ghcr.io/joaopsvieira/ticketmei-<serviço>:<sha>
                 3. manifestos: muda a tag em k8s/<serviço>.yaml e faz commit no main
                                        │
                                        ▼
                Argo CD vê o commit em k8s/ ─► rolling update no cluster
```

- **Deteção de alterações por serviço:** a versão do código de um serviço é o SHA do último commit que alterou a pasta dele; se for diferente da tag no manifesto, há uma imagem nova a publicar (`ci/cd-plano.sh`). Na primeira execução as tags são `:local`, por isso são publicados os 4 serviços.
- **Tags com o SHA do commit** (nunca `latest`): cada versão no cluster corresponde a um commit, e o rollback é um `git revert`.
- **Atualização automática dos manifestos:** `ci/cd-atualizar-manifestos.sh` muda só a linha `image:` do serviço e faz commit com o `GITHUB_TOKEN`. Os commits feitos com o `GITHUB_TOKEN` não disparam outros workflows, por isso não há ciclos.
- Uma execução de cada vez (`concurrency`). Se uma falhar ou ficar por fazer, a seguinte apanha o que estiver em atraso.

### Rate limiter no API Gateway

`POST /orders` está limitado a **5 pedidos por segundo por cliente (IP)**, com rajadas até 5, com o algoritmo *token bucket*:

- os baldes estão no **Redis** (`redis:6379`), partilhados por todos os processos e réplicas do Gateway (com contadores em memória, o limite real seria 5 × 4 processos × réplicas);
- a leitura e a escrita do balde são feitas num **script Lua**, que o Redis executa de forma atómica (sem *race conditions*), com o relógio do próprio Redis;
- acima do limite o Gateway responde **429 Too Many Requests** sem chamar o Order, com os cabeçalhos `Retry-After`, `X-RateLimit-Limit` e `X-RateLimit-Remaining`;
- se o Redis falhar, os pedidos passam (*fail open*) e ficam contados em `gateway_rate_limiter_errors_total`;
- sem `REDIS_URL` (como no Docker Compose) o limite fica desligado.

Configuração em `k8s/api-gateway.yaml`: `REDIS_URL`, `RATE_LIMIT_PER_SECOND`, `RATE_LIMIT_BURST`.

Demonstração: `.\k8s-tools\k6.ps1 limite` (um cliente a tentar 20 compras por segundo durante 30 s: ~5/s aceites, o resto 429) e o dashboard **TicketMei - Kubernetes** no Grafana.
