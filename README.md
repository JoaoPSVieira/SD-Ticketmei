# TicketMEI

Plataforma eletrónica de venda de bilhetes para eventos, baseada em microsserviços.
Projeto da unidade curricular de **Sistemas Distribuídos** — Mestrado em Engenharia Informática, Universidade da Madeira, 2026/2027.

## Grupo

| Nome | Número | GitHub |
|------|--------|--------|
| _Nome 1_ | _00000_ | [@utilizador1](https://github.com/utilizador1) |
| _Nome 2_ | _00000_ | [@utilizador2](https://github.com/utilizador2) |
| _Nome 3_ | _00000_ | [@utilizador3](https://github.com/utilizador3) |

## Estado das fases

| Fase | Prazo | Conteúdo | Estado |
|------|-------|----------|--------|
| 1 | 21/10/2026 | API Gateway + Catalog, Docker Compose, Prometheus, Grafana | Em curso |
| 2 | 28/10/2026 | Order + Notification, RabbitMQ | Por iniciar |
| 3 | 04/11/2026 | Kubernetes, GitHub Actions + GHCR, ArgoCD, rate limiter com Redis | Por iniciar |

## Arquitetura (Fase 1)

```
Cliente / k6 ──HTTP──▶ API Gateway (:8000) ──HTTP síncrono──▶ Catalog (:8001)
                              │                                    │
                              └──────── /metrics ◀── Prometheus ──▶┘
                                                       ▲    │
                                       cAdvisor (:8080)┘    ▼
                                                         Grafana (:3000)
```

- Todo o tráfego dos clientes entra pelo **API Gateway**.
- O **Catalog** é o único serviço com acesso aos dados dos eventos (`catalog/data/events.json`).
- O **Prometheus** recolhe métricas de 5 em 5 segundos do endpoint `/metrics` de cada serviço e do cAdvisor.
- O **Grafana** arranca já com o datasource e o dashboard "TicketMei - P1" configurados (provisioning).

## Estrutura do repositório

```
.
├── api-gateway/               # Serviço API Gateway (Python)
├── catalog/                   # Serviço Catalog (Python)
│   └── data/events.json       # Dados dos eventos (exclusivos do Catalog)
├── prometheus/
│   └── prometheus.yml         # Configuração de scraping
├── grafana/
│   ├── provisioning/          # Datasource e provider de dashboards
│   └── dashboards/            # dashboard1.json
├── k6/
│   └── load-test.js           # Teste de carga
├── docker-compose.yml
└── README.md
```

## Como correr

Requisitos: [Docker Desktop](https://www.docker.com/products/docker-desktop/) com Docker Compose v2.

```bash
# Construir e arrancar todos os serviços
docker compose up --build -d

# Ver o estado dos contentores
docker compose ps

# Ver logs de um serviço
docker compose logs -f api-gateway

# Parar tudo
docker compose down
```

| Serviço | URL |
|---------|-----|
| API Gateway | http://localhost:8000 |
| Catalog | http://localhost:8001 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin / admin) |
| cAdvisor | http://localhost:8080 |

## Endpoints

### API Gateway (porta 8000)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/events` | Lista todos os eventos (encaminha para o Catalog) |
| GET | `/events/{id}` | Detalhes de um evento (encaminha para o Catalog) |
| GET | `/metrics` | Métricas no formato Prometheus |

### Catalog (porta 8001)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/events` | Lista todos os eventos |
| GET | `/events/{id}` | Detalhes de um evento (404 se não existir) |
| GET | `/metrics` | Métricas no formato Prometheus |

## Métricas

| Serviço | Métrica | Tipo | Labels |
|---------|---------|------|--------|
| API Gateway | `gateway_requests_total` | Counter | `method`, `endpoint`, `http_status` |
| API Gateway | `gateway_request_duration_seconds` | Histogram | `endpoint` |
| Catalog | `catalog_requests_total` | Counter | `endpoint`, `http_status` |
| Catalog | `catalog_request_duration_seconds` | Histogram | — |

## Teste de carga (k6)

O k6 está no profile `testing`, por isso não arranca com `docker compose up`.

```bash
docker compose run --rm k6
```

O teste sobe até 500 utilizadores virtuais em 3 minutos, mantém durante 2 minutos e desce em 30 segundos.
Thresholds: p95 da latência < 200 ms e menos de 1% de pedidos falhados.

## Verificação rápida

```bash
curl http://localhost:8000/events
curl http://localhost:8000/events/evt-101
curl http://localhost:8001/events/evt-999     # deve devolver 404
curl http://localhost:8000/metrics
```

No Prometheus, em http://localhost:9090/targets, os três targets (`api-gateway`, `catalog`, `cadvisor`) devem aparecer como **UP**.

## Dashboard

_Screenshot do dashboard durante o teste de carga:_

<!-- ![Dashboard Grafana](docs/img/dashboard.png) -->
