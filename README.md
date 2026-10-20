# TicketMEI

Plataforma eletrónica de venda de bilhetes para eventos, baseada em microsserviços.
Projeto da unidade curricular de **Sistemas Distribuídos** — Mestrado em Engenharia Informática, Universidade da Madeira, 2026/2027.

## Grupo

| Nome | Número | GitHub |
|------|--------|--------|
| João Pedro Sá Vieira | 2103823 | [JoaoPSVieira](https://github.com/JoaoPSVieira) |
| Francisco Guilherme Fernandes Silva | 2114821 | [SkyJust4Fun](https://github.com/SkyJust4Fun) |
| Rodrigo Sousa Soares | 2075526 | [SoaresRodrigo8](https://github.com/SoaresRodrigo8) |

## Estrutura do repositório

```
.
├── api-gateway/               
├── catalog/                   
│   └── data/events.json       
├── prometheus/
│   └── prometheus.yml         
├── grafana/
│   ├── provisioning/          
│   └── dashboards/
├── k6/
│   └── load-test.js
├── docker-compose.yml
└── README.md
```

## Como correr
```bash
docker compose up --build -d # Construir e arrancar todos os serviços
docker compose ps # Ver o estado dos contentores ativos
docker compose logs -f api-gateway # Ver logs de um serviço
docker compose down # Parar tudo
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
| GET | `/events` | Lista de todos os eventos |
| GET | `/events/{id}` | Detalhes de um evento |
| GET | `/metrics` | Métricas no formato Prometheus |

### Catalog (porta 8001)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/events` | Lista todos os eventos |
| GET | `/events/{id}` | Detalhes de um evento |
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
Limites: p95 da latência < 200 ms e menos de 1% de pedidos falhados.

## Verificação rápida

```bash
curl http://localhost:8000/events
curl http://localhost:8000/events/evt-101
curl http://localhost:8001/events/evt-999     # deve dar erro 404
curl http://localhost:8000/metrics
```

No Prometheus, em http://localhost:9090/targets, os três targets (`api-gateway`, `catalog`, `cadvisor`) devem aparecer como **UP**.

## Dashboard

TODO - Colocar screenshot

<!-- ![Dashboard Grafana](docs/img/dashboard.png) -->
