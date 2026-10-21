# TicketMEI

Plataforma eletrónica de venda de bilhetes para eventos, baseada em microsserviços.
Projeto da unidade curricular de **Sistemas Distribuídos** — Mestrado em Engenharia Informática, Universidade da Madeira, 2026/2027.

## Grupo

| Nome                                | Número | GitHub                                             |
| ----------------------------------- | ------- | -------------------------------------------------- |
| João Pedro Sá Vieira              | 2103823 | [JoaoPSVieira](https://github.com/JoaoPSVieira)     |
| Francisco Guilherme Fernandes Silva | 2114821 | [SkyJust4Fun](https://github.com/SkyJust4Fun)       |
| Rodrigo Sousa Soares                | 2075526 | [SoaresRodrigo8](https://github.com/SoaresRodrigo8) |

## Estado das fases

| Fase | Prazo      | Conteúdo                                                         | Estado      |
| ---- | ---------- | ----------------------------------------------------------------- | ----------- |
| 1    | 21/10/2026 | API Gateway + Catalog, Docker Compose, Prometheus, Grafana        | Em curso    |
| 2    | 28/10/2026 | Order + Notification, RabbitMQ                                    | Por iniciar |
| 3    | 04/11/2026 | Kubernetes, GitHub Actions + GHCR, ArgoCD, rate limiter com Redis | Por iniciar |

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
├── docs/
│   └── img/dashboard.png
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

O API Gateway só arranca depois de o Catalog estar `healthy`, e o k6 só arranca depois de o API Gateway estar `healthy`.

| Serviço    | URL                                   |
| ----------- | ------------------------------------- |
| API Gateway | http://localhost:8000                 |
| Catalog     | http://localhost:8001                 |
| Prometheus  | http://localhost:9090                 |
| Grafana     | http://localhost:3000 (admin / admin) |
| cAdvisor    | http://localhost:8080                 |

## Endpoints

### API Gateway (porta 8000)

| Método | Endpoint         | Descrição                                          |
| ------- | ---------------- | ---------------------------------------------------- |
| GET     | `/events`      | Lista de todos os eventos (encaminha para o Catalog) |
| GET     | `/events/{id}` | Detalhes de um evento (encaminha para o Catalog)     |
| GET     | `/health`      | Estado do próprio Gateway (não depende do Catalog) |
| GET     | `/metrics`     | Métricas no formato Prometheus                      |
| GET     | `/docs`        | Documentação interativa da API (Swagger UI)        |

Comportamento quando o Catalog falha:

| Situação                                                  | Resposta do Gateway                |
| ----------------------------------------------------------- | ---------------------------------- |
| Catalog responde 2xx ou 4xx (ex.: 404)                      | A mesma resposta, sem alterações |
| Catalog responde 5xx                                        | `502 Bad Gateway`                |
| Catalog inacessível (contentor parado, ligação recusada) | `502 Bad Gateway`                |
| Catalog não responde dentro de `CATALOG_TIMEOUT_SECONDS` | `504 Gateway Timeout`            |

Configuração (variáveis de ambiente):

| Variável                   | Por omissão              | Descrição                                                |
| --------------------------- | ------------------------- | ---------------------------------------------------------- |
| `CATALOG_URL`             | `http://localhost:8001` | Endereço do Catalog (no Compose: `http://catalog:8001`) |
| `CATALOG_TIMEOUT_SECONDS` | `2`                     | Tempo máximo à espera do Catalog                         |
| `CATALOG_MAX_CONNECTIONS` | `100`                   | Ligações TCP reutilizáveis para o Catalog               |

### Catalog (porta 8001)

| Método | Endpoint         | Descrição                     |
| ------- | ---------------- | ------------------------------- |
| GET     | `/events`      | Lista todos os eventos          |
| GET     | `/events/{id}` | Detalhes de um evento           |
| GET     | `/metrics`     | Métricas no formato Prometheus |

## Desenvolvimento local (sem Docker)

Cada serviço pode ser corrido e testado diretamente com Python. Exemplo para o API Gateway, no Windows:

```bat
cd api-gateway
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements-dev.txt

rem Correr os testes (não precisam do Catalog: usam um Catalog simulado)
python -m pytest

rem Arrancar em modo de desenvolvimento (recarrega ao gravar)
uvicorn main:app --port 8000 --reload
```

Por omissão o Gateway procura o Catalog em `http://localhost:8001`, por isso basta ter o Catalog a correr ao mesmo tempo (mesmos passos, na pasta `catalog/` e na porta 8001).

## Métricas

| Serviço    | Métrica                             | Tipo      | Labels                                    |
| ----------- | ------------------------------------ | --------- | ----------------------------------------- |
| API Gateway | `gateway_requests_total`           | Counter   | `method`, `endpoint`, `http_status` |
| API Gateway | `gateway_request_duration_seconds` | Histogram | `endpoint`                              |
| Catalog     | `catalog_requests_total`           | Counter   | `endpoint`, `http_status`             |
| Catalog     | `catalog_request_duration_seconds` | Histogram | —                                        |

O label `endpoint` guarda o template da rota (`/events`, `/events/{event_id}`) e não o caminho real, para que cada id não crie uma série temporal nova. Caminhos inexistentes ficam agrupados em `unmatched`. Os pedidos a `/metrics` e `/health` não são contabilizados.

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

Dashboard "TicketMei - P1" durante o teste de carga k6 (500 utilizadores virtuais):

![Dashboard Grafana](docs/img/dashboard.png)

Resultado do teste: ~2340 pedidos/s, p95 = 132 ms (< 200 ms) e 0% de pedidos falhados (770 828 pedidos).
