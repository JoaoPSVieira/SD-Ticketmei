# TicketMEI: contrato da Fase 3 (Kubernetes, Argo CD e CD)

Este documento fixa os nomes, as portas e as regras do cluster, para que cada pessoa possa trabalhar em paralelo. **Não alterem nada daqui sem avisar o grupo.**

## Divisão do trabalho

| Pessoa | Quem | Trabalho | Merge |
|---|---|---|---|
| A | João | Manifestos Kubernetes de todos os serviços (pasta `k8s/`), RabbitMQ e Redis no cluster, Argo CD (`argocd/application.yaml`), testes k6 dentro do cluster, validação dos manifestos no CI | 1.º |
| B | Francisco | Prometheus e Grafana no cluster (pasta `k8s/monitoring/`), dashboards adaptados às métricas do Kubernetes | 2.º ou 3.º |
| C | Rodrigo | Pipeline de CD (imagens no GHCR com a tag do commit e atualização automática das tags em `k8s/`), rate limiter com Redis no Gateway (HTTP 429), k6 adaptado ao 429 | 2.º ou 3.º |

B e C começam a partir do `main` **depois** do merge da pessoa A, porque precisam da pasta `k8s/`.

## O que fica no repositório depois do merge da pessoa A

```
k8s/                      <- o Argo CD sincroniza esta pasta (e subpastas) com o cluster
  00-base.yaml            namespace ticketmei + Secret rabbitmq-credentials
  rabbitmq.yaml           RabbitMQ (disco persistente, Service LoadBalancer)
  redis.yaml              Redis para o rate limiter (sem disco)
  catalog.yaml
  api-gateway.yaml        Service LoadBalancer (localhost:8000)
  order.yaml              disco persistente para o SQLite
  notification.yaml       disco persistente para o log de entregas
argocd/application.yaml   Application do Argo CD (aplicada uma vez, à mão)
k8s-tools/                fora da pasta k8s/: o Argo CD não lhe toca
  build-local-images.ps1  constrói as imagens com a tag :local
  k6.ps1                  corre os testes k6 dentro do cluster (Jobs)
  k6-eventos.yaml, k6-compras.yaml
```

## Nomes e portas no cluster (namespace `ticketmei`)

Os nomes são os mesmos do `docker-compose.yml`, por isso os URLs entre serviços não mudam.

| Service | Endereço dentro do cluster | Tipo | No PC (Docker Desktop) |
|---|---|---|---|
| api-gateway | `http://api-gateway:8000` | LoadBalancer | http://localhost:8000 |
| catalog | `http://catalog:8001` | ClusterIP | (só com `kubectl port-forward`) |
| order | `http://order:8002` | ClusterIP | (só com `kubectl port-forward`) |
| notification | `http://notification:8003` | ClusterIP | (só com `kubectl port-forward`) |
| rabbitmq | `amqp://ticketmei:ticketmei@rabbitmq:5672/` | LoadBalancer | localhost:5672 |
| rabbitmq (interface web) | `http://rabbitmq:15672` | LoadBalancer | http://localhost:15672 |
| rabbitmq (métricas) | `http://rabbitmq:15692/metrics` | LoadBalancer | http://localhost:15692/metrics |
| redis | `redis://redis:6379/0` | ClusterIP | (só com `kubectl port-forward`) |
| Argo CD | | | https://localhost:8443 (com `kubectl port-forward`) |
| **prometheus (pessoa B)** | `http://prometheus:9090` | LoadBalancer | http://localhost:9090 |
| **grafana (pessoa B)** | `http://grafana:3000` | LoadBalancer | http://localhost:3000 |

Portas em `localhost` já ocupadas pelo cluster: 8000, 5672, 15672, 15692 (e 9090 e 3000 pela pessoa B). Antes de usar o cluster, o compose tem de estar parado (`docker compose down`), senão as portas ficam em conflito.

Todos os pods têm `enableServiceLinks: false`: o Kubernetes **não** injeta as variáveis antigas do tipo `REDIS_PORT=tcp://10.x.x.x:6379`. Os serviços encontram-se só pelo nome (DNS). Usem sempre URLs completos em variáveis de ambiente (`REDIS_URL`, `CATALOG_URL`, ...).

## Imagens e tags (pessoa C)

- Nome das imagens: `ghcr.io/joaopsvieira/ticketmei-<serviço>`, com `<serviço>` = `catalog`, `api-gateway`, `order`, `notification` (o mesmo nome da pasta e do ficheiro `k8s/<serviço>.yaml`). O GHCR exige o dono em minúsculas.
- Cada ficheiro `k8s/<serviço>.yaml` tem **exatamente uma** linha com a imagem do próprio serviço, por exemplo:
  ```yaml
          image: ghcr.io/joaopsvieira/ticketmei-catalog:local
  ```
  A pipeline altera só a tag. Este comando foi testado nos quatro ficheiros e não toca em mais nenhuma linha (as imagens `busybox`, `rabbitmq` e `redis` ficam iguais):
  ```bash
  sed -i "s|image: ghcr.io/joaopsvieira/ticketmei-${SERVICE}:.*|image: ghcr.io/joaopsvieira/ticketmei-${SERVICE}:${GITHUB_SHA}|" "k8s/${SERVICE}.yaml"
  ```
- `:local` é a tag das imagens construídas no PC com `k8s-tools/build-local-images.ps1` (o Kubernetes do Docker Desktop usa as imagens do Docker). **A primeira execução da pipeline tem de construir os 4 serviços** (por exemplo com `workflow_dispatch`), para que nenhum manifesto fique com `:local`. A partir daí, só os serviços alterados.
- `imagePullPolicy: IfNotPresent` serve para as duas situações: a tag do SHA é única, por isso não há imagens "velhas" com o mesmo nome.
- Depois da primeira publicação, o João torna os 4 pacotes públicos no GitHub (perfil → *Packages* → pacote → *Package settings* → *Change visibility* → *Public*). Pacotes privados dão `ImagePullBackOff` no cluster.
- O commit com as tags novas é feito pelo `GITHUB_TOKEN` (`permissions: contents: write` e `packages: write` no workflow). Os commits feitos com o `GITHUB_TOKEN` não disparam outros workflows, por isso não há ciclo infinito. Se o `main` tiver regras de proteção que obriguem a Pull Request, o push da pipeline é recusado: nesse caso falem com o João.
- O workflow da CD fica num ficheiro novo, `.github/workflows/cd.yml`. O `ci.yml` continua a correr os testes; o job "Manifestos Kubernetes" (da pessoa A) valida a pasta `k8s/`.

## Redis e rate limiter (pessoa C)

- O Gateway já tem `REDIS_URL=redis://redis:6379/0` no `k8s/api-gateway.yaml`. Leiam essa variável e não acrescentem `REDIS_HOST`/`REDIS_PORT`.
- O Redis não tem disco: os contadores perdem-se se o pod reiniciar, o que não faz mal (são de 1 segundo).
- **O `docker-compose.yml` não tem Redis**, e o job `compose` do CI usa o compose. Sem `REDIS_URL` (ou com o Redis em baixo), o Gateway tem de continuar a responder: ou o limite fica desligado, ou deixa passar o pedido (*fail open*). Senão o CI fica vermelho.
- O teste `k6/orders-test.js` (50 utilizadores virtuais, todos do mesmo pod) vai receber 429 com um limite de 5 pedidos/s por IP. Ajustem o script (ou criem um script próprio para mostrar o 429) para os checks e os thresholds não falharem por causa do limite.
- IP do cliente: dentro do cluster é o IP do pod que faz o pedido (por exemplo, o do k6). Os pedidos que vêm de `localhost:8000` chegam todos com o mesmo IP interno do Docker Desktop, o que chega para a demonstração.

## Observabilidade (pessoa B)

- Os manifestos ficam em `k8s/monitoring/` (o Argo CD inclui as subpastas), no namespace `ticketmei`.
- Alvos do Prometheus (uma réplica por serviço, por isso o nome do Service chega): `api-gateway:8000`, `catalog:8001`, `order:8002`, `notification:8003`, `rabbitmq:15692` (e `/metrics/detailed?family=queue_coarse_metrics` para as filas, como no compose).
- No Kubernetes não há contentor cAdvisor à parte: o kubelet de cada nó já o tem (`/metrics/cadvisor`). As labels mudam (deixa de existir `container_label_com_docker_compose_service`; passam a existir `namespace`, `pod`, `container`), por isso os painéis de CPU e memória têm de ser adaptados.
- **Não ponham ficheiros `.json` soltos dentro de `k8s/`**: o Argo CD tenta ler todos os `.yaml` e `.json` da pasta como recursos Kubernetes e a sincronização falha. Os dashboards vão para dentro de um ConfigMap, gerado a partir da pasta `grafana/dashboards/`:
  ```bash
  kubectl create configmap grafana-dashboards --from-file=grafana/dashboards/ -n ticketmei --dry-run=client -o yaml > k8s/monitoring/grafana-dashboards.yaml
  ```
- O dashboard oficial do RabbitMQ tem mais de 200 KB, e um recurso com mais de 256 KB não pode ser aplicado da forma normal (erro `metadata.annotations: Too long`). Ponham esta anotação no ConfigMap dos dashboards:
  ```yaml
  metadata:
    annotations:
      argocd.argoproj.io/sync-options: ServerSideApply=true
  ```
  e, quando o aplicarem à mão, usem `kubectl apply --server-side -f ...`.

## Regras para todos

1. **O Git manda.** O cluster da demonstração é gerido pelo Argo CD com `selfHeal`: qualquer alteração feita à mão (`kubectl edit`, `kubectl scale`, ...) é desfeita em segundos. Para mudar alguma coisa, altera-se o ficheiro em `k8s/` e faz-se merge para o `main`.
2. Para testar no vosso PC sem Argo CD: `.\k8s-tools\build-local-images.ps1` e depois `kubectl apply -f k8s/ --recursive`. Para limpar: `kubectl delete -f k8s/ --recursive` (apaga também os dados).
3. O job "Manifestos Kubernetes" do CI tem de estar verde antes do merge.
4. Não mudem nomes de Services, portas nem nomes de imagens sem avisar: a pipeline, o Prometheus e o k6 dependem deles.
