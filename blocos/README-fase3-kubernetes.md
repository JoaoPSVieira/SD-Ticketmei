## Fase 3: Kubernetes e Argo CD

A partir da Fase 3, todos os componentes correm no Kubernetes do Docker Desktop (RNF1). O `docker-compose.yml` continua no repositório para o desenvolvimento local e para o pipeline de CI.

### Recursos no cluster (namespace `ticketmei`)

| Ficheiro | Recursos | Notas |
|---|---|---|
| `k8s/00-base.yaml` | Namespace, Secret `rabbitmq-credentials` | Credenciais do RabbitMQ partilhadas pelo RabbitMQ, Order e Notification |
| `k8s/rabbitmq.yaml` | PVC, Deployment (`Recreate`), Service LoadBalancer | Filas e mensagens persistentes sobrevivem a reinícios |
| `k8s/redis.yaml` | Deployment, Service | Contadores do rate limiter do Gateway (sem disco) |
| `k8s/catalog.yaml` | Deployment, Service | |
| `k8s/order.yaml` | PVC, Deployment, Service | Base de dados SQLite no disco persistente |
| `k8s/notification.yaml` | PVC, Deployment, Service | Log de entregas no disco persistente |
| `k8s/api-gateway.yaml` | Deployment, Service LoadBalancer | Única entrada dos clientes: http://localhost:8000 |

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
