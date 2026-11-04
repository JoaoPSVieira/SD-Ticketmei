# TicketMEI: fechar a fase 3

Guia para quem fechar a fase 3. O código está todo no `main`; faltam três coisas, e este guia leva-te por elas:

1. **Pôr a pipeline de CD a funcionar.** O merge do PR #9 não chegou a produzir o commit com as tags novas: os manifestos em `k8s/` ainda têm `:local`. Sem isto não se cumpre o RNF4 e não se consegue mostrar na aula que "uma alteração no código chega sozinha ao cluster".
2. **Pôr o cluster a correr no teu PC com o Argo CD** e correr os três testes k6.
3. **Pôr os resultados e as capturas no README**, e fazer o merge.

Tempo estimado: 1 h 30 a 2 h, a maior parte à espera de testes e downloads.

Todos os comandos são em PowerShell, na raiz do repositório, salvo indicação em contrário. Antes de começar, em cada janela nova do PowerShell:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

---

## 0. Confirmar o ponto de partida

```powershell
git checkout main
git pull
git log --oneline -5
```

Este guia chegou ao `main` no mesmo commit que uma revisão do João: README corrigido, `k8s/api-gateway.yaml` com a configuração do limite, correção de um teste no Windows e pasta `blocos/` apagada. Confirma que estás nessa versão:

```powershell
Test-Path blocos
Select-String -Path README.md -Pattern "Por iniciar"
```

O primeiro comando tem de dizer `False` e o segundo não pode mostrar nada. Se não for assim, o teu `main` local está desatualizado: repete o `git pull`.

**Nota sobre os testes do Gateway:** são **40** (25 dos encaminhamentos e 15 do rate limiter). Um guia antigo dizia 41, por engano: contava um ficheiro de testes que nunca entrou no repositório. No Windows, com o Redis do Docker Desktop, o teste das "100 compras ao mesmo tempo" falhava por timeout; a revisão corrige isso, e passam os 40:

```powershell
docker run --rm -d -p 6379:6379 --name redis-testes redis:7.4-alpine
cd api-gateway
python -m pytest -q
cd ..
docker stop redis-testes
```

---

## 1. Pôr a CD a funcionar

### 1.1 Ver o que falhou

No GitHub: **Actions** → à esquerda, **CD**. Há duas situações:

- **Há uma execução vermelha.** Abre-a e vê qual dos três jobs tem a cruz vermelha (*Serviços a publicar*, *Imagem (...)* ou *Atualizar as tags em k8s/*). Clica nele e depois no passo vermelho, para ver as linhas do erro.
- **Não há nenhuma execução.** A CD nunca correu. Corre-a à mão: **Run workflow** → branch `main` → **Run workflow**.

### 1.2 Erros mais prováveis e como os resolver

Alguns destes passos são em *Settings* do repositório, e só o dono (o João) os pode fazer. Se não tiveres acesso a *Settings*, pede-lhe: são 2 minutos e dá para fazer pelo telemóvel.

| Onde falhou | O que aparece no erro | Solução |
|---|---|---|
| *Imagem (...)* → *Entrar no GHCR* ou *Publicar no GHCR* | `denied`, `permission_denied`, `write_package` | **João:** *Settings* → *Actions* → *General* → *Workflow permissions* → **Read and write permissions** → *Save*. Se o pacote já existir no GitHub (perfil → *Packages*), abre-o → *Package settings* → *Manage Actions access* → *Add repository* → `SD-Ticketmei` com **Write**. |
| *Atualizar as tags em k8s/* → *Commit e push das tags novas* | `Permission to JoaoPSVieira/SD-Ticketmei.git denied to github-actions[bot]` ou `403` | **João:** o mesmo *Workflow permissions* → **Read and write permissions**. |
| *Atualizar as tags em k8s/* | `GH006: Protected branch update failed` ou `GH013: Repository rule violations` | O `main` tem uma regra que obriga a Pull Request, e o bot não consegue fazer push. **João:** *Settings* → *Branches* (ou *Rules* → *Rulesets*) → a regra do `main` → desligar *Require a pull request before merging*, ou acrescentar o GitHub Actions à lista de quem a pode ignorar (*bypass list*), se aparecer lá. |
| *Serviços a publicar* | `Erro: k8s/<serviço>.yaml não tem a linha 'image: ...'` | Alguém mudou a linha `image:` de um manifesto. Tem de ser exatamente `image: ghcr.io/joaopsvieira/ticketmei-<serviço>:<tag>`. |
| *Imagem (...)* → *Construir a imagem* | erro do `docker build` | É um erro no Dockerfile ou nos ficheiros desse serviço, no commit indicado. Lê a mensagem: costuma dizer que ficheiro falta. |

Se o erro não estiver na tabela, copia as linhas vermelhas e manda-as ao João.

### 1.3 Voltar a correr

Depois de corrigir: na página da execução que falhou, **Re-run all jobs**. Em alternativa: *Actions* → *CD* → *Run workflow*.

**Como se vê que funcionou:**

1. os três jobs ficam verdes;
2. aparece um commit novo no `main`, do **github-actions[bot]**, com a mensagem `cd: imagens novas no cluster: catalog@... api-gateway@... order@... notification@...`;
3. depois de `git pull`, os manifestos têm o SHA em vez de `:local`:
   ```powershell
   git pull
   Select-String -Path k8s/*.yaml -Pattern "image: ghcr.io"
   ```

Na primeira vez são publicados os 4 serviços. Depois, só os que mudarem.

**Tira já a captura da CD:** abre a execução verde (vês os três jobs com o certo) e grava-a como `docs/img/github-cd.png` (secção 3.4 para as capturas).

### 1.4 Tornar as imagens públicas (só o João)

O GHCR cria os pacotes como privados, e o Kubernetes não consegue descarregar imagens privadas (dá `ImagePullBackOff`). **O João**, uma única vez: no GitHub, no perfil dele → **Packages** → `ticketmei-catalog` → **Package settings** (em baixo à direita) → *Danger Zone* → **Change visibility** → **Public** (escreve o nome do pacote para confirmar). O mesmo para `ticketmei-api-gateway`, `ticketmei-order` e `ticketmei-notification`.

Para confirmar, no teu PC (com o SHA que aparece no `k8s/catalog.yaml`):

```powershell
docker logout ghcr.io
docker pull ghcr.io/joaopsvieira/ticketmei-catalog:<SHA>
```

Se descarregar sem pedir login, está público.

---

## 2. O cluster no teu PC

### 2.1 Kubernetes do Docker Desktop

1. Docker Desktop → *Settings* → *Kubernetes* → **Enable Kubernetes**. Se aparecer *Cluster provisioning method*, escolhe **kubeadm** (não *kind*). *Apply & restart* e espera que o ícone do Kubernetes fique verde.
2. *Settings* → *Resources*: pelo menos 4 CPUs e 6 GB de memória.
3. Confirma:
   ```powershell
   kubectl config use-context docker-desktop
   kubectl get nodes
   ```
   Tem de aparecer `docker-desktop   Ready`.
4. Para o compose, que usa as mesmas portas: `docker compose down`.

### 2.2 As imagens dos serviços

| Situação | O que fazer |
|---|---|
| A CD funcionou e as imagens estão públicas (secção 1) | Nada: o cluster descarrega-as do GHCR. |
| A CD ainda não funciona (os manifestos têm `:local`) | Constrói as imagens no teu PC: `.\k8s-tools\build-local-images.ps1` |

### 2.3 Instalar o Argo CD

```powershell
kubectl create namespace argocd
kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl get pods -n argocd -w
```

O `--server-side --force-conflicts` é obrigatório: sem ele, um dos recursos do Argo CD é grande demais e dá o erro `metadata.annotations: Too long`. Espera 2 a 5 minutos, até os 7 pods estarem `1/1 Running`, e sai com `Ctrl + C`.

Liga a aplicação ao repositório:

```powershell
kubectl apply -f argocd/application.yaml
```

A partir daqui, o Argo CD cria tudo o que está na pasta `k8s/` do `main`, e mantém o cluster igual ao Git.

### 2.4 Abrir a interface do Argo CD

A interface do Argo CD só existe dentro do cluster. Para a abrires no browser, cria-se um túnel com `kubectl port-forward`. Esse comando fica a correr e não te devolve o prompt, por isso corre-o numa **segunda janela do PowerShell** que fica aberta (podes minimizá-la):

```powershell
kubectl port-forward svc/argocd-server -n argocd 8443:443
```

Aparece `Forwarding from 127.0.0.1:8443 -> 8080` e a janela fica assim: é normal. As linhas `Handling connection for 8443` também. Quando acabares, `Ctrl + C`.

No browser, **https://localhost:8443** (com `https`). Vai aparecer um aviso de que a ligação não é privada, porque o certificado é do próprio Argo CD: *Avançadas* → *Continuar para localhost*. O utilizador é `admin`, e a password obténs-na assim, na **primeira** janela:

```powershell
$p = kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}"
[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($p))
```

### 2.5 Confirmar que está tudo a correr

```powershell
kubectl get pods -n ticketmei -w
```

Na primeira vez demora alguns minutos, enquanto o Kubernetes descarrega as imagens. No fim têm de estar **9 pods** `1/1 Running`: `api-gateway`, `catalog`, `order`, `notification`, `rabbitmq`, `redis`, `prometheus`, `grafana` e `cadvisor`. Sai com `Ctrl + C`.

Verificações rápidas:

```powershell
curl.exe http://localhost:8000/events
```

| O quê | Onde | O que tem de aparecer |
|---|---|---|
| Gateway | `curl.exe http://localhost:8000/events` | Os 4 eventos |
| Argo CD | https://localhost:8443 → cartão **ticketmei** | **Synced** e **Healthy** |
| Prometheus | http://localhost:9090 → *Status* → *Targets* | 7 alvos **UP** (os do RabbitMQ podem demorar 1 minuto) |
| Grafana | http://localhost:3000 (admin/admin) → *Dashboards* → pasta **TicketMEI** | 5 dashboards |

---

## 3. Os três testes e as capturas

Antes de começar:
- abre o Grafana no dashboard **TicketMei - Kubernetes**, com o intervalo *Last 5 minutes* (canto superior direito);
- fecha programas pesados: o k6 corre no mesmo PC que os serviços, e um PC carregado piora a latência.

Corre os testes **um de cada vez**, à espera que cada um acabe.

### 3.1 Eventos (~5,5 min)

```powershell
.\k8s-tools\k6.ps1 eventos
```

No fim, o k6 mostra um resumo. Interessam estas linhas (os números são só para mostrar onde estão):

```
✓ http_req_duration..............: avg=12.1ms min=... med=... max=... p(90)=30.2ms p(95)=48.3ms
✓ http_req_failed................: 0.00%   ✓ 0        ✗ 412345
  http_reqs......................: 412345  1250.4/s
```

| No README | Onde está |
|---|---|
| p95 | o `p(95)=` da linha `http_req_duration` |
| pedidos falhados | a percentagem da linha `http_req_failed` (o `✓ 0` ao lado é o número de pedidos que falharam) |
| pedidos/s | o segundo número da linha `http_reqs` |

A última linha do script mostra `COMPLETIONS 1/1` se os thresholds passaram. Se o p95 passar dos 200 ms, repete com o PC mais livre. Se continuar acima, escreve o valor que deu: não se inventa resultados.

### 3.2 Compras (~3,5 min)

```powershell
.\k8s-tools\k6.ps1 compras
```

Tira os valores das linhas `compras_aceites`, `compras_limitadas_429` e `http_req_failed`. É normal haver muito mais limitadas do que aceites: os 50 utilizadores virtuais vêm todos do mesmo pod, e o limite é de 5 compras por segundo por IP.

### 3.3 Rate limiter (~1 min)

Põe o Grafana visível: é durante este teste que tiras a captura do dashboard.

```powershell
.\k8s-tools\k6.ps1 limite
```

Tira os valores de `compras_aceites` (à volta de 155) e `compras_limitadas_429` (à volta de 445).

**Se perderes o resumo** porque a janela fez scroll: o log de cada teste fica no cluster até voltares a correr esse teste.

```powershell
kubectl logs job/k6-eventos -n ticketmei --tail=40
kubectl logs job/k6-compras -n ticketmei --tail=40
kubectl logs job/k6-limite -n ticketmei --tail=40
```

### 3.4 As capturas

No Windows: `Win + Shift + S`, seleciona a zona, clica na notificação que aparece e guarda como PNG. Grava-as na pasta `docs/img/` do repositório, **com estes nomes exatos**:

| Ficheiro | O que mostrar |
|---|---|
| `docs/img/dashboard-kubernetes.png` | O dashboard **TicketMei - Kubernetes** perto do fim do teste `limite`, com o painel "Rate limiter: POST /orders aceites vs rejeitados" a mostrar as duas linhas |
| `docs/img/argocd.png` | No Argo CD, a aplicação **ticketmei** aberta, com a árvore dos recursos e **Synced** e **Healthy** no topo |
| `docs/img/github-cd.png` | Uma execução **verde** da CD no GitHub, com os três jobs (secção 1.3) |

---

## 4. Demonstração: alteração no código → cluster

Isto é o que se vai mostrar na aula. Faz um ensaio agora, para garantir que funciona do princípio ao fim. Só funciona depois de a secção 1 estar resolvida.

1. Num ramo novo, muda o título de um evento em `catalog/data/events.json`. Por exemplo, `"Coldplay - Music of the Spheres Tour"` passa a `"Coldplay - Music of the Spheres Tour 2027"` (nenhum teste depende do título).
2. Commit, push, PR para o `main`, CI verde, merge.
3. Em *Actions* → *CD*: o plano mostra `catalog: ... (imagem nova)` e os outros `em dia`. Só o Catalog é construído.
4. Quando aparecer o commit do bot, carrega em **Refresh** no Argo CD (sem isso, o Argo CD vê o commit sozinho em até 3 minutos). O Catalog faz rolling update: `kubectl get pods -n ticketmei -w` mostra o pod novo a arrancar e o antigo a terminar. No Grafana, "Pods a correr por serviço" mostra o Catalog com 2 pods durante alguns segundos.
5. `curl.exe http://localhost:8000/events/evt-101` mostra o título novo. Ninguém fez `kubectl apply`.

Podes deixar o título novo ou repor o original com outro PR. Repor é outra demonstração igual.

---

## 5. Os resultados no README

1. Abre o `README.md`. A secção da fase 3 é a última do ficheiro, por isso vais para o fim. A última linha atual começa por `Demonstração: \`.\k8s-tools\k6.ps1 limite\``.
2. Deixa uma linha em branco e cola o bloco abaixo, trocando cada `…` pelos teus valores:

   ```markdown
   ### Resultados no cluster

   | Teste | Comando | Resultado |
   |---|---|---|
   | Eventos (500 utilizadores virtuais) | `.\k8s-tools\k6.ps1 eventos` | p95 = … ms, pedidos falhados = … %, … pedidos/s |
   | Compras (50 utilizadores virtuais) | `.\k8s-tools\k6.ps1 compras` | … aceites, … limitadas (429), pedidos falhados = … % |
   | Rate limiter (20 tentativas/s durante 30 s) | `.\k8s-tools\k6.ps1 limite` | … aceites (esperado ~155), … com 429 |

   ![Argo CD com a aplicação ticketmei Synced e Healthy](docs/img/argocd.png)

   ![Execução da CD: plano, imagens e commit das tags](docs/img/github-cd.png)

   ![Dashboard TicketMei - Kubernetes durante o teste do rate limiter](docs/img/dashboard-kubernetes.png)
   ```

   Exemplo de uma linha preenchida (com os teus números, não com estes):

   ```markdown
   | Eventos (500 utilizadores virtuais) | `.\k8s-tools\k6.ps1 eventos` | p95 = 48,3 ms, pedidos falhados = 0,00 %, 1250 pedidos/s |
   ```

   Se não houver captura da CD (secção 1 por resolver), apaga a linha do `github-cd.png`: uma imagem que não existe aparece partida no GitHub.
3. Confirma que não ficou nada por preencher (não pode aparecer nada):
   ```powershell
   Select-String -Path README.md -Pattern "…"
   ```
4. No VS Code, `Ctrl + Shift + V` abre a pré-visualização: a tabela e as imagens têm de aparecer. Uma imagem partida quer dizer que o nome do ficheiro em `docs/img/` não é igual ao do link.

### Commit

Neste commit apaga-se também este guia, que já não faz falta no repositório:

```powershell
git checkout -b fase3-resultados
git add README.md docs/img/argocd.png docs/img/github-cd.png docs/img/dashboard-kubernetes.png
git rm docs/GUIA-FECHAR-FASE3.md
git commit -m "docs: resultados da fase 3 no cluster (k6, Argo CD, CD e rate limiter)"
git push -u origin fase3-resultados
```

PR para o `main`, CI verde, merge. (Este PR só mexe no README e em imagens, por isso não dispara a CD.)

---

## 6. Lista final antes de entregar

- [ ] *Actions* → **CI**: a última execução no `main` está verde.
- [ ] *Actions* → **CD**: a última execução está verde, e há um commit do **github-actions[bot]** no `main`.
- [ ] `Select-String -Path k8s/*.yaml -Pattern ":local"` não mostra nada (todas as imagens com SHA).
- [ ] Os 4 pacotes do GHCR estão **públicos**.
- [ ] Argo CD: **ticketmei** **Synced** e **Healthy**.
- [ ] `kubectl get pods -n ticketmei`: 9 pods `1/1 Running`.
- [ ] README: a tabela de resultados está preenchida, sem `…`, e as 3 imagens aparecem no GitHub.
- [ ] Não há pasta `blocos/` nem o ficheiro `docs/GUIA-FECHAR-FASE3.md` no repositório.
- [ ] O ensaio da secção 4 funcionou: uma alteração no `catalog/` chegou ao cluster sem `kubectl apply`.

---

## 7. Problemas comuns

| Sintoma | Causa provável e solução |
|---|---|
| Pods em `ImagePullBackOff` com imagens `ghcr.io/...:<SHA>` | Os pacotes ainda são privados (secção 1.4). Os pods antigos continuam a correr (o rolling update só troca quando o novo arranca), por isso o serviço não cai. Quando ficarem públicos, os pods novos arrancam sozinhos. |
| Pods em `ErrImagePull` com imagens `:local` | As imagens não foram construídas no teu PC (`.\k8s-tools\build-local-images.ps1`), ou o Kubernetes está no modo *kind* (muda para *kubeadm*). |
| `localhost:3000`, `8000` ou `9090` não abre | O pod ainda não está pronto (`kubectl get pods -n ticketmei`), ou o compose está a correr e ocupa a porta (`docker compose down`). |
| Faltam o Prometheus, o Grafana ou o cAdvisor | O Argo CD não está a sincronizar. Abre o Argo CD e vê a mensagem da aplicação, ou corre `kubectl describe application ticketmei -n argocd`. |
| `kubectl port-forward` dá erro | O Argo CD ainda não está instalado (secção 2.3), ou os pods do `argocd` ainda não estão prontos. Se o túnel cair mais tarde, volta a correr o comando. |
| Uma alteração feita com `kubectl` é desfeita sozinha | É o Argo CD a repor o que está no Git (`selfHeal`). As alterações fazem-se no Git. |
| O Argo CD não aplica um commit novo | Espera até 3 minutos ou carrega em **Refresh**. |
| `.ps1 cannot be loaded because running scripts is disabled` | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` nessa janela. |
| `.\k8s-tools\k6.ps1 limite` diz que `limite` não é válido | O teu `main` local está desatualizado: `git checkout main` e `git pull`. |
| O k6 falha os thresholds | PC sobrecarregado. Fecha programas, confirma os recursos do Docker Desktop e repete. |
