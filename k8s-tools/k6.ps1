# Corre um teste de carga k6 dentro do cluster e mostra o resultado.
#
# Uso (na raiz do repositório):
#   .\k8s-tools\k6.ps1 eventos    -> k6/load-test.js   (GET /events, até 500 utilizadores virtuais, ~5,5 min)
#   .\k8s-tools\k6.ps1 compras    -> k6/orders-test.js (POST e GET /orders, 50 utilizadores virtuais, ~3,5 min)
#   .\k8s-tools\k6.ps1 limite     -> k6/rate-limit-test.js (um cliente a tentar 20 compras/s durante 30 s: 429)

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("eventos", "compras", "limite")]
    [string]$Teste
)

$job = "k6-$Teste"

# 1. Copia os scripts da pasta k6/ para o cluster (ConfigMap k6-scripts), sempre na versão atual
kubectl delete configmap k6-scripts -n ticketmei --ignore-not-found
kubectl create configmap k6-scripts --from-file=k6/ -n ticketmei
if ($LASTEXITCODE -ne 0) { exit 1 }

# 2. Um Job só corre uma vez: apaga a execução anterior, se existir. "foreground" espera até o pod
#    antigo desaparecer, para não se confundir com o pod novo no passo 4
kubectl delete job $job -n ticketmei --ignore-not-found --cascade=foreground

# 3. Arranca o teste
kubectl apply -f "k8s-tools/$job.yaml"
if ($LASTEXITCODE -ne 0) { exit 1 }

# 4. Espera que o pod arranque (até 5 min: na primeira vez descarrega a imagem do k6)
Write-Host "À espera que o pod do k6 arranque..."
$deadline = (Get-Date).AddMinutes(5)
do {
    Start-Sleep -Seconds 2
    $phase = kubectl get pods -n ticketmei -l "job-name=$job" -o "jsonpath={.items[*].status.phase}"
} while ((-not $phase -or $phase -eq "Pending") -and (Get-Date) -lt $deadline)

if (-not $phase -or $phase -eq "Pending") {
    Write-Host "O pod do k6 não arrancou. Ver o motivo com:" -ForegroundColor Red
    Write-Host "  kubectl describe pod -l job-name=$job -n ticketmei"
    exit 1
}

# 5. Mostra o output do k6 até ao fim do teste
kubectl logs -f "job/$job" -n ticketmei

# 6. Resultado: COMPLETIONS 1/1 se os thresholds passaram; "Failed" se algum falhou
kubectl get job $job -n ticketmei
