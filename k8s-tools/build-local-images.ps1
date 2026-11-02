# Constrói as imagens dos 4 serviços com os nomes usados nos manifestos da pasta k8s/.
#
# Uso (na raiz do repositório):   .\k8s-tools\build-local-images.ps1
#
# O Kubernetes do Docker Desktop (modo kubeadm) usa as mesmas imagens que o Docker, por isso as
# imagens construídas aqui ficam logo disponíveis para os pods, sem registo nenhum.
# Depois de a pipeline de CD (pessoa C) estar a funcionar, os manifestos passam a usar as imagens
# do GHCR com a tag do commit e este script deixa de ser necessário.

$ErrorActionPreference = "Stop"
$services = @("catalog", "api-gateway", "order", "notification")

foreach ($service in $services) {
    $image = "ghcr.io/joaopsvieira/ticketmei-${service}:local"
    Write-Host "==> $image" -ForegroundColor Cyan
    docker build -t $image "./$service"
    if ($LASTEXITCODE -ne 0) { throw "Falhou a construção da imagem de $service" }
}

Write-Host "Imagens prontas:" -ForegroundColor Green
docker images "ghcr.io/joaopsvieira/ticketmei-*"
