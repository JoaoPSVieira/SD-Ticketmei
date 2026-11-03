#!/usr/bin/env bash
# Decide que serviços precisam de uma imagem nova (1.º passo do workflow CD).
#
# A versão do código de um serviço é o SHA do último commit que alterou a pasta dele (por exemplo
# catalog/). A versão que está no cluster é a tag da imagem em k8s/<serviço>.yaml. Se forem
# diferentes, há código novo por publicar. Na primeira execução as tags são ":local", por isso são
# publicados os 4 serviços; depois, só os que mudaram.
#
# Escreve no $GITHUB_OUTPUT (ou no ecrã, se correr fora do GitHub Actions):
#   matrix={"include":[{"service":"catalog","tag":"<sha>"}, ...]}   (os serviços a publicar)
#   count=<quantos são>
#
# Correr à mão (na raiz do repositório, por exemplo no Git Bash):   bash ci/cd-plano.sh
set -euo pipefail

SERVICES="catalog api-gateway order notification"
REGISTRY="${REGISTRY:-ghcr.io/joaopsvieira/ticketmei}"
OUTPUT="${GITHUB_OUTPUT:-/dev/stdout}"

include="[]"
for service in $SERVICES; do
  code_version="$(git log -1 --format=%H -- "$service/")"
  deployed="$(sed -n "s|^ *image: $REGISTRY-$service:\(.*\)$|\1|p" "k8s/$service.yaml")"

  if [ -z "$code_version" ]; then
    echo "Erro: não há commits na pasta $service/" >&2
    exit 1
  fi
  if [ -z "$deployed" ]; then
    echo "Erro: k8s/$service.yaml não tem a linha 'image: $REGISTRY-$service:...'" >&2
    exit 1
  fi

  if [ "$code_version" = "$deployed" ]; then
    echo "$service: em dia (${deployed:0:7})"
  else
    echo "$service: ${deployed:0:7} -> ${code_version:0:7} (imagem nova)"
    include="$(jq -c --arg service "$service" --arg tag "$code_version" '. + [{service: $service, tag: $tag}]' <<< "$include")"
  fi
done

{
  echo "matrix={\"include\":$include}"
  echo "count=$(jq length <<< "$include")"
} >> "$OUTPUT"
