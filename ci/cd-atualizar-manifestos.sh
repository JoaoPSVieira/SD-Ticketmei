#!/usr/bin/env bash
# Muda as tags das imagens em k8s/ e faz commit e push para o main (último passo do workflow CD).
# O Argo CD vê esse commit e faz o rolling update dos serviços no cluster.
#
# Recebe em $PLANO o resultado do ci/cd-plano.sh: {"include":[{"service":"catalog","tag":"<sha>"}, ...]}
# Só toca na linha "image:" da imagem do próprio serviço (ver docs/CONTRATO-FASE3.md).
set -euo pipefail

REGISTRY="${REGISTRY:-ghcr.io/joaopsvieira/ticketmei}"
BRANCH="${BRANCH:-main}"

summary=""
while read -r service tag; do
  sed -i "s|image: $REGISTRY-$service:.*|image: $REGISTRY-$service:$tag|" "k8s/$service.yaml"
  summary="$summary $service@${tag:0:7}"
done < <(jq -r '.include[] | "\(.service) \(.tag)"' <<< "$PLANO")

if git diff --quiet -- k8s/; then
  echo "As tags já estavam atualizadas: nada a fazer."
  exit 0
fi
git --no-pager diff -- k8s/

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add k8s/
git commit -m "cd: imagens novas no cluster:$summary"

# Se alguém fez push para o main entretanto, o push é recusado: junta as alterações e tenta outra vez
for attempt in 1 2 3; do
  if git push origin "HEAD:$BRANCH"; then
    echo "Tags atualizadas no $BRANCH:$summary"
    exit 0
  fi
  echo "Push recusado (tentativa $attempt): a juntar as alterações mais recentes do $BRANCH"
  git pull --rebase origin "$BRANCH"
done
echo "Erro: não foi possível fazer push das tags novas" >&2
exit 1
