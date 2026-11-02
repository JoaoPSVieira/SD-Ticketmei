#!/usr/bin/env bash
# Smoke test do sistema completo (fases 1 e 2).
#
# Usado pelo pipeline de CI depois de "docker compose up". Também pode ser corrido à mão, com o
# compose a correr: no Windows, no Git Bash ("bash ci/smoke-test.sh").
#
# Verifica, através do Gateway, que os eventos e as compras funcionam, que o Notification recebe
# a mensagem da compra pelo RabbitMQ e que o Prometheus recolhe as métricas de todos os serviços.
set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:8000}"
NOTIFICATION="${NOTIFICATION:-http://localhost:8003}"
PROMETHEUS="${PROMETHEUS:-http://localhost:9090}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FALHOU: $*" >&2; exit 1; }
check_status() {  # check_status <descrição> <esperado> <obtido>
  if [ "$3" = "$2" ]; then echo "  ok   $1 -> $3"; else fail "$1: esperado $2, obtido $3 ($(cat "$TMP/body" 2>/dev/null))"; fi
}
json() { python3 -c "import json; print(json.load(open('$TMP/body'))$1)"; }
json_length() { python3 -c "import json; print(len(json.load(open('$TMP/body'))))"; }
sent_notifications() {
  curl -s "$NOTIFICATION/metrics" | awk '/^notifications_processed_total\{status="SENT"\}/ {print int($2)}'
}

echo "Eventos (Gateway -> Catalog)"
check_status "GET /events" 200 "$(curl -s -o "$TMP/body" -w '%{http_code}' "$GATEWAY/events")"
[ "$(json_length)" = "4" ] || fail "GET /events devia devolver 4 eventos"
check_status "GET /events/evt-101" 200 "$(curl -s -o "$TMP/body" -w '%{http_code}' "$GATEWAY/events/evt-101")"
check_status "GET /events/evt-999" 404 "$(curl -s -o "$TMP/body" -w '%{http_code}' "$GATEWAY/events/evt-999")"

echo "Compras (Gateway -> Order -> Catalog, SQLite, RabbitMQ)"
sent_before="$(sent_notifications)"
check_status "POST /orders" 202 "$(curl -s -o "$TMP/body" -w '%{http_code}' -X POST "$GATEWAY/orders" \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"ci","customer_email":"ci@exemplo.pt","event_id":"evt-101","quantity":2}')"
[ "$(json "['status']")" = "PENDING" ] || fail "a compra devia ficar PENDING"
[ "$(json "['total_amount']")" = "170.0" ] || fail "total_amount devia ser 170.0 (2 x 85)"
order_id="$(json "['id']")"
check_status "GET /orders/{id}" 200 "$(curl -s -o "$TMP/body" -w '%{http_code}' "$GATEWAY/orders/$order_id")"
check_status "POST /orders com 5 bilhetes" 422 "$(curl -s -o "$TMP/body" -w '%{http_code}' -X POST "$GATEWAY/orders" \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"ci","customer_email":"ci@exemplo.pt","event_id":"evt-101","quantity":5}')"
check_status "POST /orders com evento inexistente" 422 "$(curl -s -o "$TMP/body" -w '%{http_code}' -X POST "$GATEWAY/orders" \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"ci","customer_email":"ci@exemplo.pt","event_id":"evt-999","quantity":1}')"

echo "Notificação (Order -> RabbitMQ -> Notification)"
for _ in $(seq 1 30); do
  [ "$(sent_notifications)" -gt "$sent_before" ] && break
  sleep 1
done
[ "$(sent_notifications)" -gt "$sent_before" ] || fail "o Notification não processou a compra em 30 s"
echo "  ok   email de confirmação enviado (notifications_processed_total{status=\"SENT\"} = $(sent_notifications))"

echo "Prometheus (targets)"
expected_jobs="api-gateway catalog order notification rabbitmq rabbitmq-queues"
for _ in $(seq 1 30); do
  curl -s "$PROMETHEUS/api/v1/targets" > "$TMP/body"
  down="$(python3 -c "
import json
targets = {t['labels']['job']: t['health'] for t in json.load(open('$TMP/body'))['data']['activeTargets']}
print(' '.join(job for job in '$expected_jobs'.split() if targets.get(job) != 'up'))")"
  [ -z "$down" ] && break
  sleep 2
done
[ -z "$down" ] || fail "targets do Prometheus em baixo: $down"
echo "  ok   targets UP: $expected_jobs"

echo "Smoke test concluído com sucesso"
