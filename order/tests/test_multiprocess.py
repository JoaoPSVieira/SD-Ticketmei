"""
Testes de integração com o Order a correr a sério com 2 processos (uvicorn --workers 2).

Confirmam que os dois processos partilham a mesma base de dados SQLite (uma compra criada por um
processo é encontrada pelo outro) e que o /metrics soma as métricas dos dois.
"""

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from prometheus_client.parser import text_string_to_metric_families

from conftest import VALID_ORDER
from rabbitmq_helpers import RABBITMQ_TEST_URL, delete_queue, rabbitmq_available, read_messages

SERVICE_DIR = Path(__file__).resolve().parent.parent
WORKERS = 2
ORDERS = 100

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="No Windows os processos do uvicorn podem ficar órfãos ao terminar o teste; corre em Linux (CI)",
)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def request(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def initiated(base_url: str, status: str) -> float:
    with urllib.request.urlopen(f"{base_url}/metrics", timeout=5) as response:
        text = response.read().decode()
    return sum(
        sample.value
        for family in text_string_to_metric_families(text)
        for sample in family.samples
        if sample.name == "orders_initiated_total" and sample.labels.get("status") == status
    )


def start_order_service(tmp_path, fake_catalog_url, rabbitmq_url, queue):
    port = free_port()
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    env = {
        **os.environ,
        "PROMETHEUS_MULTIPROC_DIR": str(metrics_dir),
        "ORDERS_DB_PATH": str(tmp_path / "orders.db"),
        "CATALOG_URL": fake_catalog_url,
        "RABBITMQ_URL": rabbitmq_url,
        "ORDER_QUEUE": queue,
    }
    # O log do Order fica num ficheiro, para ser mostrado se o teste falhar
    log_file = open(tmp_path / "order.log", "w")
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(port),
         "--workers", str(WORKERS), "--no-access-log"],
        cwd=SERVICE_DIR, env=env, stdout=log_file, stderr=subprocess.STDOUT,
    )
    process.log_path = tmp_path / "order.log"
    return process, f"http://127.0.0.1:{port}"


def order_log_tail(process, lines: int = 40) -> str:
    """Últimas linhas do log do Order (para perceber porque é que não ficou pronto)."""
    try:
        return "\n".join(process.log_path.read_text(errors="replace").splitlines()[-lines:])
    except OSError:
        return "(sem log)"


def wait_until(process, base_url: str, rabbitmq_connected: bool) -> None:
    """Espera até o serviço responder (e, se pedido, até os processos estarem ligados ao RabbitMQ)."""
    deadline = time.monotonic() + 30
    connected_checks = 0
    while time.monotonic() < deadline:
        try:
            status, health = request("GET", f"{base_url}/health")
            if status == 200 and not rabbitmq_connected:
                return
            if status == 200 and health["rabbitmq"] == "connected":
                connected_checks += 1
                if connected_checks >= 10:  # vários pedidos seguidos, para apanhar os dois processos
                    return
        except OSError:
            pass
        time.sleep(0.2)
    pytest.fail(
        f"O Order não ficou pronto com vários processos "
        f"(RabbitMQ esperado: {rabbitmq_connected}). Últimas linhas do log do Order:\n{order_log_tail(process)}"
    )


def create_orders_concurrently(base_url: str) -> list[tuple[int, dict]]:
    with ThreadPoolExecutor(max_workers=20) as pool:
        return list(pool.map(lambda _: request("POST", f"{base_url}/orders", VALID_ORDER), range(ORDERS)))


def test_workers_share_the_database_and_metrics(tmp_path, fake_catalog_url):
    # Sem RabbitMQ (porta fechada): todas as compras ficam FAILED, mas ficam todas gravadas
    process, base_url = start_order_service(
        tmp_path, fake_catalog_url, f"amqp://ticketmei:ticketmei@127.0.0.1:{free_port()}/", "nao_usada"
    )
    try:
        wait_until(process, base_url, rabbitmq_connected=False)
        results = create_orders_concurrently(base_url)

        assert [status for status, _ in results] == [503] * ORDERS
        ids = [payload["order"]["id"] for _, payload in results]
        assert len(set(ids)) == ORDERS

        # Cada compra é encontrada, seja qual for o processo que responde
        for order_id in ids:
            status, order = request("GET", f"{base_url}/orders/{order_id}")
            assert status == 200
            assert order["status"] == "FAILED"

        assert {initiated(base_url, "FAILED") for _ in range(10)} == {ORDERS}
    finally:
        process.terminate()
        process.wait(timeout=15)


@pytest.mark.skipif(not rabbitmq_available(), reason="Sem RabbitMQ acessível em RABBITMQ_TEST_URL")
def test_workers_publish_one_message_per_order(tmp_path, fake_catalog_url):
    queue = f"test_order_created_{uuid.uuid4().hex[:8]}"
    process, base_url = start_order_service(tmp_path, fake_catalog_url, RABBITMQ_TEST_URL, queue)
    try:
        wait_until(process, base_url, rabbitmq_connected=True)
        results = create_orders_concurrently(base_url)

        assert [status for status, _ in results] == [202] * ORDERS
        ids = {payload["id"] for _, payload in results}

        messages = read_messages(queue)
        assert {json.loads(m.body)["order_id"] for m in messages} == ids
        assert len(messages) == ORDERS
        assert {initiated(base_url, "PENDING") for _ in range(10)} == {ORDERS}
    finally:
        process.terminate()
        process.wait(timeout=15)
        delete_queue(queue)
