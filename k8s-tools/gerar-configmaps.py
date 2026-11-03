"""
Gera k8s/monitoring/configmaps.yaml a partir dos ficheiros de configuração do compose.

    python k8s-tools/gerar-configmaps.py           (no Windows também: py k8s-tools\\gerar-configmaps.py)
    python k8s-tools/gerar-configmaps.py --check   (só verifica; usado pelo CI)

O Prometheus e o Grafana usam no Kubernetes exatamente os mesmos ficheiros que no compose:

    prometheus/prometheus.yml                 -> ConfigMap prometheus-config
    grafana/provisioning/datasources/*.yml    -> ConfigMap grafana-datasources
    grafana/provisioning/dashboards/*.yml     -> ConfigMap grafana-dashboard-providers
    grafana/dashboards/*.json                 -> ConfigMap grafana-dashboards

Por isso, para mudar um dashboard ou um alvo do Prometheus, altera-se o ficheiro original e corre-se
este script; nunca se edita o configmaps.yaml à mão.

Os dashboards ficam em JSON compacto (sem espaços): o kubectl guarda uma cópia de cada recurso numa
anotação com o máximo de 256 KB, e só o dashboard oficial do RabbitMQ já tem 220 KB formatado.

O script também escreve um resumo (hash) da configuração na anotação ticketmei/config-hash dos
Deployments do Prometheus e do Grafana: quando a configuração muda, o pod é recriado e lê a nova.

Só usa a biblioteca padrão do Python.
"""

import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "k8s" / "monitoring" / "configmaps.yaml"
NAMESPACE = "ticketmei"
# O kubectl guarda uma cópia de cada recurso numa anotação de 256 KB no máximo; fica uma margem
LIMIT_KB = 240

HEADER = """\
# FICHEIRO GERADO por k8s-tools/gerar-configmaps.py: não editar à mão.
# Para mudar a configuração, altera os ficheiros originais (prometheus/prometheus.yml,
# grafana/provisioning/ e grafana/dashboards/) e volta a correr o script.
"""


def read_text(path: Path) -> str:
    # Finais de linha do Linux, para o resultado ser igual no Windows e no CI
    return path.read_text(encoding="utf-8").replace("\r\n", "\n")


def compact_json(path: Path) -> str:
    try:
        dashboard = json.loads(read_text(path))
    except json.JSONDecodeError as exc:
        sys.exit(f"Erro: {path.relative_to(ROOT)} não é JSON válido ({exc})")
    return json.dumps(dashboard, ensure_ascii=False, separators=(",", ":"))


def configmaps() -> list[tuple[str, dict[str, str]]]:
    """Lista de (nome do ConfigMap, {nome do ficheiro: conteúdo})."""
    def files(folder: str, pattern: str, reader=read_text) -> dict[str, str]:
        found = sorted((ROOT / folder).glob(pattern))
        if not found:
            sys.exit(f"Erro: não há ficheiros {pattern} em {folder}/")
        return {f.name: reader(f) for f in found}

    return [
        ("prometheus-config", {"prometheus.yml": read_text(ROOT / "prometheus" / "prometheus.yml")}),
        ("grafana-datasources", files("grafana/provisioning/datasources", "*.yml")),
        ("grafana-dashboard-providers", files("grafana/provisioning/dashboards", "*.yml")),
        ("grafana-dashboards", files("grafana/dashboards", "*.json", compact_json)),
    ]


def yaml_block(content: str, indent: str) -> str:
    """
    Conteúdo de um ficheiro como bloco literal do YAML ("|-"): o texto fica tal e qual, linha a
    linha, só sem as mudanças de linha do fim (o Prometheus e o Grafana não precisam delas).
    """
    if content[:1] in (" ", "\t"):
        sys.exit("Erro: um ficheiro começa com espaços; o bloco literal do YAML não o suporta")
    lines = content.rstrip("\n").split("\n")
    return "|-\n" + "\n".join(indent + line if line else "" for line in lines)


def render(maps: list[tuple[str, dict[str, str]]]) -> str:
    docs = []
    for name, data in maps:
        lines = [
            "apiVersion: v1",
            "kind: ConfigMap",
            "metadata:",
            f"  name: {name}",
            f"  namespace: {NAMESPACE}",
            "data:",
        ]
        for filename, content in data.items():
            lines.append(f"  {filename}: {yaml_block(content, '    ')}")
        docs.append("\n".join(lines) + "\n")
    return HEADER + "---\n".join(docs)


def config_hash(*datas: dict[str, str]) -> str:
    digest = hashlib.sha256()
    for data in datas:
        for filename, content in data.items():
            digest.update(filename.encode() + b"\0" + content.encode() + b"\0")
    return digest.hexdigest()[:12]


def with_hash(path: Path, value: str) -> str:
    text = read_text(path)
    new, count = re.subn(r'(ticketmei/config-hash: )"[0-9a-f]*"', rf'\g<1>"{value}"', text)
    if count != 1:
        sys.exit(f"Erro: {path.relative_to(ROOT)} devia ter exatamente uma linha ticketmei/config-hash")
    return new


def main() -> None:
    check = "--check" in sys.argv[1:]
    maps = configmaps()
    by_name = dict(maps)
    wanted = {
        OUTPUT: render(maps),
        ROOT / "k8s/monitoring/prometheus.yaml": with_hash(
            ROOT / "k8s/monitoring/prometheus.yaml", config_hash(by_name["prometheus-config"])
        ),
        ROOT / "k8s/monitoring/grafana.yaml": with_hash(
            ROOT / "k8s/monitoring/grafana.yaml",
            config_hash(by_name["grafana-datasources"], by_name["grafana-dashboard-providers"],
                        by_name["grafana-dashboards"]),
        ),
    }

    size_kb = len(json.dumps({"data": by_name["grafana-dashboards"]}).encode()) // 1024
    if size_kb > LIMIT_KB:
        sys.exit(f"Erro: os dashboards ocupam ~{size_kb} KB e o kubectl só aceita até ~{LIMIT_KB} KB por recurso")

    outdated = [p for p, text in wanted.items() if not p.exists() or read_text(p) != text]
    if check:
        if outdated:
            names = ", ".join(str(p.relative_to(ROOT)) for p in outdated)
            sys.exit(f"Desatualizado: {names}. Corre 'python k8s-tools/gerar-configmaps.py' e faz commit.")
        print("ConfigMaps da monitorização em dia.")
        return

    for path, text in wanted.items():
        if path in outdated:
            path.write_text(text, encoding="utf-8", newline="\n")
            print(f"Atualizado: {path.relative_to(ROOT)}")
    if not outdated:
        print("Nada a mudar: os ConfigMaps já estavam em dia.")
    print(f"Dashboards: {len(by_name['grafana-dashboards'])} ficheiros, ~{size_kb} KB (máximo: {LIMIT_KB} KB)")


if __name__ == "__main__":
    main()
