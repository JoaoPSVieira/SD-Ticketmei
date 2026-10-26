FROM python:3.13-slim

# - PYTHONDONTWRITEBYTECODE: não cria ficheiros .pyc dentro do contentor
# - PYTHONUNBUFFERED: os logs aparecem logo em "docker compose logs"
# - PIP_*: imagem mais pequena e sem avisos desnecessários
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependências primeiro e código depois, para aproveitar a cache de camadas do Docker
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY main.py .

# Utilizador sem privilégios. A pasta /app/data (onde fica a base de dados SQLite) pertence-lhe,
# para que o volume do Docker montado nessa pasta herde estas permissões e o serviço possa escrever.
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/data \
    && chown appuser /app/data
USER appuser

# Base de dados do Order (ficheiro SQLite dentro do volume montado em /app/data)
ENV ORDERS_DB_PATH=/app/data/orders.db

EXPOSE 8002

# O Docker marca o contentor como "healthy" quando /health responde
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8002/health', timeout=2)"]

# Vários processos para usar vários cores; as métricas de todos são somadas no /metrics
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus-metrics \
    WEB_CONCURRENCY=2

# Limpa a pasta das métricas a cada arranque e arranca o uvicorn com WEB_CONCURRENCY processos
CMD ["sh", "-c", "rm -rf \"$PROMETHEUS_MULTIPROC_DIR\" && mkdir -p \"$PROMETHEUS_MULTIPROC_DIR\" && exec uvicorn main:app --host 0.0.0.0 --port 8002 --workers \"$WEB_CONCURRENCY\" --no-access-log"]
