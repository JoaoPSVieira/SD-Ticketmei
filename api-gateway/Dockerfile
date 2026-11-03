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

COPY main.py rate_limiter.py ./

# Corre com um utilizador sem privilégios em vez de root
RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000

# O Docker marca o contentor como "healthy" quando /health responde.
# O k6 espera por isto antes de começar o teste de carga.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"]

# Vários processos para usar vários cores; as métricas de todos são somadas no /metrics
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus-metrics \
    WEB_CONCURRENCY=4

# Limpa a pasta das métricas a cada arranque e arranca o uvicorn com WEB_CONCURRENCY processos
CMD ["sh", "-c", "rm -rf \"$PROMETHEUS_MULTIPROC_DIR\" && mkdir -p \"$PROMETHEUS_MULTIPROC_DIR\" && exec uvicorn main:app --host 0.0.0.0 --port 8000 --workers \"$WEB_CONCURRENCY\" --no-access-log"]