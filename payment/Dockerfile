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

# Base de dados do Payment (ficheiro SQLite dentro do volume montado em /app/data)
ENV PAYMENTS_DB_PATH=/app/data/payments.db

EXPOSE 8004

# O Docker marca o contentor como "healthy" quando /health responde
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8004/health', timeout=2)"]

# Um só processo (WEB_CONCURRENCY=1): a configuração das falhas vive na memória do processo, e com
# vários processos o POST /payments/configure-faults só mudava o processo que recebeu o pedido.
# Para escalar usam-se réplicas, não processos.
ENV WEB_CONCURRENCY=1

CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port 8004 --workers \"$WEB_CONCURRENCY\" --no-access-log"]
