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
# Stock inicial fora de /app/data: o volume montado nessa pasta escondia-o
COPY data/stock.json ./stock.json
ENV STOCK_FILE=/app/stock.json

# Utilizador sem privilégios. A pasta /app/data (onde fica a base de dados SQLite) pertence-lhe,
# para que o volume do Docker montado nessa pasta herde estas permissões e o serviço possa escrever.
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/data \
    && chown appuser /app/data
USER appuser

# Base de dados do Inventory (ficheiro SQLite dentro do volume montado em /app/data)
ENV INVENTORY_DB_PATH=/app/data/inventory.db

EXPOSE 8005

# O Docker marca o contentor como "healthy" quando /health responde
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8005/health', timeout=2)"]

# Um só processo (WEB_CONCURRENCY=1): as escritas no SQLite ficam em fila num só processo, o que é
# simples e seguro (e no cluster global o disco é NFS, onde vários escritores dão "database is locked").
# Para escalar usam-se réplicas, não processos.
ENV WEB_CONCURRENCY=1

CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port 8005 --workers \"$WEB_CONCURRENCY\" --no-access-log"]
