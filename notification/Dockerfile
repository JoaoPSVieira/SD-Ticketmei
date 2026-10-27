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

# Utilizador sem privilégios. A pasta /app/logs (onde fica o log de entregas) pertence-lhe,
# para que o volume do Docker montado nessa pasta herde estas permissões e o serviço possa escrever.
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/logs \
    && chown appuser /app/logs
USER appuser

# Log de entregas: uma linha JSON por notificação (order_id, email, status, reason, timestamp)
ENV NOTIFICATIONS_LOG_PATH=/app/logs/notifications.log

EXPOSE 8003

# O Docker marca o contentor como "healthy" quando /health responde
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8003/health', timeout=2)"]

# Um só processo: o paralelismo vem do prefetch (várias mensagens tratadas ao mesmo tempo no mesmo
# processo, porque o envio do email é uma espera). Para escalar, arrancam-se mais contentores
# Notification, que repartem as mensagens da mesma fila entre si (competing consumers).
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003", "--no-access-log"]
