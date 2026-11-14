# Entorno equivalente a Colab para correr el pipeline localmente (Spark local + Python).
# Uso: ver infra/README.md
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
# jupyter no hace falta para correr el pipeline: se excluye para achicar la imagen
RUN grep -v jupyter /tmp/requirements.txt > /tmp/req.txt \
 && pip install --no-cache-dir -r /tmp/req.txt

CMD ["python", "-m", "cpa.run_pipeline", "--help"]
