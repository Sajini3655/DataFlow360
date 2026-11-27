FROM python:3.15-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY spark/requirements.txt /app/spark/requirements.txt
RUN pip install --no-cache-dir -r /app/spark/requirements.txt
RUN python -c "import os, pyspark; os.symlink(pyspark.__path__[0], '/opt/spark')"
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="/opt/spark/bin:${PATH}"
COPY backend/pipeline.py /app/backend/pipeline.py
COPY spark /app/spark

CMD ["uvicorn", "runner:app", "--app-dir", "/app/spark", "--host", "0.0.0.0", "--port", "8083"]
