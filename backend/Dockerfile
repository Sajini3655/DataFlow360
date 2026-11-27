FROM python:3.15-slim
WORKDIR /app
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ARG INSTALL_AZURE_SDK=false
COPY backend/requirements-azure.txt /tmp/requirements-azure.txt
RUN if [ "$INSTALL_AZURE_SDK" = "true" ]; then pip install --no-cache-dir -r /tmp/requirements-azure.txt; fi
COPY backend /app/backend
COPY data /app/data
COPY warehouse /app/warehouse
COPY scripts /app/scripts
WORKDIR /app/backend
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8000"]
