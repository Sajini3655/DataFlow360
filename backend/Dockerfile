FROM python:3.12-slim
WORKDIR /app
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY backend /app/backend
COPY data /app/data
COPY warehouse /app/warehouse
COPY scripts /app/scripts
WORKDIR /app/backend
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8000"]
