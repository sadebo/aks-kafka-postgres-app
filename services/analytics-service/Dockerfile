# Use slim Python base image
FROM python:3.10-slim-bullseye

# Set working directory
WORKDIR /app

# Minimal dependencies (avoid unnecessary layers)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir fastapi uvicorn
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary confluent-kafka

# Copy application code
COPY . .

# Expose service port
EXPOSE 8080

# Run FastAPI app with Uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
