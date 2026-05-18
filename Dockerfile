FROM python:3.11-slim

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY . .

# Ensure data directories exist
RUN mkdir -p data/processed data/sample

# Default command (overridden per service in docker-compose)
CMD ["python", "webapp/app.py"]
