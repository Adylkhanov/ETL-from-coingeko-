FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirments.txt /app/requirments.txt
RUN pip install --no-cache-dir -r /app/requirments.txt

COPY app/ /app/app/

# запуск
CMD ["python", "-m", "app.main"]
