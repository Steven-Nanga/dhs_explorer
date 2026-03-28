FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

COPY . .

RUN mkdir -p /app/data /app/logs

# For docker-compose (local), these defaults connect to the 'db' service.
# For cloud deploy, set DATABASE_URL instead and these are ignored.
ENV DHS_DB_HOST=db
ENV DHS_DB_PORT=5432
ENV DHS_DB_NAME=dhs
ENV DHS_DB_USER=postgres
ENV DHS_DB_PASSWORD=changeme
ENV DHS_SECRET=change-this-to-a-random-string
ENV DHS_PASSWORD=admin
ENV DHS_DATA_DIR=/app/data

EXPOSE 5000

CMD ["gunicorn", "-b", "0.0.0.0:5000", "-w", "2", "--timeout", "120", "webapp.app:app"]
