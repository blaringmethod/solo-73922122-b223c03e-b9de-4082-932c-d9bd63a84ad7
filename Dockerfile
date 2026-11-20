# Lean runtime image; wheels for duckdb/uvicorn cover all native needs.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Non-root user. Observation data lives in memory only; successful result CSVs
# are retained on disk (RESULT_DIR) so history does not grow process memory.
RUN groupadd --system app && useradd --system --gid app --home-dir /app app \
    && mkdir -p /var/lib/sql-sandbox/results \
    && chown -R app:app /var/lib/sql-sandbox

ENV RESULT_DIR=/var/lib/sql-sandbox/results

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app

USER app
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=3).status==200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
