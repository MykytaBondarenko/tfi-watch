FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .
COPY scripts ./scripts

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=4)"]

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
