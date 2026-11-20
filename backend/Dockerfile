FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY scripts ./scripts

# Placeholder entrypoint; the API server arrives in Phase 2.
CMD ["python", "-c", "print('TFI backend image built; API entrypoint arrives in Phase 2')"]
