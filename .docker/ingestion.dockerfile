FROM python:sha256:d67a7b66b989ad6b6d6b10d428dcc5e0bfc3e5f88906e67d490c4d3daac57047

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /app

COPY pyproject.toml uv.lock Makefile ./
COPY src/ ./src/

RUN make install-dev-env

COPY .docker/ingest_data.py ingest_data.py
COPY ./data/preprocessed data

CMD ["python", "ingest_data.py"]
