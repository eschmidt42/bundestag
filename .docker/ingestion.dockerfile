FROM python:sha256:d67a7b66b989ad6b6d6b10d428dcc5e0bfc3e5f88906e67d490c4d3daac57047

RUN pip3 install pip==23.0.1 setuptools==67.6.1 wheel==0.40.0
WORKDIR /app
COPY .config/database-requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY .docker/ingest_data.py ingest_data.py
COPY ./data/preprocessed data

ENTRYPOINT ["python"]
CMD ["ingest_data.py"]
