FROM python:3.9.16-slim

RUN pip3 install pip==23.0.1 setuptools==67.6.1 wheel==0.40.0
WORKDIR /app
COPY .config/database-requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY .docker/ingest_data.py ingest_data.py
COPY ./data/preprocessed data

ENTRYPOINT ["python"]
CMD ["ingest_data.py"]
