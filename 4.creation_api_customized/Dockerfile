FROM python:3.7
ENV PROJECT_DIR=app_endpoint_bigquery

# Install jq
RUN apt-get update && apt-get install -y jq

RUN mkdir /$PROJECT_DIR
WORKDIR /$PROJECT_DIR
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PORT 8080

# Add Tini
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--", "python", "BigQuery_API.py"]
