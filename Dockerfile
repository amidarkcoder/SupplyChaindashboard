FROM apache/airflow:3.3.2

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    JAVA_HOME_PATH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"; \
    ln -s "$JAVA_HOME_PATH" /opt/java

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN curl -fsSL \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar \
    -o /opt/postgresql-42.7.7.jar \
    && chmod 644 /opt/postgresql-42.7.7.jar

USER airflow

RUN python -m pip install --no-cache-dir \
    Faker \
    python-dotenv \
    psycopg2-binary \
    "pyspark==4.0.1"
FROM apache/superset:4.1.1

USER root
RUN python -m pip install --no-cache-dir psycopg2-binary \
    && mkdir -p /app/pythonpath
USER superset