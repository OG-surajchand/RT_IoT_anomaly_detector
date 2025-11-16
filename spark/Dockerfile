
FROM apache/spark:3.4.1

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip

RUN pip install \
    redis \
    psycopg2-binary \
    pandas \
    kafka-python

# Create and fix permissions for Ivy cache directory
RUN mkdir -p /home/spark/.ivy2 && \
    chown -R 185:185 /home/spark/.ivy2

WORKDIR /app

USER 185