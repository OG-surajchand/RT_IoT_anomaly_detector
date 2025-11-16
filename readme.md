# RT_IoT Anomaly Detector

Real-time IoT anomaly detection using Apache Spark, Kafka, and PostgreSQL.

## Overview

This project streams IoT sensor data through Kafka, processes it with PySpark for anomaly detection, and stores results in PostgreSQL. All services run in Docker containers for easy deployment.

## Prerequisites

- Docker & Docker Compose
- Python 3.x (for local development)

## Quick Start

1. **Clone and navigate to the project:**
   ```bash
   cd RT_IoT_anomaly_detector
   ```

2. **Start all services:**
   ```bash
   docker-compose up
   ```

This will spin up:
- Kafka (message broker)
- PostgreSQL (results storage)
- Spark (stream processing engine)

3. **Monitor logs:**
   ```bash
   docker-compose logs -f spark
   ```

## Project Structure

```
├── spark/
│   ├── Dockerfile
│   └── spark_app.py          # Main anomaly detection logic
├── docker-compose.yml
└── README.md
```

## Configuration

Update the following in `spark_app.py`:
- **Kafka topic**: Topic name for sensor data
- **Kafka broker**: `kafka:9092` (default for docker-compose)
- **PostgreSQL connection**: Database credentials and table names
- **Anomaly thresholds**: Detection sensitivity parameters

## Architecture

```
IoT Sensors → Kafka → Spark Streaming → PostgreSQL/Redis
```

Spark continuously reads from Kafka, detects anomalies in real-time, and writes results to the database.

## Troubleshooting

**Spark can't connect to Kafka:**
- Ensure Kafka is running: `docker-compose ps`
- Verify topic exists or enable auto-create in Kafka config

**Permission errors:**
- Clear volumes: `docker-compose down -v`
- Rebuild: `docker-compose build --no-cache`

## Development

To modify the anomaly detection logic, edit `spark/spark_app.py` and rebuild:
```bash
docker-compose build spark
docker-compose up spark
```

## License

MIT