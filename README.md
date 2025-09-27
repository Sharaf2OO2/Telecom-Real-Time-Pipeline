# Telecom Real-Time Pipeline
<img width="2711" height="945" alt="Telecom Real-Time Pipeline" src="https://github.com/user-attachments/assets/b5a603b3-b106-4600-a280-acc54a657d29" />

## Project Overview

The Telecom Real-Time Pipeline is a comprehensive data processing system designed to handle telecom operations data in both real-time and batch modes. This project demonstrates a modern data engineering architecture that combines stream processing, change data capture, and orchestrated batch workflows to create a complete telecom analytics platform.

### What This Project Does

The system processes telecom data through multiple pathways:

**Real-time Stream Processing**: Handles live Call Detail Records (CDRs) and database changes, processing events as they occur for immediate analytics. The system generates synthetic CDR events representing voice calls, SMS, and data usage across a simulated network of 500 subscribers and 160 cell towers

**Change Data Capture (CDC)**: Monitors PostgreSQL database tables for changes to subscriber information, billing events, and cell tower data, streaming these changes in real-time to downstream systems.

**Batch Data Processing**: Orchestrates daily data transformations through a medallion architecture (bronze → silver → gold layers) using Apache Airflow, creating enriched analytics datasets.

### Key Use Cases

- **Network Performance Monitoring**: Track call volumes, durations, and patterns across different regions and cell towers
- **Customer Analytics**: Monitor subscriber behavior, plan usage, and billing patterns
- **Infrastructure Management**: Analyze cell tower capacity utilization and regional performance
- **Real-time Alerting**: Detect anomalies in network usage or billing events as they happen

### Technology Stack

The project showcases integration of modern data engineering tools:

- **Apache Flink**: Stream processing engine handling real-time data ingestion
- **Apache Airflow**: Workflow orchestration for batch processing
- **Debezium**: Change data capture from PostgreSQL
- **Redpanda**: Kafka-compatible message streaming
- **ClickHouse**: Analytics database for high-performance queries
- **PostgreSQL**: Source database with logical replication enabled

## Architecture Overview

This project implements a modern data pipeline architecture with the following key components:

- **Apache Airflow**: Orchestrates batch data processing workflows through bronze, silver, and gold data layers
- **Apache Flink**: Handles real-time stream processing of telecom events
- **Debezium CDC**: Captures database changes in real-time using PostgreSQL logical replication
- **Redpanda**: Kafka-compatible message broker for event streaming
- **ClickHouse**: Analytics database for storing processed telecom data

## Quick Start

### Prerequisites

- Docker and Docker Compose

### Running the Pipeline

1. **Start the infrastructure**:
   ```bash
   docker-compose up -d
   ```

2. **Access the services**:
   - Airflow Web UI: http://localhost:8080
   - Apache Flink Dashboard: http://localhost:8081
   - Redpanda Console: http://localhost:8082
   - Grafana: http://localhost:3000

3. **Configure CDC connector**:
   ```bash
   curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @debezium.json
   ```
   
4. **Submit Flink jobs**
    ```bash
    docker exec -it FLINK-JOBMANAGER-ID flink run jobs/YourFlinkJob.py
    ```

5. **Generate test data**:
   ```bash
   python Scripts/cdc_data.py  # Populate database tables
   python Scripts/producer.py  # Start CDR event generation
   ```


## Data Flow Architecture

### Batch Processing (Airflow)

<img width="989" height="284" alt="image" src="https://github.com/user-attachments/assets/8c45d1da-08e8-428a-a90c-cc15c481567f" />


The `warm_dag` orchestrates daily data transformations through a medallion architecture:

- **Bronze Layer**: Raw data ingestion from hot storage
- **Silver Layer**: Data enrichment with subscriber and tower information
- **Gold Layer**: Aggregated analytics with hourly metrics

### Real-time Processing (CDC + Flink)
<img width="1914" height="649" alt="Screenshot 2025-09-27 031434" src="https://github.com/user-attachments/assets/2db95a17-4f70-422e-95ff-081fc9bf00bb" />

Change Data Capture monitors PostgreSQL tables and streams changes to Kafka topics:

- `telecom.public.subscribers` - Customer account changes
- `telecom.public.billing_events` - Billing transactions
- `telecom.public.cell_towers` - Infrastructure updates

## Configuration

### Database Setup

The pipeline uses two PostgreSQL instances:

- **Airflow Metadata**: `postgres:5432` (airflow/airflow)
- **Telecom Data**: `telecom-postgres:5432` (admin/admin) with CDC enabled

### ClickHouse Integration

External configuration file manages ClickHouse connections:

```json
{
  "CLICKHOUSE_HOST": "your-host",
  "CLICKHOUSE_USERNAME": "username", 
  "CLICKHOUSE_PASSWORD": "password"
}
```

## Development Tools

The project includes comprehensive data generation utilities:

- **CDR Generator**: Creates realistic call detail records
- **Database Seeder**: Populates tables with test subscribers, towers, and billing events

## Monitoring

The pipeline includes comprehensive monitoring with:

- **Prometheus**: Metrics collection on port 9090
- **Grafana**: Visualization dashboard on port 3000
- **Redpanda Console**: Kafka topic management on port 8082

## Notes

This is a development configuration demonstrating modern data engineering patterns for telecom analytics. The system processes synthetic data to simulate real-world telecom operations, making it ideal for learning and experimentation with stream processing, CDC, and data orchestration technologies.

Wiki pages you might want to explore:
- [Real-time Stream Processing Jobs](https://deepwiki.com/Sharaf2OO2/Telecom-Real-Time-Pipeline/3.2-real-time-stream-processing-jobs)
- [Change Data Capture and Data Integration](https://deepwiki.com/Sharaf2OO2/Telecom-Real-Time-Pipeline/5-change-data-capture-and-data-integration)
- [Development Tools and Data Generation](https://deepwiki.com/Sharaf2OO2/Telecom-Real-Time-Pipeline/7-development-tools-and-data-generation)

## Contact

For questions or collaboration opportunities, feel free to reach out:

- [**Gmail**](mailto:sharafahmed2002@gmail.com)
- [**LinkedIn**](https://www.linkedin.com/in/sharaf-ahmed-72955b248/)
- [**X**](https://x.com/SharafAhmed_)
