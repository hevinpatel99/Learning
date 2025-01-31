# Project Name: Data Pipeline System

## Project Overview

This project consists of multiple services that manage each step of a data pipeline. The pipeline performs tasks such as file ingestion, data transformation, validation, loading, and error handling. Kafka is used as the message broker for communication between the components.

### Components:

1. **File Ingestion Pipeline**
   - **Description**: This component reads `.txt` files from the source directory and streams the raw content to the `raw-text-files` Kafka topic.
   - **Kafka Topic**: `raw-text-files`
   
2. **Data Transformation Pipeline**
   - **Description**: Consumes messages from the `raw-text-files` topic, applies data cleaning, validation, and enrichment, and sends the processed data to the `sanitized-data` Kafka topic.
   - **Kafka Topic**: `sanitized-data`
   
3. **Data Validation Pipeline**
   - **Description**: Validates the schema of the incoming data, checks for required fields, and sends the validated data to the `validated-data` Kafka topic.
   - **Kafka Topic**: `validated-data`

4. **Data Loading Pipeline**
   - **Description**: Consumes messages from the `validated-data` topic and loads the data to target systems like PostgreSQL or Elasticsearch.
   - **Target Systems**: PostgresSQL, Elasticsearch
   
5. **Error Handling Pipeline**
   - **Description**: Listens to the `errors` Kafka topic and handles errors by logging them to a file or an error management system.
   - **Kafka Topic**: `errors`

---

## How It Works

The project is built on a series of interconnected pipelines that perform the following operations:

1. **File Ingestion**: 
   - A source directory containing `.txt` files is monitored.
   - The files are read and streamed to the `raw-text-files` Kafka topic.

2. **Data Transformation**: 
   - The data from `raw-text-files` is consumed, cleaned, validated, and enriched before being sent to the `sanitized-data` Kafka topic.

3. **Data Validation**: 
   - The sanitized data is validated for schema correctness and required fields, then forwarded to the `validated-data` Kafka topic.

4. **Data Loading**: 
   - Validated data is loaded into target systems such as PostgreSQL or Elasticsearch, based on predefined configurations.

5. **Error Handling**: 
   - Any errors encountered during the pipelines are forwarded to the `errors` Kafka topic and are logged into the appropriate system for further investigation.

---

## Prerequisites

- Kafka and Zookeeper
- PostgreSQL or Elasticsearch (depending on the target system)
- Python 3.x (for custom processing scripts)
- Docker (optional for containerization)

---

## Installation

### 1. Clone the repository
```bash
git clone https://github.com/your-repo/data-pipeline.git
cd data-pipeline
