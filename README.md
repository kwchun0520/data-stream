# Data Stream

A Python-based Kafka streaming application that demonstrates real-time data processing using Apache Kafka with Avro serialization and Schema Registry integration.

## Overview

This project implements a simple Kafka streaming ecosystem using confluent-kafka-python with Avro serialization. It demonstrates modern Kafka best practices with schema evolution support, automatic schema registry integration, and robust error handling. The project includes a FastAPI-based producer for HTTP-to-Kafka message ingestion and a consumer for real-time message processing.

## Features

- **Confluent Kafka Python**: Modern Kafka client with high performance and reliability
- **Avro Serialization**: Type-safe message serialization with schema evolution support
- **Schema Registry Integration**: Automatic schema registration and management
- **REST API Producer**: FastAPI-based HTTP endpoint for producing Avro messages
- **Robust Consumer**: Event-driven consumer with proper error handling
- **Docker Setup**: Complete Kafka, ZooKeeper, and Schema Registry environment
- **Type Safety**: Strong typing with Avro schemas for data validation
- **Production Ready**: Comprehensive error handling, logging, and configuration management

## Project Structure

```
data-stream/
├── .gitignore             # Python cache and virtual environment files
├── .python-version        # Python version specification (3.13)
├── docker-compose.yaml    # Kafka, ZooKeeper, and Schema Registry setup
├── producer.py            # FastAPI-based Avro producer with Schema Registry
├── consumer.py            # Confluent Kafka consumer with Avro deserialization
├── main.py                # Main application entry point and CLI
├── schema_manager.py      # Schema Registry management utility
├── schema/                # Avro schema definitions
│   └── user_event.avsc    # UserEvent schema definition
├── pyproject.toml         # Project configuration and dependencies
├── requirements.txt       # Python dependencies (auto-generated)
├── uv.lock                # Dependency lock file
└── README.md              # This file
```

## Prerequisites

- Python 3.13+
- Docker and Docker Compose
- UV package manager (recommended) or pip

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd data-stream
   ```

2. **Set up Python environment**:
   ```bash
   # Using UV (recommended)
   uv sync

   # Or using pip
   pip install -r requirements.txt
   ```

3. **Start Kafka infrastructure**:
   ```bash
   docker-compose up -d
   ```

   This will start:
   - ZooKeeper on port 2181
   - Kafka broker on port 9092
   - Schema Registry on port 8081

## Usage

### Running the Producer

The FastAPI-based producer provides HTTP endpoints for producing Avro messages:

```bash
uv run ython main.py producer
```

This will:
- Start a FastAPI server on `http://localhost:8000`
- Provide `/events/user_action` POST endpoint for producing events
- Automatically register the Avro schema with Schema Registry
- Serialize messages using Confluent's Avro format

**Example API calls:**
```bash
# Send a login event with page information
curl -X POST "http://localhost:8000/events/user_action?user_id=123&action=login&page=homepage"

# Send a purchase event
curl -X POST "http://localhost:8000/events/user_action?user_id=456&action=purchase&page=checkout"

# Send a logout event
curl -X POST "http://localhost:8000/events/user_action?user_id=123&action=logout&page=profile"
```

**API Documentation:**
Visit `http://localhost:8000/docs` for interactive API documentation.

### Running the Consumer

The consumer processes Avro messages from the topic:

```bash
uv run python main.py consumer
```

This will:
- Listen for messages on `user_events`
- Process messages with manual offset commits
- Display message details including topic, partition, and offset
- Handle errors gracefully without crashing

### Schema Management

The project includes a comprehensive schema management utility (`schema_manager.py`) for handling Avro schemas in the Schema Registry:

```bash
# List all schemas in the registry
uv run python schema_manager.py list

# Register a new schema
uv run python schema_manager.py register user_events-value ./schema/user_event.avsc

# Update an existing schema (creates new version)
uv run python schema_manager.py update user_events-value ./schema/user_event.avsc

# Check schema compatibility before updating
uv run python schema_manager.py check-compatibility user_events-value ./schema/user_event.avsc

# Get current schema details
uv run python schema_manager.py get user_events-value

# View specific version
uv run python schema_manager.py get user_events-value 1

# Check current compatibility settings
uv run python schema_manager.py config

# Set compatibility level
uv run python schema_manager.py config BACKWARD
```


### Schema Update Process

To update your Avro schemas in the Schema Registry, follow this step-by-step process:

#### 1. Check Current Schema Status
```bash
# List all registered schemas
uv run python schema_manager.py list

# View current schema version
uv run python schema_manager.py get user_events-value latest
```

#### 2. Modify Your Schema File
Edit `schema/user_event.avsc` with your changes. For backward compatibility, always add new fields with default values:

```json
{
  "type": "record",
  "name": "UserEvent", 
  "namespace": "com.example.events",
  "fields": [
    {"name": "user_id", "type": "long", "doc": "Unique ID of the user"},
    {"name": "action", "type": "string", "doc": "The action performed"},
    {"name": "page", "type": "string", "default": "unknown", "doc": "The page"},
    {"name": "new_field", "type": "string", "default": "default_value", "doc": "New field"},
    {"name": "timestamp", "type": "long", "doc": "Time of the event"}
  ]
}
```

#### 3. Validate Compatibility
```bash
# Check if your changes are backward compatible
uv run python schema_manager.py check-compatibility user_events-value ./schema/user_event.avsc
```

#### 4. Register the New Schema Version
```bash
# Update the schema in the registry (creates new version)
uv run python schema_manager.py update user_events-value ./schema/user_event.avsc
```

#### 5. Verify the Update
```bash
# Confirm the new version was created
uv run python schema_manager.py list user_events-value

# View the new schema
uv run python schema_manager.py get user_events-value latest
```


### Testing the Application

```bash
# Test schema registration and management
uv run python schema_manager.py list
curl http://localhost:8081/subjects/user_events-value/versions

# Send test events with all fields
curl -X POST "http://localhost:8000/events/user_action?user_id=1&action=test&page=test_page"

# Check consumer output in the consumer terminal
# Verify Schema Registry integration
uv run python schema_manager.py get user_events-value latest
```

### Cleanup

```bash
# Stop containers
docker-compose down

# Remove containers and volumes
docker-compose down -v

# Remove Docker images (optional)
docker-compose down --rmi all
```

## Development

### Extending the Application

1. **Add new event types**: Modify the Avro schema in `schema/user_event.avsc` and use the schema manager to update
2. **Add new endpoints**: Extend the FastAPI producer with additional routes
3. **Add processing logic**: Enhance the consumer with custom message processing
4. **Scale horizontally**: Run multiple consumers with the same group ID
5. **Schema evolution**: Use the schema manager tools to handle schema changes safely


## Resources

- [Confluent Kafka Python Documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Avro Documentation](https://avro.apache.org/docs/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)