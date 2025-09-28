import time
from typing import Dict, Any, Optional
from fastapi import FastAPI
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# --- CONFIGURATION ---
TOPIC = "user_events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
SCHEMA_FILE = "./schema/user_event.avsc"

app = FastAPI()
producer: Optional[Producer] = None
avro_serializer: Optional[AvroSerializer] = None
schema_registry_client: Optional[SchemaRegistryClient] = None

# --- SCHEMA REGISTRY FUNCTIONS ---

def user_event_to_dict(user_event: Dict[str, Any], ctx: SerializationContext) -> Dict[str, Any]:
    """
    Convert user event dict to format expected by Avro serializer.
    
    Args:
        user_event: Dictionary containing user event data
        ctx: Serialization context containing topic and message field information
        
    Returns:
        The user event dictionary in the format expected by Avro
    """
    return user_event

def initialize_kafka_components() -> None:
    """
    Initialize Kafka producer, schema registry client, and Avro serializer.
    
    This function sets up the global Kafka components needed for message production:
    - Schema Registry Client for schema management
    - Avro Serializer for message serialization
    - Kafka Producer for sending messages
    
    Returns:
        None
        
    Raises:
        FileNotFoundError: If the schema file cannot be found
        Exception: If Kafka or Schema Registry connection fails
    """
    global producer, avro_serializer, schema_registry_client
    
    # Initialize Schema Registry Client
    schema_registry_client = SchemaRegistryClient({
        'url': SCHEMA_REGISTRY_URL
    })
    
    # Read schema from file
    with open(SCHEMA_FILE, 'r') as f:
        schema_str = f.read()
    
    # Initialize Avro Serializer
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        user_event_to_dict
    )
    
    # Initialize Kafka Producer
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'user_event_producer'
    }
    producer = Producer(producer_config)
    
    print("Kafka components initialized successfully.")

# --- LIFECYCLE HOOKS ---

@app.on_event("startup")
async def startup_event() -> None:
    """
    FastAPI startup event handler that initializes Kafka components.
    
    This function is automatically called when the FastAPI application starts.
    It initializes the Kafka producer and related components needed for
    message production.
    
    Returns:
        None
    """
    initialize_kafka_components()
    print("FastAPI Producer started.")

@app.on_event("shutdown")
async def shutdown_event() -> None:
    """
    FastAPI shutdown event handler that cleans up Kafka connections.
    
    This function is automatically called when the FastAPI application shuts down.
    It ensures all pending messages are flushed before closing the producer.
    
    Returns:
        None
    """
    if producer:
        producer.flush()
        print("FastAPI Producer shut down.")

# --- API ENDPOINT ---

def delivery_callback(err: Optional[Exception], msg) -> None:
    """
    Callback function for message delivery status reporting.
    
    Args:
        err: Error object if delivery failed, None if successful
        msg: Kafka message object containing delivery information
        
    Returns:
        None
    """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

@app.post("/events/user_action")
async def produce_user_event(user_id: int, action: str, page: str) -> Dict[str, Any]:
    """
    API endpoint to produce a new user event to Kafka.
    
    This endpoint accepts user event data via HTTP POST and produces it to
    the configured Kafka topic using Avro serialization.
    
    Args:
        user_id: Unique identifier for the user performing the action
        action: The action performed by the user (e.g., 'login', 'purchase')
        page: The page or location where the action occurred
        
    Returns:
        Dictionary containing the operation status and relevant information:
        - status: 'success' or 'error'
        - message: Description of the operation result
        - data: The event data that was sent (only on success)
        
    Raises:
        Exception: Various exceptions related to serialization or Kafka production
    """
    """API endpoint to produce a new user event to Kafka."""
    
    event_data = {
        "user_id": user_id,
        "action": action,
        "page": page,
        "timestamp": int(time.time() * 1000)
    }
    
    # Create serialization context
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    
    try:
        # Serialize the data using Avro
        serialized_value = avro_serializer(event_data, ctx)
        
        # Produce the message
        producer.produce(
            topic=TOPIC,
            value=serialized_value,
            callback=delivery_callback
        )
        
        # Trigger delivery reports
        producer.poll(0)
        
        return {"status": "success", "message": "Event produced to Kafka", "data": event_data}
    
    except Exception as e:
        return {"status": "error", "message": f"Failed to produce event: {str(e)}"}