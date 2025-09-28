from typing import Dict, Any, Optional, Tuple
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# --- CONFIGURATION ---
TOPIC = "user_events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "consumer_group"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
SCHEMA_FILE = "./schema/user_event.avsc"

# --- AVRO DESERIALIZATION FUNCTION ---

def dict_to_user_event(obj: Optional[Dict[str, Any]], ctx: SerializationContext) -> Optional[Dict[str, Any]]:
    """
    Convert Avro record to user event dict.
    
    Args:
        obj: The deserialized Avro object as a dictionary, or None if no data
        ctx: Serialization context containing topic and message field information
        
    Returns:
        The user event dictionary or None if no object provided
    """
    if obj is None:
        return None
    return obj

def initialize_kafka_components() -> Tuple[Consumer, AvroDeserializer]:
    """
    Initialize Kafka consumer, schema registry client, and Avro deserializer.
    
    Returns:
        A tuple containing the configured Kafka Consumer and AvroDeserializer instances
        
    Raises:
        FileNotFoundError: If the schema file cannot be found
        Exception: If Kafka or Schema Registry connection fails
    """
    # Initialize Schema Registry Client
    schema_registry_client = SchemaRegistryClient({
        'url': SCHEMA_REGISTRY_URL
    })
    
    # Read schema from file
    with open(SCHEMA_FILE, 'r') as f:
        schema_str = f.read()
    
    # Initialize Avro Deserializer
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        schema_str,
        dict_to_user_event
    )
    
    # Initialize Kafka Consumer
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC])
    
    return consumer, avro_deserializer

# --- CONSUMER SETUP ---

def main() -> None:
    """
    Main consumer function that continuously polls for and processes Kafka messages.
    
    This function initializes the Kafka consumer and deserializer, then enters
    a polling loop to receive and process Avro-serialized messages from the
    configured topic. Messages are deserialized and their contents are printed
    to the console.
    
    Returns:
        None
        
    Raises:
        KeyboardInterrupt: When user interrupts the process with Ctrl+C
        Exception: For various Kafka or deserialization errors
    """
    consumer, avro_deserializer = initialize_kafka_components()
    
    print(f"Listening for AVRO messages on topic: {TOPIC}...")
    
    try:
        while True:
            msg = consumer.poll(1.0)  # Timeout of 1 second
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                else:
                    print(f"Consumer error: {msg.error()}")
                continue
            
            # Create deserialization context
            ctx = SerializationContext(msg.topic(), MessageField.VALUE)
            
            # Deserialize the message value
            event = avro_deserializer(msg.value(), ctx)
            
            if event is not None:
                print("-" * 30)
                print(f"Received Event: {event['action']}")
                print(f"  User ID: {event['user_id']}")
                print(f"  Page: {event['page']}")
                print(f"  Timestamp: {event['timestamp']}")
    
    except KeyboardInterrupt:
        print("\nConsumer interrupted by user")
    finally:
        consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    main()