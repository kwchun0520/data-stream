"""
Data Stream Application - Kafka with Avro Serialization

This application demonstrates:
- Kafka producer and consumer using confluent-kafka-python
- Avro serialization/deserialization with Schema Registry
- FastAPI for HTTP endpoints

Usage:
    uv run python main.py producer  # Start the FastAPI producer server
    uv run python main.py consumer  # Start the Kafka consumer
"""

import sys
import os
from typing import NoReturn

def run_producer() -> NoReturn:
    """
    Start the FastAPI producer server using uvicorn.
    
    This function starts a FastAPI server that provides HTTP endpoints
    for producing messages to Kafka. The server runs with hot-reload
    enabled for development.
    
    Returns:
        NoReturn: This function runs indefinitely until interrupted
        
    Note:
        The server will run on host 0.0.0.0 and port 8000
        API documentation is available at /docs endpoint
    """
    print("Starting FastAPI producer server...")
    print("API will be available at: http://localhost:8000")
    print("Docs available at: http://localhost:8000/docs")
    print("\nTo produce a message, POST to:")
    print("curl -X POST 'http://localhost:8000/events/user_action?user_id=123&action=login'")
    print("\nPress Ctrl+C to stop the server\n")
    
    os.system("uvicorn producer:app --host 0.0.0.0 --port 8000 --reload")

def run_consumer() -> None:
    """
    Start the Kafka consumer to process messages from the topic.
    
    This function starts a Kafka consumer that continuously polls
    for messages from the configured topic and processes them using
    Avro deserialization.
    
    Returns:
        None
        
    Raises:
        ImportError: If the consumer module cannot be imported
        KeyboardInterrupt: When user interrupts the consumer with Ctrl+C
    """
    print("Starting Kafka consumer...")
    print("Press Ctrl+C to stop the consumer\n")
    
    from consumer import main as consumer_main
    consumer_main()

def main() -> None:
    """
    Main entry point for the application.
    
    Parses command line arguments and executes the appropriate command.
    Supports 'producer' and 'consumer' commands.
    
    Returns:
        None
        
    Raises:
        SystemExit: If invalid arguments are provided or unknown command is used
    """
    if len(sys.argv) != 2:
        print(__doc__)
        print("\nAvailable commands:")
        print("  producer  - Start the FastAPI producer server")
        print("  consumer  - Start the Kafka consumer")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "producer":
        run_producer()
    elif command == "consumer":
        run_consumer()
    else:
        print(f"Unknown command: {command}")
        print("Available commands: producer, consumer")
        sys.exit(1)

if __name__ == "__main__":
    main()
