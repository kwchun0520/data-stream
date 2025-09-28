"""
Schema Registry Management Script

This script provides utilities for managing Avro schemas in the Confluent Schema Registry.
It includes functionality to:
- Register new schemas
- Update existing schemas
- List all subjects and versions
- Check schema compatibility
- Download and view schemas
- Delete schemas (with caution)

Usage:
    python schema_manager.py register <subject> <schema_file>
    python schema_manager.py update <subject> <schema_file>
    python schema_manager.py list
    python schema_manager.py get <subject> [version]
    python schema_manager.py check-compatibility <subject> <schema_file>
    python schema_manager.py delete <subject> [version]
"""

import json
import requests
import sys
import os
from typing import Dict, Any, List

class SchemaRegistryClient:
    """Client for interacting with Confluent Schema Registry."""
    
    def __init__(self, schema_registry_url: str = "http://localhost:8081"):
        """Initialize the Schema Registry client.
        
        Args:
            schema_registry_url: The URL of the Schema Registry server
        """
        self.base_url = schema_registry_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/vnd.schemaregistry.v1+json'
        })
    
    def health_check(self) -> bool:
        """
        Check if Schema Registry is accessible and responding.
        
        Returns:
            bool: True if Schema Registry is accessible, False otherwise
        """
        try:
            response = self.session.get(f"{self.base_url}/subjects")
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def list_subjects(self) -> List[str]:
        """
        List all subjects in the Schema Registry.
        
        Returns:
            List of subject names available in the registry
            
        Raises:
            requests.HTTPError: If the request to Schema Registry fails
        """
        response = self.session.get(f"{self.base_url}/subjects")
        response.raise_for_status()
        return response.json()
    
    def list_versions(self, subject: str) -> List[int]:
        """
        List all versions for a specific subject.
        
        Args:
            subject: The subject name to list versions for
            
        Returns:
            List of version numbers for the specified subject
            
        Raises:
            requests.HTTPError: If the subject doesn't exist or request fails
        """
        response = self.session.get(f"{self.base_url}/subjects/{subject}/versions")
        response.raise_for_status()
        return response.json()
    
    def get_schema(self, subject: str, version: str = "latest") -> Dict[str, Any]:
        """
        Get a specific schema version for a subject.
        
        Args:
            subject: The subject name
            version: The version number or "latest" for the most recent version
            
        Returns:
            Dictionary containing schema information including ID, version, and schema content
            
        Raises:
            requests.HTTPError: If the subject/version doesn't exist or request fails
        """
        response = self.session.get(f"{self.base_url}/subjects/{subject}/versions/{version}")
        response.raise_for_status()
        return response.json()
    
    def register_schema(self, subject: str, schema: str) -> Dict[str, Any]:
        """
        Register a new schema or get existing schema ID if it already exists.
        
        Args:
            subject: The subject name to register the schema under
            schema: The schema content as a JSON string
            
        Returns:
            Dictionary containing the schema ID and version information
            
        Raises:
            requests.HTTPError: If registration fails due to compatibility or other issues
        """
        payload = {"schema": schema}
        response = self.session.post(
            f"{self.base_url}/subjects/{subject}/versions",
            json=payload
        )
        response.raise_for_status()
        return response.json()
    
    def check_compatibility(self, subject: str, schema: str, version: str = "latest") -> Dict[str, Any]:
        """
        Check if a schema is compatible with an existing version.
        
        Args:
            subject: The subject name to check compatibility against
            schema: The schema content as a JSON string to check
            version: The version to check compatibility against (default: "latest")
            
        Returns:
            Dictionary containing compatibility result ({"is_compatible": bool})
            
        Raises:
            requests.HTTPError: If the request fails or subject doesn't exist
        """
        payload = {"schema": schema}
        response = self.session.post(
            f"{self.base_url}/compatibility/subjects/{subject}/versions/{version}",
            json=payload
        )
        response.raise_for_status()
        return response.json()
    
    def delete_subject(self, subject: str) -> List[int]:
        """
        Delete all versions of a subject.
        
        Args:
            subject: The subject name to delete
            
        Returns:
            List of deleted version numbers
            
        Raises:
            requests.HTTPError: If deletion fails or subject doesn't exist
        """
        response = self.session.delete(f"{self.base_url}/subjects/{subject}")
        response.raise_for_status()
        return response.json()
    
    def delete_schema_version(self, subject: str, version: str) -> int:
        """
        Delete a specific version of a schema.
        
        Args:
            subject: The subject name
            version: The version number to delete
            
        Returns:
            The deleted version number
            
        Raises:
            requests.HTTPError: If deletion fails or version doesn't exist
        """
        response = self.session.delete(f"{self.base_url}/subjects/{subject}/versions/{version}")
        response.raise_for_status()
        return response.json()
    
    def get_global_compatibility_level(self) -> Dict[str, str]:
        """
        Get the global compatibility level setting.
        
        Returns:
            Dictionary containing the current compatibility level
            
        Raises:
            requests.HTTPError: If the request fails
        """
        response = self.session.get(f"{self.base_url}/config")
        response.raise_for_status()
        return response.json()
    
    def set_global_compatibility_level(self, compatibility: str) -> Dict[str, str]:
        """
        Set the global compatibility level.
        
        Args:
            compatibility: The compatibility level to set (e.g., 'BACKWARD', 'FORWARD', 'FULL')
            
        Returns:
            Dictionary containing the updated compatibility level
            
        Raises:
            requests.HTTPError: If the request fails or compatibility level is invalid
        """
        payload = {"compatibility": compatibility}
        response = self.session.put(f"{self.base_url}/config", json=payload)
        response.raise_for_status()
        return response.json()

def load_schema_file(schema_file: str) -> str:
    """
    Load and validate an Avro schema file.
    
    Args:
        schema_file: Path to the schema file to load
        
    Returns:
        The schema content as a JSON string
        
    Raises:
        FileNotFoundError: If the schema file doesn't exist
        ValueError: If the schema file contains invalid JSON
    """
    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    
    with open(schema_file, 'r') as f:
        schema_content = f.read().strip()
    
    # Validate JSON format
    try:
        json.loads(schema_content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in schema file: {e}")
    
    return schema_content

def pretty_print_schema(schema_data: Dict[str, Any]) -> None:
    """
    Pretty print schema information in a formatted way.
    
    Args:
        schema_data: Dictionary containing schema information from the registry
        
    Returns:
        None
    """
    print(f"Subject: {schema_data.get('subject', 'N/A')}")
    print(f"Version: {schema_data.get('version', 'N/A')}")
    print(f"ID: {schema_data.get('id', 'N/A')}")
    print("Schema:")
    schema_obj = json.loads(schema_data['schema'])
    print(json.dumps(schema_obj, indent=2))

def cmd_register(client: SchemaRegistryClient, args: List[str]) -> None:
    """
    Command handler for registering a new schema.
    
    Args:
        client: The Schema Registry client instance
        args: Command line arguments [subject, schema_file]
        
    Returns:
        None
        
    Raises:
        SystemExit: If invalid arguments are provided
    """
    """Register a new schema."""
    if len(args) < 2:
        print("Usage: register <subject> <schema_file>")
        sys.exit(1)
    
    subject = args[0]
    schema_file = args[1]
    
    try:
        schema_content = load_schema_file(schema_file)
        result = client.register_schema(subject, schema_content)
        
        print("✅ Schema registered successfully!")
        print(f"Subject: {subject}")
        print(f"Schema ID: {result['id']}")
        
        if 'version' in result:
            print(f"Version: {result['version']}")
        
    except Exception as e:
        print(f"❌ Failed to register schema: {e}")
        sys.exit(1)

def cmd_update(client: SchemaRegistryClient, args: List[str]):
    """Update an existing schema (register new version)."""
    if len(args) < 2:
        print("Usage: update <subject> <schema_file>")
        sys.exit(1)
    
    subject = args[0]
    schema_file = args[1]
    
    try:
        schema_content = load_schema_file(schema_file)
        
        # Check compatibility first
        try:
            compatibility = client.check_compatibility(subject, schema_content)
            if not compatibility.get('is_compatible', False):
                print(f"⚠️  Warning: New schema is not compatible with the latest version!")
                response = input("Do you want to continue anyway? (y/N): ")
                if response.lower() != 'y':
                    print("Schema update cancelled.")
                    sys.exit(0)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print("ℹ️  No existing schema found. This will be the first version.")
            else:
                print(f"⚠️  Could not check compatibility: {e}")
        
        result = client.register_schema(subject, schema_content)
        
        print("✅ Schema updated successfully!")
        print(f"Subject: {subject}")
        print(f"New Schema ID: {result['id']}")
        
        if 'version' in result:
            print(f"New Version: {result['version']}")
        
    except Exception as e:
        print(f"❌ Failed to update schema: {e}")
        sys.exit(1)

def cmd_list(client: SchemaRegistryClient, args: List[str]):
    """List all subjects or versions for a specific subject."""
    try:
        if len(args) == 0:
            # List all subjects
            subjects = client.list_subjects()
            if not subjects:
                print("No subjects found in the Schema Registry.")
                return
            
            print("📋 Subjects in Schema Registry:")
            print("-" * 40)
            for subject in subjects:
                try:
                    versions = client.list_versions(subject)
                    latest_schema = client.get_schema(subject, "latest")
                    print(f"🔹 {subject}")
                    print(f"   Versions: {versions}")
                    print(f"   Latest ID: {latest_schema['id']}")
                    print()
                except Exception as e:
                    print(f"🔹 {subject} (Error getting details: {e})")
        else:
            # List versions for a specific subject
            subject = args[0]
            versions = client.list_versions(subject)
            print(f"📋 Versions for subject '{subject}':")
            print("-" * 40)
            for version in versions:
                schema_data = client.get_schema(subject, str(version))
                print(f"Version {version}: ID {schema_data['id']}")
    
    except Exception as e:
        print(f"❌ Failed to list: {e}")
        sys.exit(1)

def cmd_get(client: SchemaRegistryClient, args: List[str]):
    """Get and display a specific schema."""
    if len(args) < 1:
        print("Usage: get <subject> [version]")
        sys.exit(1)
    
    subject = args[0]
    version = args[1] if len(args) > 1 else "latest"
    
    try:
        schema_data = client.get_schema(subject, version)
        print("📄 Schema Details:")
        print("=" * 50)
        pretty_print_schema(schema_data)
        
    except Exception as e:
        print(f"❌ Failed to get schema: {e}")
        sys.exit(1)

def cmd_check_compatibility(client: SchemaRegistryClient, args: List[str]):
    """Check schema compatibility."""
    if len(args) < 2:
        print("Usage: check-compatibility <subject> <schema_file>")
        sys.exit(1)
    
    subject = args[0]
    schema_file = args[1]
    
    try:
        schema_content = load_schema_file(schema_file)
        result = client.check_compatibility(subject, schema_content)
        
        if result.get('is_compatible', False):
            print("✅ Schema is compatible!")
        else:
            print("❌ Schema is NOT compatible!")
            if 'messages' in result:
                print("Compatibility issues:")
                for message in result['messages']:
                    print(f"  - {message}")
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print("ℹ️  No existing schema found to check compatibility against.")
        else:
            print(f"❌ Failed to check compatibility: {e}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Failed to check compatibility: {e}")
        sys.exit(1)

def cmd_delete(client: SchemaRegistryClient, args: List[str]):
    """Delete a schema or specific version."""
    if len(args) < 1:
        print("Usage: delete <subject> [version]")
        sys.exit(1)
    
    subject = args[0]
    version = args[1] if len(args) > 1 else None
    
    if version:
        confirm_msg = f"Are you sure you want to delete version {version} of subject '{subject}'? (y/N): "
    else:
        confirm_msg = f"Are you sure you want to delete ALL versions of subject '{subject}'? (y/N): "
    
    response = input(confirm_msg)
    if response.lower() != 'y':
        print("Delete cancelled.")
        return
    
    try:
        if version:
            result = client.delete_schema_version(subject, version)
            print(f"✅ Deleted version {version} (ID: {result}) of subject '{subject}'")
        else:
            result = client.delete_subject(subject)
            print(f"✅ Deleted subject '{subject}' (versions: {result})")
    
    except Exception as e:
        print(f"❌ Failed to delete: {e}")
        sys.exit(1)

def cmd_config(client: SchemaRegistryClient, args: List[str]):
    """Get or set compatibility configuration."""
    if len(args) == 0:
        # Get current config
        try:
            config = client.get_global_compatibility_level()
            print(f"Current global compatibility level: {config['compatibilityLevel']}")
        except Exception as e:
            print(f"❌ Failed to get config: {e}")
            sys.exit(1)
    else:
        # Set new config
        compatibility = args[0].upper()
        valid_levels = ['BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE', 'NONE']
        
        if compatibility not in valid_levels:
            print(f"❌ Invalid compatibility level. Valid options: {', '.join(valid_levels)}")
            sys.exit(1)
        
        try:
            result = client.set_global_compatibility_level(compatibility)
            print(f"✅ Global compatibility level set to: {result['compatibility']}")
        except Exception as e:
            print(f"❌ Failed to set config: {e}")
            sys.exit(1)

def main():
    """Main CLI entry point."""
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nAvailable commands:")
        print("  register <subject> <schema_file>      - Register a new schema")
        print("  update <subject> <schema_file>        - Update/register new version of schema")
        print("  list [subject]                        - List all subjects or versions for subject")
        print("  get <subject> [version]               - Get and display a schema")
        print("  check-compatibility <subject> <file>  - Check schema compatibility")
        print("  delete <subject> [version]            - Delete schema or specific version")
        print("  config [compatibility_level]          - Get/set global compatibility level")
        print("\nExamples:")
        print("  python schema_manager.py register user_events-value ./schema/user_event.avsc")
        print("  python schema_manager.py update user_events-value ./schema/user_event.avsc")
        print("  python schema_manager.py list")
        print("  python schema_manager.py get user_events-value")
        print("  python schema_manager.py check-compatibility user_events-value ./schema/user_event.avsc")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    args = sys.argv[2:]
    
    # Initialize client
    client = SchemaRegistryClient()
    
    # Health check
    if not client.health_check():
        print("❌ Cannot connect to Schema Registry at http://localhost:8081")
        print("Make sure the Schema Registry is running: docker-compose up -d")
        sys.exit(1)
    
    # Route to appropriate command
    commands = {
        'register': cmd_register,
        'update': cmd_update,
        'list': cmd_list,
        'get': cmd_get,
        'check-compatibility': cmd_check_compatibility,
        'delete': cmd_delete,
        'config': cmd_config,
    }
    
    if command not in commands:
        print(f"❌ Unknown command: {command}")
        print(f"Available commands: {', '.join(commands.keys())}")
        sys.exit(1)
    
    commands[command](client, args)

if __name__ == "__main__":
    main()
