# Airflow MCP Server

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server that provides integration with Apache Airflow through its REST API. This server enables AI assistants and other MCP clients to interact with Airflow workflows, monitor DAG runs, and manage Airflow resources.

## Features

- **Comprehensive Airflow API Coverage**: Access DAGs, task instances, variables, connections, and more
- **Flexible Authentication**: Support for both basic authentication and JWT tokens
- **Security-First Design**: Read-only mode option for safe monitoring and inspection
- **Modern Implementation**: Built with Python 3.10+, async/await, and type hints
- **MCP Compliant**: Follows the latest MCP specification (November 2025)

## Installation

```bash
# Install from source
git clone <repository-url>
cd airflow-mcp-server
pip install -e .

# Or install dependencies directly
pip install mcp httpx pydantic python-dotenv click
```

## Configuration

Configure the server using environment variables or a `.env` file:

```bash
# Required
AIRFLOW_BASE_URL=http://localhost:8080
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin

# Optional
AIRFLOW_AUTH_TYPE=basic          # basic or jwt
AIRFLOW_ACCESS_LEVEL=read_only   # read_only or full
AIRFLOW_VERIFY_SSL=true
AIRFLOW_TIMEOUT=30
```

### Authentication Methods

#### Basic Authentication
```bash
AIRFLOW_AUTH_TYPE=basic
AIRFLOW_USERNAME=your_username
AIRFLOW_PASSWORD=your_password
```

#### JWT Authentication
```bash
AIRFLOW_AUTH_TYPE=jwt
AIRFLOW_JWT_TOKEN=your_jwt_token
```

### Access Levels

- **read_only**: Only read operations are allowed (default for security)
- **full**: Both read and write operations are enabled

## Usage

### Standalone Server
```bash
# Run with default configuration
airflow-mcp-server

# Run with custom env file
airflow-mcp-server --env-file /path/to/.env

# Run with debug logging
airflow-mcp-server --log-level DEBUG
```

### Claude Desktop Integration

Add to your Claude Desktop configuration file:

```json
{
  "mcpServers": {
    "airflow": {
      "command": "airflow-mcp-server",
      "env": {
        "AIRFLOW_BASE_URL": "http://localhost:8080",
        "AIRFLOW_USERNAME": "admin",
        "AIRFLOW_PASSWORD": "admin",
        "AIRFLOW_ACCESS_LEVEL": "read_only"
      }
    }
  }
}
```

## Available Tools

### Read Operations (Always Available)

- **list_dags**: List all DAGs with optional pagination
- **get_dag**: Get detailed information about a specific DAG
- **get_dag_runs**: Get DAG run history for a specific DAG
- **get_task_instances**: Get task instances for a specific DAG run
- **get_task_logs**: Retrieve logs for a specific task instance
- **get_variables**: List Airflow variables
- **get_variable**: Get a specific Airflow variable
- **get_connections**: List Airflow connections
- **get_health**: Check Airflow health status

### Write Operations (Full Access Mode Only)

- **trigger_dag**: Trigger a new DAG run
- **pause_dag**: Pause a DAG
- **unpause_dag**: Unpause a DAG  
- **set_variable**: Create or update an Airflow variable
- **delete_variable**: Delete an Airflow variable

## Examples

### Monitoring DAG Status
```python
# List all DAGs
await client.call_tool("list_dags", {"limit": 10})

# Get specific DAG details
await client.call_tool("get_dag", {"dag_id": "my_workflow"})

# Check recent DAG runs
await client.call_tool("get_dag_runs", {"dag_id": "my_workflow", "limit": 5})
```

### Managing Variables
```python
# List variables
await client.call_tool("get_variables", {})

# Get specific variable
await client.call_tool("get_variable", {"key": "my_config"})

# Set variable (full access mode only)
await client.call_tool("set_variable", {
    "key": "my_config",
    "value": "production_value"
})
```

### Triggering Workflows
```python
# Trigger DAG (full access mode only)
await client.call_tool("trigger_dag", {
    "dag_id": "my_workflow",
    "conf": {"param1": "value1"}
})
```

## Development

### Setup Development Environment
```bash
# Clone the repository
git clone <repository-url>
cd airflow-mcp-server

# Install in development mode
pip install -e .[dev]

# Run tests
pytest

# Format code
black src/
ruff check src/

# Type checking
mypy src/
```

### Project Structure
```
src/
├── airflow_mcp_server/
│   ├── __init__.py
│   ├── main.py          # Entry point and CLI
│   ├── server.py        # MCP server implementation
│   ├── client.py        # Airflow API client
│   └── config.py        # Configuration management
├── tests/               # Test suite
└── examples/            # Usage examples
```

## Security Considerations

- **Read-Only Default**: Server defaults to read-only mode for safety
- **Authentication Required**: All requests require proper Airflow authentication
- **SSL Verification**: SSL verification is enabled by default
- **No Credential Storage**: Credentials are passed via environment variables only

## Requirements

- Python 3.10+
- Apache Airflow with REST API enabled
- Network access to Airflow instance

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Support

For issues and questions:
- Check the documentation at [MCP Specification](https://modelcontextprotocol.io)
- Review Airflow's [REST API documentation](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
- Create an issue in this repository