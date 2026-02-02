# Airflow MCP Server

Connect Claude to your Apache Airflow instance to monitor DAGs, check task status, and manage workflows.

## Quick Start

### 1. Clone and Setup
```bash
git clone https://github.com/anupamprataps/airflow_mcp_server.git
cd airflow_mcp_server
```

### 2. Run the Server
```bash
python working_mcp_server.py
```

The server uses these defaults:
- **URL**: `http://localhost:8082/api/v1` 
- **Username**: `airflow`
- **Password**: `airflow`

Only change these if your Airflow setup is different by setting environment variables:
```bash
export AIRFLOW_BASE_URL=http://localhost:8080
export AIRFLOW_USERNAME=admin  
export AIRFLOW_PASSWORD=admin
```

## Claude Desktop Setup

Add this to your Claude Desktop config file:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "airflow": {
      "command": "python",
      "args": ["/path/to/your/airflow_mcp_server/working_mcp_server.py"],
      "env": {
        "AIRFLOW_BASE_URL": "http://localhost:8082",
        "AIRFLOW_USERNAME": "airflow", 
        "AIRFLOW_PASSWORD": "airflow"
      }
    }
  }
}
```

Replace `/path/to/your/` with your actual directory path.

## Available Commands

Once connected, you can ask Claude to:

- **List DAGs**: "Show me all my Airflow DAGs"
- **Check Health**: "Is Airflow running properly?"
- **DAG Details**: "Tell me about the 'my_dag' workflow"

## Troubleshooting

### Connection Issues
- Make sure your Airflow instance is running on the specified port
- Check that the Airflow webserver is accessible at your configured URL
- Verify your username and password are correct

### Claude Desktop Issues
- Restart Claude Desktop after adding the MCP server configuration
- Check that the file path in your config points to the correct location
- Ensure Python is in your system PATH

## Requirements
- Python 3.7+
- Apache Airflow running locally
- Claude Desktop application

## License
MIT License - see LICENSE file for details.