"""MCP Server implementation for Apache Airflow."""

import json
import logging
from typing import Any, Dict, List, Optional, Sequence
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp import types
from pydantic import BaseModel

from .client import AirflowAPIClient
from .config import AirflowConfig, AccessLevel


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AirflowMCPServer:
    """MCP Server for Apache Airflow integration."""
    
    def __init__(self, config: AirflowConfig):
        self.config = config
        self.server = Server("airflow-mcp-server")
        self._setup_handlers()
    
    def _setup_handlers(self):
        """Set up MCP server handlers."""
        
        @self.server.list_tools()
        async def handle_list_tools() -> List[types.Tool]:
            """List available tools."""
            tools = [
                types.Tool(
                    name="list_dags",
                    description="List all DAGs in Airflow",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer", "description": "Number of DAGs to return", "default": 100},
                            "offset": {"type": "integer", "description": "Number of DAGs to skip", "default": 0}
                        }
                    }
                ),
                types.Tool(
                    name="get_dag",
                    description="Get details of a specific DAG",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dag_id": {"type": "string", "description": "The DAG ID"}
                        },
                        "required": ["dag_id"]
                    }
                ),
                types.Tool(
                    name="get_dag_runs",
                    description="Get DAG runs for a specific DAG",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dag_id": {"type": "string", "description": "The DAG ID"},
                            "limit": {"type": "integer", "description": "Number of DAG runs to return", "default": 100},
                            "offset": {"type": "integer", "description": "Number of DAG runs to skip", "default": 0}
                        },
                        "required": ["dag_id"]
                    }
                ),
                types.Tool(
                    name="get_task_instances",
                    description="Get task instances for a specific DAG run",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dag_id": {"type": "string", "description": "The DAG ID"},
                            "dag_run_id": {"type": "string", "description": "The DAG run ID"},
                            "limit": {"type": "integer", "description": "Number of task instances to return", "default": 100},
                            "offset": {"type": "integer", "description": "Number of task instances to skip", "default": 0}
                        },
                        "required": ["dag_id", "dag_run_id"]
                    }
                ),
                types.Tool(
                    name="get_task_logs",
                    description="Get logs for a specific task instance",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "dag_id": {"type": "string", "description": "The DAG ID"},
                            "dag_run_id": {"type": "string", "description": "The DAG run ID"},
                            "task_id": {"type": "string", "description": "The task ID"},
                            "task_try_number": {"type": "integer", "description": "The task try number", "default": 1}
                        },
                        "required": ["dag_id", "dag_run_id", "task_id"]
                    }
                ),
                types.Tool(
                    name="get_variables",
                    description="Get Airflow variables",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer", "description": "Number of variables to return", "default": 100},
                            "offset": {"type": "integer", "description": "Number of variables to skip", "default": 0}
                        }
                    }
                ),
                types.Tool(
                    name="get_variable",
                    description="Get a specific Airflow variable",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "key": {"type": "string", "description": "The variable key"}
                        },
                        "required": ["key"]
                    }
                ),
                types.Tool(
                    name="get_connections",
                    description="Get Airflow connections",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "limit": {"type": "integer", "description": "Number of connections to return", "default": 100},
                            "offset": {"type": "integer", "description": "Number of connections to skip", "default": 0}
                        }
                    }
                ),
                types.Tool(
                    name="get_health",
                    description="Get Airflow health status",
                    inputSchema={"type": "object", "properties": {}}
                )
            ]
            
            # Add write operations only if not in read-only mode
            if self.config.access_level != AccessLevel.READ_ONLY:
                write_tools = [
                    types.Tool(
                        name="trigger_dag",
                        description="Trigger a DAG run",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "dag_id": {"type": "string", "description": "The DAG ID"},
                                "conf": {"type": "object", "description": "Configuration for the DAG run"}
                            },
                            "required": ["dag_id"]
                        }
                    ),
                    types.Tool(
                        name="pause_dag",
                        description="Pause a DAG",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "dag_id": {"type": "string", "description": "The DAG ID"}
                            },
                            "required": ["dag_id"]
                        }
                    ),
                    types.Tool(
                        name="unpause_dag",
                        description="Unpause a DAG",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "dag_id": {"type": "string", "description": "The DAG ID"}
                            },
                            "required": ["dag_id"]
                        }
                    ),
                    types.Tool(
                        name="set_variable",
                        description="Set an Airflow variable",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "key": {"type": "string", "description": "The variable key"},
                                "value": {"type": "string", "description": "The variable value"},
                                "description": {"type": "string", "description": "Variable description", "default": ""}
                            },
                            "required": ["key", "value"]
                        }
                    ),
                    types.Tool(
                        name="delete_variable",
                        description="Delete an Airflow variable",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "key": {"type": "string", "description": "The variable key"}
                            },
                            "required": ["key"]
                        }
                    )
                ]
                tools.extend(write_tools)
            
            return tools

        @self.server.call_tool()
        async def handle_call_tool(
            name: str, arguments: Dict[str, Any]
        ) -> List[types.TextContent]:
            """Handle tool calls."""
            try:
                async with AirflowAPIClient(self.config) as client:
                    result = await self._execute_tool(client, name, arguments)
                    return [
                        types.TextContent(
                            type="text",
                            text=json.dumps(result, indent=2)
                        )
                    ]
            except Exception as e:
                logger.error(f"Error executing tool {name}: {e}")
                return [
                    types.TextContent(
                        type="text",
                        text=f"Error: {str(e)}"
                    )
                ]

    async def _execute_tool(
        self, 
        client: AirflowAPIClient, 
        name: str, 
        arguments: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a specific tool."""
        
        if name == "list_dags":
            return await client.get_dags(
                limit=arguments.get("limit", 100),
                offset=arguments.get("offset", 0)
            )
        
        elif name == "get_dag":
            return await client.get_dag(arguments["dag_id"])
        
        elif name == "get_dag_runs":
            return await client.get_dag_runs(
                dag_id=arguments["dag_id"],
                limit=arguments.get("limit", 100),
                offset=arguments.get("offset", 0)
            )
        
        elif name == "get_task_instances":
            return await client.get_task_instances(
                dag_id=arguments["dag_id"],
                dag_run_id=arguments["dag_run_id"],
                limit=arguments.get("limit", 100),
                offset=arguments.get("offset", 0)
            )
        
        elif name == "get_task_logs":
            return await client.get_task_instance_logs(
                dag_id=arguments["dag_id"],
                dag_run_id=arguments["dag_run_id"],
                task_id=arguments["task_id"],
                task_try_number=arguments.get("task_try_number", 1)
            )
        
        elif name == "get_variables":
            return await client.get_variables(
                limit=arguments.get("limit", 100),
                offset=arguments.get("offset", 0)
            )
        
        elif name == "get_variable":
            return await client.get_variable(arguments["key"])
        
        elif name == "get_connections":
            return await client.get_connections(
                limit=arguments.get("limit", 100),
                offset=arguments.get("offset", 0)
            )
        
        elif name == "get_health":
            return await client.get_health()
        
        # Write operations (only available if not in read-only mode)
        elif name == "trigger_dag":
            if self.config.access_level == AccessLevel.READ_ONLY:
                raise ValueError("Operation not permitted in read-only mode")
            return await client.trigger_dag(
                dag_id=arguments["dag_id"],
                conf=arguments.get("conf")
            )
        
        elif name == "pause_dag":
            if self.config.access_level == AccessLevel.READ_ONLY:
                raise ValueError("Operation not permitted in read-only mode")
            return await client.pause_dag(arguments["dag_id"])
        
        elif name == "unpause_dag":
            if self.config.access_level == AccessLevel.READ_ONLY:
                raise ValueError("Operation not permitted in read-only mode")
            return await client.unpause_dag(arguments["dag_id"])
        
        elif name == "set_variable":
            if self.config.access_level == AccessLevel.READ_ONLY:
                raise ValueError("Operation not permitted in read-only mode")
            return await client.set_variable(
                key=arguments["key"],
                value=arguments["value"],
                description=arguments.get("description", "")
            )
        
        elif name == "delete_variable":
            if self.config.access_level == AccessLevel.READ_ONLY:
                raise ValueError("Operation not permitted in read-only mode")
            return await client.delete_variable(arguments["key"])
        
        else:
            raise ValueError(f"Unknown tool: {name}")

    def get_server(self) -> Server:
        """Get the MCP server instance."""
        return self.server