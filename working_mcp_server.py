#!/usr/bin/env python3
"""
Working MCP Server for Airflow - follows exact MCP protocol.
"""

import asyncio
import json
import sys
import os
import base64
import urllib.request
import ssl
from typing import Dict, Any, Optional


class AirflowMCPServer:
    def __init__(self):
        self.base_url = os.getenv('AIRFLOW_BASE_URL', 'http://localhost:8082').rstrip('/') + '/api/v1'
        username = os.getenv('AIRFLOW_USERNAME', 'airflow')
        password = os.getenv('AIRFLOW_PASSWORD', 'airflow')

        # Create auth header
        auth_string = f"{username}:{password}"
        auth_bytes = auth_string.encode('ascii')
        auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
        self.headers = {
            'Authorization': f'Basic {auth_b64}',
            'Content-Type': 'application/json'
        }

        # SSL context
        self.ssl_context = None
        if os.getenv('AIRFLOW_VERIFY_SSL', 'true').lower() == 'false':
            self.ssl_context = ssl.create_default_context()
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE

    def make_request(self, endpoint: str) -> Dict[str, Any]:
        """Make HTTP request to Airflow API."""
        url = f"{self.base_url}{endpoint}"
        req = urllib.request.Request(url, headers=self.headers)

        try:
            with urllib.request.urlopen(req, context=self.ssl_context, timeout=30) as response:
                return json.loads(response.read().decode('utf-8'))
        except Exception as e:
            return {"error": f"API request failed: {str(e)}"}

    async def handle_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle incoming MCP messages."""
        method = message.get("method")
        msg_id = message.get("id")

        # Notifications (no id) must never get a response
        # e.g. notifications/initialized, notifications/progress, etc.
        if msg_id is None:
            return None

        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": "airflow-mcp-server",
                        "version": "0.1.0"
                    }
                }
            }

        elif method == "ping":
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {}
            }

        elif method == "tools/list":
            tools = [
                {
                    "name": "list_dags",
                    "description": "List all DAGs in Airflow",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "limit": {
                                "type": "integer",
                                "description": "Number of DAGs to return",
                                "default": 100
                            }
                        }
                    }
                },
                {
                    "name": "get_health",
                    "description": "Get Airflow health status",
                    "inputSchema": {
                        "type": "object",
                        "properties": {}
                    }
                }
            ]

            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "tools": tools
                }
            }

        elif method == "tools/call":
            params = message.get("params", {})
            tool_name = params.get("name")
            arguments = params.get("arguments", {})

            result = await self.call_tool(tool_name, arguments)

            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(result, indent=2)
                        }
                    ]
                }
            }

        else:
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            }

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute tool calls."""
        if tool_name == "list_dags":
            limit = arguments.get("limit", 100)
            return self.make_request(f"/dags?limit={limit}")

        elif tool_name == "get_health":
            return self.make_request("/health")

        else:
            return {"error": f"Unknown tool: {tool_name}"}

    async def run(self):
        """Main server loop."""
        while True:
            try:
                line = await asyncio.get_event_loop().run_in_executor(
                    None, sys.stdin.readline
                )

                if not line:
                    break

                line = line.strip()
                if not line:
                    continue

                try:
                    message = json.loads(line)
                    response = await self.handle_message(message)

                    # Only write back if there's a response
                    # Notifications return None and must be silently ignored
                    if response is not None:
                        print(json.dumps(response), flush=True)

                except json.JSONDecodeError:
                    continue

            except Exception:
                break


async def main():
    """Entry point."""
    server = AirflowMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())