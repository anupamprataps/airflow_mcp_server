#!/usr/bin/env python3
"""FastMCP-based Airflow MCP Server."""

import os
import logging
from typing import Any, Dict, Optional
from fastmcp import FastMCP
from dotenv import load_dotenv

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow_mcp_server.client import AirflowAPIClient
from airflow_mcp_server.config import AirflowConfig, AccessLevel


# Set up logging to stderr (important for MCP stdio servers)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize FastMCP server
mcp = FastMCP("airflow-mcp-server")

# Global config (will be set in main)
config: Optional[AirflowConfig] = None


@mcp.tool
async def list_dags(limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    """List all DAGs in Airflow.
    
    Args:
        limit: Number of DAGs to return (default: 100)
        offset: Number of DAGs to skip (default: 0)
        
    Returns:
        Dictionary containing DAG information
    """
    async with AirflowAPIClient(config) as client:
        return await client.get_dags(limit=limit, offset=offset)


@mcp.tool
async def get_dag(dag_id: str) -> Dict[str, Any]:
    """Get details of a specific DAG.
    
    Args:
        dag_id: The DAG identifier
        
    Returns:
        Dictionary containing DAG details
    """
    async with AirflowAPIClient(config) as client:
        return await client.get_dag(dag_id)


@mcp.tool
async def get_dag_runs(dag_id: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    """Get DAG runs for a specific DAG.
    
    Args:
        dag_id: The DAG identifier
        limit: Number of DAG runs to return (default: 100)
        offset: Number of DAG runs to skip (default: 0)
        
    Returns:
        Dictionary containing DAG run information
    """
    async with AirflowAPIClient(config) as client:
        return await client.get_dag_runs(dag_id=dag_id, limit=limit, offset=offset)


@mcp.tool
async def get_task_instances(dag_id: str, dag_run_id: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    """Get task instances for a specific DAG run.
    
    Args:
        dag_id: The DAG identifier
        dag_run_id: The DAG run identifier
        limit: Number of task instances to return (default: 100)
        offset: Number of task instances to skip (default: 0)
        
    Returns:
        Dictionary containing task instance information
    """
    async with AirflowAPIClient(config) as client:
        return await client.get_task_instances(
            dag_id=dag_id, dag_run_id=dag_run_id, limit=limit, offset=offset
        )


@mcp.tool
async def get_task_logs(dag_id: str, dag_run_id: str, task_id: str, task_try_number: int = 1) -> Dict[str, Any]:
    """Get logs for a specific task instance.
    
    Args:
        dag_id: The DAG identifier
        dag_run_id: The DAG run identifier
        task_id: The task identifier
        task_try_number: The task try number (default: 1)
        
    Returns:
        Dictionary containing task logs
    """
    async with AirflowAPIClient(config) as client:
        return await client.get_task_instance_logs(
            dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id, task_try_number=task_try_number
        )


@mcp.tool
async def get_variables(limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    """Get Airflow variables.
    
    Args:
        limit: Number of variables to return (default: 100)
        offset: Number of variables to skip (default: 0)
        
    Returns:
        Dictionary containing variable information
    """
    async with AirflowAPIClient(config) as client:
        return await client.get_variables(limit=limit, offset=offset)


@mcp.tool
async def get_variable(key: str) -> Dict[str, Any]:
    """Get a specific Airflow variable.
    
    Args:
        key: The variable key
        
    Returns:
        Dictionary containing variable information
    """
    async with AirflowAPIClient(config) as client:
        return await client.get_variable(key)


@mcp.tool
async def get_connections(limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    """Get Airflow connections.
    
    Args:
        limit: Number of connections to return (default: 100)
        offset: Number of connections to skip (default: 0)
        
    Returns:
        Dictionary containing connection information
    """
    async with AirflowAPIClient(config) as client:
        return await client.get_connections(limit=limit, offset=offset)


@mcp.tool
async def get_health() -> Dict[str, Any]:
    """Get Airflow health status.
    
    Returns:
        Dictionary containing health status information
    """
    async with AirflowAPIClient(config) as client:
        return await client.get_health()


# Write operations (only available if not in read-only mode)
def register_write_tools():
    """Register write tools if access level allows it."""
    
    @mcp.tool
    async def trigger_dag(dag_id: str, conf: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Trigger a DAG run.
        
        Args:
            dag_id: The DAG identifier
            conf: Optional configuration for the DAG run
            
        Returns:
            Dictionary containing DAG run information
        """
        if config.access_level == AccessLevel.READ_ONLY:
            raise ValueError("Operation not permitted in read-only mode")
        async with AirflowAPIClient(config) as client:
            return await client.trigger_dag(dag_id=dag_id, conf=conf)
    
    @mcp.tool
    async def pause_dag(dag_id: str) -> Dict[str, Any]:
        """Pause a DAG.
        
        Args:
            dag_id: The DAG identifier
            
        Returns:
            Dictionary containing DAG information
        """
        if config.access_level == AccessLevel.READ_ONLY:
            raise ValueError("Operation not permitted in read-only mode")
        async with AirflowAPIClient(config) as client:
            return await client.pause_dag(dag_id)
    
    @mcp.tool
    async def unpause_dag(dag_id: str) -> Dict[str, Any]:
        """Unpause a DAG.
        
        Args:
            dag_id: The DAG identifier
            
        Returns:
            Dictionary containing DAG information
        """
        if config.access_level == AccessLevel.READ_ONLY:
            raise ValueError("Operation not permitted in read-only mode")
        async with AirflowAPIClient(config) as client:
            return await client.unpause_dag(dag_id)
    
    @mcp.tool
    async def set_variable(key: str, value: str, description: str = "") -> Dict[str, Any]:
        """Set an Airflow variable.
        
        Args:
            key: The variable key
            value: The variable value
            description: Variable description (optional)
            
        Returns:
            Dictionary containing variable information
        """
        if config.access_level == AccessLevel.READ_ONLY:
            raise ValueError("Operation not permitted in read-only mode")
        async with AirflowAPIClient(config) as client:
            return await client.set_variable(key=key, value=value, description=description)
    
    @mcp.tool
    async def delete_variable(key: str) -> Dict[str, Any]:
        """Delete an Airflow variable.
        
        Args:
            key: The variable key
            
        Returns:
            Dictionary containing deletion confirmation
        """
        if config.access_level == AccessLevel.READ_ONLY:
            raise ValueError("Operation not permitted in read-only mode")
        async with AirflowAPIClient(config) as client:
            return await client.delete_variable(key)


def main():
    """Main entry point for the FastMCP Airflow server."""
    global config
    
    try:
        # Create and validate configuration
        config = AirflowConfig.from_env()
        config.validate()
        
        logger.info("Starting Airflow MCP Server (FastMCP)")
        logger.info(f"Airflow URL: {config.base_url}")
        logger.info(f"Auth Type: {config.auth_type}")
        logger.info(f"Access Level: {config.access_level}")
        
        # Register write tools if not in read-only mode
        if config.access_level != AccessLevel.READ_ONLY:
            register_write_tools()
            logger.info("Write operations enabled")
        else:
            logger.info("Read-only mode enabled")
        
        # Start the server
        mcp.run(transport="stdio")
        
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        raise


if __name__ == "__main__":
    main()