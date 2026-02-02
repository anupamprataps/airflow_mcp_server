"""Main entry point for the Airflow MCP Server."""

import asyncio
import logging
import sys
from typing import Optional

import click
from dotenv import load_dotenv
from mcp.server.stdio import stdio_server

from .config import AirflowConfig
from .server import AirflowMCPServer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--env-file",
    type=click.Path(exists=True),
    help="Path to .env file with configuration",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    default="INFO",
    help="Logging level",
)
def main(env_file: Optional[str], log_level: str) -> None:
    """Start the Airflow MCP Server."""
    
    # Load environment variables
    if env_file:
        load_dotenv(env_file)
    else:
        load_dotenv()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, log_level))
    
    try:
        # Create and validate configuration
        config = AirflowConfig.from_env()
        config.validate()
        
        logger.info("Starting Airflow MCP Server")
        logger.info(f"Airflow URL: {config.base_url}")
        logger.info(f"Auth Type: {config.auth_type}")
        logger.info(f"Access Level: {config.access_level}")
        
        # Create MCP server
        mcp_server = AirflowMCPServer(config)
        server = mcp_server.get_server()
        
        # Run the server
        async def run_server():
            async with stdio_server(server) as server_context:
                pass
        
        asyncio.run(run_server())
        
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()