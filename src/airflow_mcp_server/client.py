"""Airflow REST API client."""

import base64
from typing import Any, Dict, List, Optional
import httpx
from .config import AirflowConfig, AuthType


class AirflowAPIClient:
    """Client for interacting with Airflow REST API."""

    def __init__(self, config: AirflowConfig):
        self.config = config
        self.base_url = config.base_url.rstrip("/") + "/api/v1"
        
        headers = {"Content-Type": "application/json"}
        
        if config.auth_type == AuthType.BASIC:
            auth_string = f"{config.username}:{config.password}"
            auth_bytes = auth_string.encode("ascii")
            auth_b64 = base64.b64encode(auth_bytes).decode("ascii")
            headers["Authorization"] = f"Basic {auth_b64}"
        elif config.auth_type == AuthType.JWT:
            headers["Authorization"] = f"Bearer {config.jwt_token}"
        
        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=config.timeout,
            verify=config.verify_ssl
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def _request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make a request to the Airflow API."""
        url = f"{self.base_url}{endpoint}"
        response = await self.client.request(
            method, url, params=params, json=json
        )
        response.raise_for_status()
        return response.json()

    async def get_dags(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """Get list of DAGs."""
        return await self._request(
            "GET", 
            "/dags", 
            params={"limit": limit, "offset": offset}
        )

    async def get_dag(self, dag_id: str) -> Dict[str, Any]:
        """Get details of a specific DAG."""
        return await self._request("GET", f"/dags/{dag_id}")

    async def trigger_dag(self, dag_id: str, conf: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Trigger a DAG run."""
        payload = {}
        if conf:
            payload["conf"] = conf
        return await self._request("POST", f"/dags/{dag_id}/dagRuns", json=payload)

    async def pause_dag(self, dag_id: str) -> Dict[str, Any]:
        """Pause a DAG."""
        return await self._request(
            "PATCH", 
            f"/dags/{dag_id}", 
            json={"is_paused": True}
        )

    async def unpause_dag(self, dag_id: str) -> Dict[str, Any]:
        """Unpause a DAG."""
        return await self._request(
            "PATCH", 
            f"/dags/{dag_id}", 
            json={"is_paused": False}
        )

    async def get_dag_runs(
        self, 
        dag_id: str, 
        limit: int = 100, 
        offset: int = 0
    ) -> Dict[str, Any]:
        """Get DAG runs for a specific DAG."""
        return await self._request(
            "GET", 
            f"/dags/{dag_id}/dagRuns",
            params={"limit": limit, "offset": offset}
        )

    async def get_task_instances(
        self, 
        dag_id: str, 
        dag_run_id: str,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Get task instances for a DAG run."""
        return await self._request(
            "GET",
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            params={"limit": limit, "offset": offset}
        )

    async def get_task_instance_logs(
        self, 
        dag_id: str, 
        dag_run_id: str, 
        task_id: str, 
        task_try_number: int = 1
    ) -> Dict[str, Any]:
        """Get logs for a task instance."""
        return await self._request(
            "GET",
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}"
        )

    async def get_variables(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """Get Airflow variables."""
        return await self._request(
            "GET",
            "/variables",
            params={"limit": limit, "offset": offset}
        )

    async def get_variable(self, variable_key: str) -> Dict[str, Any]:
        """Get a specific Airflow variable."""
        return await self._request("GET", f"/variables/{variable_key}")

    async def set_variable(self, key: str, value: str, description: str = "") -> Dict[str, Any]:
        """Set an Airflow variable."""
        return await self._request(
            "POST",
            "/variables",
            json={"key": key, "value": value, "description": description}
        )

    async def delete_variable(self, variable_key: str) -> Dict[str, Any]:
        """Delete an Airflow variable."""
        return await self._request("DELETE", f"/variables/{variable_key}")

    async def get_connections(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """Get Airflow connections."""
        return await self._request(
            "GET",
            "/connections",
            params={"limit": limit, "offset": offset}
        )

    async def get_health(self) -> Dict[str, Any]:
        """Get Airflow health status."""
        return await self._request("GET", "/health")