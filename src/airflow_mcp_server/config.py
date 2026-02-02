"""Configuration management for Airflow MCP Server."""

import os
from dataclasses import dataclass
from typing import Optional
from enum import Enum


class AuthType(str, Enum):
    BASIC = "basic"
    JWT = "jwt"


class AccessLevel(str, Enum):
    READ_ONLY = "read_only"
    FULL = "full"


@dataclass
class AirflowConfig:
    """Airflow connection configuration."""
    
    base_url: str
    auth_type: AuthType = AuthType.BASIC
    username: Optional[str] = None
    password: Optional[str] = None
    jwt_token: Optional[str] = None
    access_level: AccessLevel = AccessLevel.READ_ONLY
    verify_ssl: bool = True
    timeout: int = 30

    @classmethod
    def from_env(cls) -> "AirflowConfig":
        """Create configuration from environment variables."""
        return cls(
            base_url=os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080"),
            auth_type=AuthType(os.getenv("AIRFLOW_AUTH_TYPE", "basic")),
            username=os.getenv("AIRFLOW_USERNAME"),
            password=os.getenv("AIRFLOW_PASSWORD"),
            jwt_token=os.getenv("AIRFLOW_JWT_TOKEN"),
            access_level=AccessLevel(os.getenv("AIRFLOW_ACCESS_LEVEL", "read_only")),
            verify_ssl=os.getenv("AIRFLOW_VERIFY_SSL", "true").lower() == "true",
            timeout=int(os.getenv("AIRFLOW_TIMEOUT", "30")),
        )

    def validate(self) -> None:
        """Validate the configuration."""
        if not self.base_url:
            raise ValueError("AIRFLOW_BASE_URL is required")
        
        if self.auth_type == AuthType.BASIC:
            if not self.username or not self.password:
                raise ValueError("Username and password are required for basic auth")
        elif self.auth_type == AuthType.JWT:
            if not self.jwt_token:
                raise ValueError("JWT token is required for JWT auth")