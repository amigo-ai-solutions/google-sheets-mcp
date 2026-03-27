"""Application configuration via environment variables."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """All config is injected via environment variables (Cloud Run / Secret Manager)."""

    # Google OAuth
    google_oauth_client_id: str = ""
    google_oauth_client_secret: str = ""
    allowed_domain: str = "amigo.ai"

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    base_url: str = ""
    log_level: str = "INFO"

    # Local mode (service account)
    google_application_credentials: str = ""

    model_config = {"env_file": ".env", "case_sensitive": False}


settings = Settings()
