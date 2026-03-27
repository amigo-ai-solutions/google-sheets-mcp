"""Hosted Supersheets MCP server — SSE transport with Google OAuth.

Entry point for Cloud Run deployment. Wraps the MCP server with OAuth
so users authenticate with their Google account and access their own sheets.
"""

from __future__ import annotations

import logging
import os

import uvicorn
from mcp.server.auth.provider import ProviderTokenVerifier
from mcp.server.auth.settings import AuthSettings, ClientRegistrationOptions
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

from google_sheets_mcp.auth import GoogleOAuthProvider
from google_sheets_mcp.config import settings
from google_sheets_mcp.logging_config import configure_logging
from google_sheets_mcp.server import mcp

logger = logging.getLogger(__name__)


def _get_base_url() -> str:
    """Resolve the base URL for OAuth callbacks.

    On Cloud Run, K_SERVICE is set automatically. We derive the URL from it
    since Cloud Run URLs are always HTTPS.
    """
    if settings.base_url:
        return settings.base_url.rstrip("/")
    k_service = os.environ.get("K_SERVICE")
    k_revision = os.environ.get("K_CONFIGURATION")
    if k_service:
        # Cloud Run — derive URL from service name and region
        region = os.environ.get("CLOUD_RUN_REGION", "us-east1")
        project_hash = os.environ.get("K_CONFIGURATION", "")
        # Can't reliably derive the full URL; require BASE_URL on Cloud Run
        # Fall back to a placeholder that will fail loudly
        logger.warning("BASE_URL not set on Cloud Run — OAuth callbacks will fail")
        return f"https://{k_service}.run.app"
    return f"http://{settings.host}:{settings.port}"


def create_app():
    """Create the hosted MCP SSE application with Google OAuth."""
    configure_logging(settings.log_level)

    base_url = _get_base_url()
    logger.info("Starting Supersheets MCP server (base_url=%s)", base_url)

    # Wire up OAuth provider
    provider = GoogleOAuthProvider()
    mcp._auth_server_provider = provider
    mcp._token_verifier = ProviderTokenVerifier(provider)
    mcp.settings.auth = AuthSettings(
        issuer_url=base_url,
        resource_server_url=base_url,
        client_registration_options=ClientRegistrationOptions(
            enabled=True,
            valid_scopes=["sheets", "drive"],
            default_scopes=["sheets", "drive"],
        ),
        revocation_options=None,
        required_scopes=None,
    )

    # Disable DNS rebinding protection (Cloud Run handles this at infra level)
    mcp.settings.transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    )

    # Register Google OAuth callback as custom route
    @mcp.custom_route("/callback", methods=["GET"])
    async def google_callback(request: Request):
        return await provider.handle_callback(request)

    # Health check
    @mcp.custom_route("/health", methods=["GET"])
    async def health(request: Request):
        return JSONResponse({"status": "ok", "service": "supersheets-mcp"})

    # Build SSE app with auth middleware
    app = mcp.sse_app()

    logger.info(
        "Supersheets MCP ready — %d tools, OAuth domain=%s",
        len(mcp._tool_manager._tools),
        settings.allowed_domain,
    )
    return app


def main() -> None:
    """Entry point for google-sheets-mcp-hosted."""
    app = create_app()
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
