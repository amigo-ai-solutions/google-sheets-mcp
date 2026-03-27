"""Google OAuth provider for MCP authentication.

Implements OAuthAuthorizationServerProvider to wrap Google OAuth,
allowing users to authenticate with their Google account and
access their own Google Sheets.
"""

from __future__ import annotations

import logging
import os
import secrets
import time
from urllib.parse import urlencode

import httpx
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    AuthorizeError,
    RefreshToken,
    RegistrationError,
    TokenError,
    construct_redirect_url,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from starlette.requests import Request
from starlette.responses import RedirectResponse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Token stores (module-level, shared between auth provider and MCP tools)
# ---------------------------------------------------------------------------

# Maps our access token string → Google credentials dict
google_token_store: dict[str, dict] = {}

# Maps our access token string → user email
access_token_to_user: dict[str, str] = {}

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

GOOGLE_SCOPES = " ".join([
    "openid",
    "email",
    "profile",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
])


def _env(key: str, default: str | None = None) -> str:
    val = os.environ.get(key, default)
    if val is None:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


class GoogleOAuthProvider:
    """MCP OAuth provider that delegates to Google OAuth.

    Flow:
    1. Claude Code → /authorize → we redirect to Google OAuth
    2. User signs in with Google → Google redirects to /callback
    3. /callback exchanges Google code → stores tokens → redirects to Claude
    4. Claude Code → /token → we issue our access token (linked to Google creds)
    5. MCP tools use the linked Google creds to access Sheets
    """

    def __init__(self):
        self.google_client_id = _env("GOOGLE_OAUTH_CLIENT_ID")
        self.google_client_secret = _env("GOOGLE_OAUTH_CLIENT_SECRET")
        self.allowed_domain = _env("ALLOWED_DOMAIN", "amigo.ai")

        # In-memory stores (single Cloud Run instance)
        self._clients: dict[str, OAuthClientInformationFull] = {}
        self._auth_codes: dict[str, AuthorizationCode] = {}
        self._access_tokens: dict[str, AccessToken] = {}
        self._refresh_tokens: dict[str, RefreshToken] = {}

        # Link auth codes to Google tokens
        self._google_tokens_by_code: dict[str, dict] = {}
        # Link refresh tokens to Google tokens
        self._google_tokens_by_refresh: dict[str, dict] = {}
        # Pending Google OAuth flows (state → original MCP auth params)
        self._pending_auth: dict[str, dict] = {}

    # --- Client Registration ---

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        return self._clients.get(client_id)

    async def register_client(
        self, client_info: OAuthClientInformationFull
    ) -> None:
        self._clients[client_info.client_id] = client_info

    # --- Authorization ---

    async def authorize(
        self,
        client: OAuthClientInformationFull,
        params: AuthorizationParams,
    ) -> str:
        """Store MCP auth params, return Google OAuth URL."""
        state_id = secrets.token_urlsafe(32)

        self._pending_auth[state_id] = {
            "client_id": client.client_id,
            "redirect_uri": str(params.redirect_uri),
            "code_challenge": params.code_challenge,
            "redirect_uri_provided_explicitly": params.redirect_uri_provided_explicitly,
            "state": params.state,
            "scopes": params.scopes or [],
            "resource": params.resource,
        }

        google_params = {
            "client_id": self.google_client_id,
            "redirect_uri": self._callback_url(),
            "response_type": "code",
            "scope": GOOGLE_SCOPES,
            "state": state_id,
            "access_type": "offline",
            "prompt": "consent",
        }
        if self.allowed_domain:
            google_params["hd"] = self.allowed_domain

        return f"{GOOGLE_AUTH_URL}?{urlencode(google_params)}"

    # --- Token Exchange ---

    async def load_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: str,
    ) -> AuthorizationCode | None:
        code_obj = self._auth_codes.get(authorization_code)
        if code_obj and code_obj.expires_at < time.time():
            self._auth_codes.pop(authorization_code, None)
            self._google_tokens_by_code.pop(authorization_code, None)
            return None
        return code_obj

    async def exchange_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: AuthorizationCode,
    ) -> OAuthToken:
        google_creds = self._google_tokens_by_code.pop(
            authorization_code.code, None
        )
        if not google_creds:
            raise TokenError("invalid_grant")

        # Clean up auth code
        self._auth_codes.pop(authorization_code.code, None)

        # Create our tokens
        access_token_str = secrets.token_urlsafe(48)
        refresh_token_str = secrets.token_urlsafe(48)
        now = int(time.time())

        self._access_tokens[access_token_str] = AccessToken(
            token=access_token_str,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=now + 3600,
        )
        self._refresh_tokens[refresh_token_str] = RefreshToken(
            token=refresh_token_str,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=now + 86400 * 30,
        )

        # Link our tokens to Google credentials
        google_token_store[access_token_str] = google_creds
        access_token_to_user[access_token_str] = google_creds.get("email", "")
        self._google_tokens_by_refresh[refresh_token_str] = google_creds

        return OAuthToken(
            access_token=access_token_str,
            token_type="bearer",
            expires_in=3600,
            refresh_token=refresh_token_str,
            scope=" ".join(authorization_code.scopes) if authorization_code.scopes else None,
        )

    # --- Refresh ---

    async def load_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: str,
    ) -> RefreshToken | None:
        return self._refresh_tokens.get(refresh_token)

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        google_creds = self._google_tokens_by_refresh.get(refresh_token.token)
        if not google_creds:
            raise TokenError("invalid_grant")

        # Rotate tokens
        old_access = [
            k for k, v in access_token_to_user.items()
            if v == google_creds.get("email")
        ]
        for k in old_access:
            self._access_tokens.pop(k, None)
            google_token_store.pop(k, None)
            access_token_to_user.pop(k, None)

        new_access_str = secrets.token_urlsafe(48)
        new_refresh_str = secrets.token_urlsafe(48)
        now = int(time.time())

        self._access_tokens[new_access_str] = AccessToken(
            token=new_access_str,
            client_id=client.client_id,
            scopes=scopes or refresh_token.scopes,
            expires_at=now + 3600,
        )
        self._refresh_tokens[new_refresh_str] = RefreshToken(
            token=new_refresh_str,
            client_id=client.client_id,
            scopes=scopes or refresh_token.scopes,
            expires_at=now + 86400 * 30,
        )

        # Link new tokens to same Google creds
        google_token_store[new_access_str] = google_creds
        access_token_to_user[new_access_str] = google_creds.get("email", "")
        self._google_tokens_by_refresh[new_refresh_str] = google_creds

        # Revoke old refresh
        self._refresh_tokens.pop(refresh_token.token, None)
        self._google_tokens_by_refresh.pop(refresh_token.token, None)

        return OAuthToken(
            access_token=new_access_str,
            token_type="bearer",
            expires_in=3600,
            refresh_token=new_refresh_str,
            scope=" ".join(scopes) if scopes else None,
        )

    # --- Token Verification ---

    async def load_access_token(self, token: str) -> AccessToken | None:
        at = self._access_tokens.get(token)
        if at and at.expires_at and at.expires_at < int(time.time()):
            self._access_tokens.pop(token, None)
            google_token_store.pop(token, None)
            access_token_to_user.pop(token, None)
            return None
        return at

    # --- Revocation ---

    async def revoke_token(
        self,
        token: AccessToken | RefreshToken,
    ) -> None:
        if isinstance(token, AccessToken):
            self._access_tokens.pop(token.token, None)
            google_token_store.pop(token.token, None)
            access_token_to_user.pop(token.token, None)
        elif isinstance(token, RefreshToken):
            self._refresh_tokens.pop(token.token, None)
            self._google_tokens_by_refresh.pop(token.token, None)

    # --- Google OAuth Callback (custom route) ---

    async def handle_callback(self, request: Request):
        """Handle Google OAuth callback, exchange code, redirect to Claude."""
        error = request.query_params.get("error")
        if error:
            return RedirectResponse(f"/?error={error}")

        google_code = request.query_params.get("code")
        state_id = request.query_params.get("state")

        if not google_code or not state_id:
            return RedirectResponse("/?error=missing_params")

        pending = self._pending_auth.pop(state_id, None)
        if not pending:
            return RedirectResponse("/?error=invalid_state")

        # Exchange Google code for tokens
        async with httpx.AsyncClient() as client:
            token_resp = await client.post(
                GOOGLE_TOKEN_URL,
                data={
                    "code": google_code,
                    "client_id": self.google_client_id,
                    "client_secret": self.google_client_secret,
                    "redirect_uri": self._callback_url(),
                    "grant_type": "authorization_code",
                },
            )
            if token_resp.status_code != 200:
                return RedirectResponse("/?error=google_token_exchange_failed")
            google_tokens = token_resp.json()

            # Get user info
            userinfo_resp = await client.get(
                GOOGLE_USERINFO_URL,
                headers={"Authorization": f"Bearer {google_tokens['access_token']}"},
            )
            if userinfo_resp.status_code != 200:
                return RedirectResponse("/?error=google_userinfo_failed")
            user_info = userinfo_resp.json()

        # Verify domain
        email = user_info.get("email", "")
        logger.info("OAuth callback: user=%s", email)
        if self.allowed_domain and not email.endswith(f"@{self.allowed_domain}"):
            logger.warning("Domain rejected: email=%s allowed=%s", email, self.allowed_domain)
            return RedirectResponse("/?error=domain_not_allowed")

        # Create our auth code linked to Google tokens
        our_code = secrets.token_urlsafe(48)
        self._auth_codes[our_code] = AuthorizationCode(
            code=our_code,
            client_id=pending["client_id"],
            code_challenge=pending["code_challenge"],
            redirect_uri=pending["redirect_uri"],
            redirect_uri_provided_explicitly=pending["redirect_uri_provided_explicitly"],
            scopes=pending["scopes"],
            expires_at=time.time() + 600,
            resource=pending.get("resource"),
        )
        self._google_tokens_by_code[our_code] = {
            "access_token": google_tokens.get("access_token"),
            "refresh_token": google_tokens.get("refresh_token"),
            "email": email,
            "name": user_info.get("name", ""),
        }

        # Redirect to Claude Code's redirect_uri
        redirect_uri = construct_redirect_url(
            pending["redirect_uri"],
            code=our_code,
            state=pending.get("state"),
        )
        return RedirectResponse(redirect_uri)

    def _callback_url(self) -> str:
        base = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")
        return f"{base}/callback"
