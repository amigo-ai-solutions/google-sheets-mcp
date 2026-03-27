# Supersheets — Google Sheets MCP Server

## What This Is
A pandas-powered MCP server giving Claude Code full data science capabilities
over Google Sheets. Supports two modes:
- **Local (stdio)**: Service account auth, runs as a local process
- **Hosted (SSE + OAuth)**: Deployed on Cloud Run, users sign in with Google

Built with `FastMCP` + `gspread` + `pandas` + `numpy` + `google-api-python-client`.

## Project Structure
```
google_sheets_mcp/
  __init__.py
  server.py          # MCP tools (37 tools) + auth helpers
  auth.py            # GoogleOAuthProvider (MCP OAuth wrapping Google)
  app.py             # Hosted entry point (SSE + OAuth + health check)
  config.py          # Pydantic settings (env vars)
  logging_config.py  # Structured JSON logging (Cloud Logging compatible)
pyproject.toml       # Deps, ruff config, entry points
Dockerfile           # Multi-stage, non-root user, health check
```

## Code Standards (match ../medina-health-kiosk-proxy)
- `from __future__ import annotations` at top of every `.py` file
- Ruff: `line-length = 100`, rules `E, F, I, N, W, UP`, target `py312`
- Structured JSON logging via `logging_config.StructuredFormatter`
- Pydantic `BaseSettings` for all config (`config.py`)
- Full type hints, `|` for unions, `list[T]` for generics
- Multi-stage Dockerfile, non-root user, `PYTHONUNBUFFERED=1`
- No `print()` — always `logger.info/warning/exception()`

## Available MCP Tools (37)

### Core CRUD (8)
| Tool | Purpose | Mutates? |
|---|---|---|
| `list_spreadsheets` | List all accessible spreadsheets (optionally by Drive folder) | No |
| `get_spreadsheet_info` | Get worksheet names, row/col counts | No |
| `read_sheet` | Read data (full or A1 range) | No |
| `get_sheet_formulas` | Read formulas, not computed values | No |
| `write_cells` | Write 2D array to A1 range | **Yes** |
| `batch_update_cells` | Update multiple ranges at once | **Yes** |
| `append_rows` | Append rows to end | **Yes** |
| `search_cells` | Search across one or all sheets | No |

### Structure (5)
| Tool | Purpose | Mutates? |
|---|---|---|
| `create_worksheet` | Add a new tab | **Yes** |
| `add_rows` | Insert rows at position | **Yes** |
| `add_columns` | Insert columns at position | **Yes** |
| `copy_sheet` | Copy worksheet across spreadsheets | **Yes** |
| `rename_sheet` | Rename a worksheet tab | **Yes** |

### Multi-Read (2)
| Tool | Purpose | Mutates? |
|---|---|---|
| `get_multiple_sheet_data` | Batch read from multiple ranges/sheets | No |
| `get_multiple_spreadsheet_summary` | Preview headers + rows from multiple spreadsheets | No |

### Drive (4)
| Tool | Purpose | Mutates? |
|---|---|---|
| `create_spreadsheet` | Create new spreadsheet (optionally in folder) | **Yes** |
| `share_spreadsheet` | Share with users (reader/commenter/writer) | **Yes** |
| `search_spreadsheets` | Search Drive by name | No |
| `list_folders` | List Drive folders | No |

### Raw API (2)
| Tool | Purpose | Mutates? |
|---|---|---|
| `batch_update` | Raw Sheets API batchUpdate (full power) | **Yes** |
| `add_chart` | Add chart overlay to a sheet | **Yes** |

### Analytics (16)
| Tool | Purpose | Mutates? |
|---|---|---|
| `describe_sheet` | Statistical summary — types, stats, distributions | No |
| `query_sheet` | Filter with pandas expressions (`Revenue > 10000`) | No |
| `pivot_table` | Pivot table with aggregation | Optional |
| `group_by` | GROUP BY with multi-column aggregations | Optional |
| `vlookup` | Join/merge worksheets (even cross-spreadsheet) | Optional |
| `add_computed_column` | Formula column (`Revenue - Cost`) | **Yes** |
| `sort_sheet` | Sort by columns | **Yes** |
| `deduplicate` | Remove duplicate rows | **Yes** |
| `fill_missing` | Fill blanks (value, ffill, mean, median, mode) | **Yes** |
| `correlation_matrix` | Correlation between numeric columns | Optional |
| `histogram` | Frequency distribution with stats | No |
| `percentile_rank` | Add percentile rank (0-100) | **Yes** |
| `cross_tab` | Cross-tabulation between two columns | Optional |
| `time_series_resample` | Resample time series (daily→monthly) | Optional |
| `rolling_window` | Moving averages, rolling sums/std | **Yes** |
| `outlier_detection` | Detect outliers via IQR or Z-score | No |

## Authentication

### Hosted Mode (Cloud Run + OAuth)
Users sign in with their @amigo.ai Google account. The server uses their
Google OAuth token to access their sheets — each user sees their own data.

Flow:
1. Claude Code connects to SSE URL
2. Gets 401 → discovers OAuth metadata at `/.well-known/oauth-authorization-server`
3. Opens browser → Google sign-in → user authorizes
4. Server issues MCP access token linked to user's Google credentials
5. All subsequent MCP calls use the user's Google token for Sheets/Drive

### Local Mode (stdio + Service Account)
Set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json` env var.
Share spreadsheets with the SA email.

## Configuration (Pydantic Settings)
All via env vars (see `config.py`):
| Var | Description | Default |
|---|---|---|
| `GOOGLE_OAUTH_CLIENT_ID` | Google OAuth client ID | (required for hosted) |
| `GOOGLE_OAUTH_CLIENT_SECRET` | Google OAuth client secret | (required for hosted) |
| `ALLOWED_DOMAIN` | Restrict auth to this email domain | `amigo.ai` |
| `BASE_URL` | Server URL for OAuth callbacks | auto-detected |
| `LOG_LEVEL` | Logging level | `INFO` |
| `PORT` | Server port | `8000` |
| `GOOGLE_APPLICATION_CREDENTIALS` | SA key path (local mode) | |

## Deployment

### Cloud Run (hosted mode)
Managed via Terraform in `../poc-infra/sheets_mcp.tf`:
```bash
# Build and push
docker build --platform linux/amd64 -t us-east1-docker.pkg.dev/amigo-poc/sheets-mcp/sheets-mcp:latest .
docker push us-east1-docker.pkg.dev/amigo-poc/sheets-mcp/sheets-mcp:latest

# Deploy
cd ../poc-infra && terraform apply
```

### User config (Claude Code)
```json
{
  "mcpServers": {
    "google-sheets": {
      "type": "sse",
      "url": "https://<cloud-run-url>/sse"
    }
  }
}
```

## Commands
```bash
# Development
pip install -e ".[dev]"
google-sheets-mcp              # Local stdio mode
google-sheets-mcp-hosted       # Hosted SSE mode

# Linting
ruff check google_sheets_mcp/

# Docker
docker build --platform linux/amd64 -t sheets-mcp .
docker run -e GOOGLE_OAUTH_CLIENT_ID=... -e GOOGLE_OAUTH_CLIENT_SECRET=... -p 8000:8000 sheets-mcp
```

## Adding a New Tool
1. Add a `@mcp.tool()` function in `server.py`
2. For read-only analytics: `_sheet_to_df()` → compute → `_df_to_json()`
3. For write-back: compute → `_df_to_sheet()` → return status JSON
4. For Drive/raw API: use `_get_drive_service()` / `_get_sheets_service()`
5. All return values must be JSON strings
6. Add `from __future__ import annotations` if creating a new file
7. Update tool tables above and in `README.md`

## Conventions
- All tool return values are JSON strings — MCP expects string content
- No classes for tools — plain `@mcp.tool()` functions
- DataFrames are internal; JSON strings are external
- `_df_to_json()` caps at 500 rows for context safety
- Numeric columns auto-detected when reading sheets
- Error handling left to gspread/pandas/MCP framework
- Python 3.12+, ruff-clean, typed
