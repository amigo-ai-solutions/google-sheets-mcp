# Google Sheets MCP Server

## What This Is
An MCP (Model Context Protocol) server that gives Claude Code read/write access
to Google Sheets via stdio transport. Built with `FastMCP` + `gspread`.

## Architecture
```
google_sheets_mcp/
  __init__.py
  server.py        # All tools + auth logic (single file, ~175 lines)
pyproject.toml     # Package config, dependencies, entry point
```

This is intentionally a single-module server. All tools live in `server.py`.
Do not split into multiple files unless it exceeds ~500 lines.

## Available MCP Tools

| Tool | Purpose | Mutates? |
|---|---|---|
| `list_spreadsheets` | List all spreadsheets the SA can access | No |
| `get_spreadsheet_info` | Get worksheet names, row/col counts | No |
| `read_sheet` | Read all data or a specific A1 range | No |
| `write_cells` | Write a 2D array to a specific A1 range | **Yes** |
| `append_rows` | Append rows to end of worksheet | **Yes** |
| `create_worksheet` | Add a new tab to a spreadsheet | **Yes** |
| `search_cells` | Find cells matching a text query | No |

### Tool Parameters
- `spreadsheet_id` — the ID from a Google Sheets URL: `https://docs.google.com/spreadsheets/d/{THIS_PART}/edit`
- `worksheet` — the tab name (e.g. "Sheet1", "Revenue", "Q1 Data")
- `range` — A1 notation (e.g. "A1:D10", "B2:B", "A:A")
- `values` / `rows` — always a 2D array: `[["a", "b"], ["c", "d"]]`

## Auth
- Uses `GOOGLE_APPLICATION_CREDENTIALS` env var pointing to a service account JSON key
- Falls back to Application Default Credentials if env var is not set
- Scopes: `spreadsheets` (read/write) + `drive.readonly` (list files)
- Spreadsheets must be shared with the service account email

## How Users Install
```json
{
  "mcpServers": {
    "google-sheets": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/amigo-ai-solutions/google-sheets-mcp", "google-sheets-mcp"],
      "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/sa-key.json"
      }
    }
  }
}
```

## Development
```bash
uv venv && source .venv/bin/activate
uv pip install -e .
google-sheets-mcp          # run the server (stdio)
```

## Commands
```bash
uv pip install -e .        # Install in dev mode
python -m google_sheets_mcp.server   # Run directly
```

## Adding a New Tool
1. Add a new `@mcp.tool()` function in `server.py`
2. Use `_get_client()` to get an authenticated `gspread.Client`
3. Return `json.dumps(...)` — all tools return JSON strings
4. Update the tool table above and in `README.md`

## Conventions
- All tool return values are JSON strings (not dicts) — MCP expects string content
- No classes, no abstractions — plain functions with the `@mcp.tool()` decorator
- Error handling is left to gspread/MCP framework (they surface useful errors)
- Python 3.11+, no type:ignore, no noqa
