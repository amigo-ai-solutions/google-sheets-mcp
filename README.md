# Google Sheets MCP Server

MCP server that gives Claude Code read/write access to Google Sheets.

## Tools

| Tool | Description |
|---|---|
| `list_spreadsheets` | List all accessible spreadsheets |
| `get_spreadsheet_info` | Get spreadsheet metadata and worksheet names |
| `read_sheet` | Read data from a worksheet (full or range) |
| `write_cells` | Write data to a cell range |
| `append_rows` | Append rows to the end of a sheet |
| `create_worksheet` | Create a new worksheet tab |
| `search_cells` | Search for cells matching a query |

## Setup

### 1. Create a Google Cloud Service Account

```bash
# Create SA
gcloud iam service-accounts create sheets-mcp \
  --display-name="Sheets MCP" \
  --project=amigo-poc

# Download key
gcloud iam service-accounts keys create sa-key.json \
  --iam-account=sheets-mcp@amigo-poc.iam.gserviceaccount.com

# Enable Sheets + Drive APIs
gcloud services enable sheets.googleapis.com drive.googleapis.com \
  --project=amigo-poc
```

### 2. Share spreadsheets

Share each spreadsheet with the service account email:
`sheets-mcp@amigo-poc.iam.gserviceaccount.com`

### 3. Configure Claude Code

Add to your `~/.claude/settings.json`:

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

Or if installed locally:

```json
{
  "mcpServers": {
    "google-sheets": {
      "command": "google-sheets-mcp",
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
google-sheets-mcp  # runs stdio MCP server
```
