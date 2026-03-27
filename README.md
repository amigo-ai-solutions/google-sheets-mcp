# Supersheets — Google Sheets MCP Server

Pandas-powered MCP server giving Claude Code full data science capabilities over Google Sheets. 35 tools: CRUD, Drive ops, pivot tables, GROUP BY, VLOOKUP, time series, outlier detection, charts, and more.

**Two modes:**
- **Hosted** — Deploy on Cloud Run with Google OAuth. Users sign in with their Google account.
- **Local** — Run as stdio MCP server with a service account key.

## Tools (35)

### Core CRUD (8)
| Tool | Description |
|---|---|
| `list_spreadsheets` | List all accessible spreadsheets (optionally by folder) |
| `get_spreadsheet_info` | Spreadsheet metadata and worksheet names |
| `read_sheet` | Read data (full or A1 range) |
| `get_sheet_formulas` | Read formulas, not computed values |
| `write_cells` | Write data to a cell range |
| `batch_update_cells` | Update multiple ranges in one call |
| `append_rows` | Append rows to end of sheet |
| `search_cells` | Search across one or all sheets |

### Structure (5)
| Tool | Description |
|---|---|
| `create_worksheet` | Create a new worksheet tab |
| `add_rows` | Insert rows at position |
| `add_columns` | Insert columns at position |
| `copy_sheet` | Copy worksheet across spreadsheets |
| `rename_sheet` | Rename a worksheet tab |

### Multi-Read (2)
| Tool | Description |
|---|---|
| `get_multiple_sheet_data` | Batch read from multiple ranges/sheets |
| `get_multiple_spreadsheet_summary` | Preview multiple spreadsheets |

### Drive (4)
| Tool | Description |
|---|---|
| `create_spreadsheet` | Create new spreadsheet |
| `share_spreadsheet` | Share with users (reader/commenter/writer) |
| `search_spreadsheets` | Search Drive for spreadsheets |
| `list_folders` | List Google Drive folders |

### Raw API (2)
| Tool | Description |
|---|---|
| `batch_update` | Raw Sheets API batchUpdate (full power) |
| `add_chart` | Add chart overlay (column, bar, line, pie, scatter, etc.) |

### Analytics & Transformation (14)
| Tool | Description |
|---|---|
| `describe_sheet` | Statistical summary — types, stats, distributions |
| `query_sheet` | Filter with pandas expressions (`Revenue > 10000 and Status == 'Active'`) |
| `pivot_table` | Pivot table with rows, columns, values, aggregation |
| `group_by` | GROUP BY with multi-column aggregations |
| `vlookup` | Join/merge worksheets (even cross-spreadsheet) |
| `add_computed_column` | Formula columns (`Revenue - Cost`) |
| `sort_sheet` | Sort by one or more columns |
| `deduplicate` | Remove duplicate rows |
| `fill_missing` | Fill blanks (value, forward fill, mean, median, mode) |
| `correlation_matrix` | Correlation between numeric columns |
| `histogram` | Frequency distribution with skew stats |
| `percentile_rank` | Add percentile rank (0-100) column |
| `cross_tab` | Cross-tabulation between two categorical columns |
| `time_series_resample` | Resample time series (daily→monthly, etc.) |
| `rolling_window` | Moving averages, rolling sums/std |
| `outlier_detection` | Detect outliers via IQR or Z-score |

## Setup — Hosted Mode (Cloud Run + Google OAuth)

### 1. Create OAuth Client

In GCP Console → APIs & Services → Credentials → Create OAuth 2.0 Client ID:
- Application type: **Web application**
- Authorized redirect URIs: `https://<cloud-run-url>/callback`

Store the client ID and secret in Secret Manager (Terraform handles this).

### 2. Deploy via Terraform

```bash
# Build and push image
docker build --platform linux/amd64 -t me-central1-docker.pkg.dev/amigo-poc/kiosk-proxy/sheets-mcp:v1 .
docker push me-central1-docker.pkg.dev/amigo-poc/kiosk-proxy/sheets-mcp:v1

# Set OAuth secrets
gcloud secrets versions add sheets-mcp-oauth-client-id --data-file=<(echo -n "YOUR_CLIENT_ID") --project=amigo-poc
gcloud secrets versions add sheets-mcp-oauth-client-secret --data-file=<(echo -n "YOUR_CLIENT_SECRET") --project=amigo-poc

# Deploy
cd ../poc-infra && terraform apply
```

### 3. Configure Claude Code

Add to `~/.claude/settings.json`:

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

First use will open a browser for Google sign-in. After that, it just works.

## Setup — Local Mode (Service Account)

### 1. Create Service Account

```bash
gcloud iam service-accounts create sheets-mcp \
  --display-name="Sheets MCP" --project=amigo-poc

gcloud iam service-accounts keys create sa-key.json \
  --iam-account=sheets-mcp@amigo-poc.iam.gserviceaccount.com

gcloud services enable sheets.googleapis.com drive.googleapis.com --project=amigo-poc
```

### 2. Share spreadsheets with the SA email

### 3. Configure Claude Code

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

## Examples

```
"Summarize the Q1 revenue sheet"
→ describe_sheet + query_sheet

"Show monthly revenue trends"
→ time_series_resample(freq="M", aggfunc="sum")

"Which departments overspent?"
→ group_by(by="Department", agg="Budget:sum,Actual:sum") + add_computed_column("Actual - Budget")

"Find salary outliers"
→ outlier_detection(column="Salary", method="iqr")

"Join employees with department budgets"
→ vlookup(left_worksheet="Employees", right_worksheet="Budgets", on="Department")

"Create a pivot of sales by region and product"
→ pivot_table(index="Region", columns="Product", values="Revenue", aggfunc="sum")

"Add a revenue chart"
→ add_chart(chart_type="LINE", data_range="A1:C12", title="Monthly Revenue")
```

## Development

```bash
pip install -e ".[dev]"
google-sheets-mcp              # Local stdio mode
google-sheets-mcp-hosted       # Hosted SSE mode
ruff check google_sheets_mcp/  # Lint
pytest                         # Test
```
