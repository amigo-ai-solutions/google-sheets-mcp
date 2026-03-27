# Supersheets — Google Sheets MCP Server

Pandas-powered MCP server that gives Claude Code full data science capabilities over Google Sheets. Not just read/write — pivot tables, GROUP BY, VLOOKUP across sheets, time series analysis, outlier detection, and more.

## Tools (20)

### Core CRUD
| Tool | Description |
|---|---|
| `list_spreadsheets` | List all accessible spreadsheets |
| `get_spreadsheet_info` | Get spreadsheet metadata and worksheet names |
| `read_sheet` | Read data from a worksheet (full or range) |
| `write_cells` | Write data to a cell range |
| `append_rows` | Append rows to the end of a sheet |
| `create_worksheet` | Create a new worksheet tab |
| `search_cells` | Search for cells matching a query |

### Analytics & Transformation
| Tool | Description |
|---|---|
| `describe_sheet` | Statistical summary — types, stats, distributions |
| `query_sheet` | Filter rows with pandas expressions (`Revenue > 10000 and Status == 'Active'`) |
| `pivot_table` | Pivot table with rows, columns, values, aggregation |
| `group_by` | GROUP BY with multi-column aggregations |
| `vlookup` | Join/merge worksheets (even across spreadsheets) |
| `add_computed_column` | Add formula columns (`Revenue - Cost`) |
| `sort_sheet` | Sort by one or more columns |
| `deduplicate` | Remove duplicate rows |
| `fill_missing` | Fill blanks (value, forward fill, mean, median, mode) |
| `correlation_matrix` | Correlation between numeric columns |
| `histogram` | Frequency distribution with skew/kurtosis |
| `percentile_rank` | Add percentile rank (0-100) column |
| `cross_tab` | Cross-tabulation between two categorical columns |
| `time_series_resample` | Resample time series (daily→monthly, etc.) |
| `rolling_window` | Moving averages, rolling sums/std |
| `outlier_detection` | Detect outliers via IQR or Z-score |

## Setup

### 1. Create a Google Cloud Service Account

```bash
gcloud iam service-accounts create sheets-mcp \
  --display-name="Sheets MCP" \
  --project=amigo-poc

gcloud iam service-accounts keys create sa-key.json \
  --iam-account=sheets-mcp@amigo-poc.iam.gserviceaccount.com

gcloud services enable sheets.googleapis.com drive.googleapis.com \
  --project=amigo-poc
```

### 2. Share spreadsheets

Share each spreadsheet with the service account email:
`sheets-mcp@amigo-poc.iam.gserviceaccount.com`

### 3. Configure Claude Code

Add to `~/.claude/settings.json`:

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

"Show me monthly revenue trends from the transactions sheet"
→ time_series_resample(freq="M", aggfunc="sum")

"Which departments are overspending?"
→ group_by(by="Department", agg="Budget:sum,Actual:sum") + add_computed_column("Actual - Budget")

"Find outliers in the salary data"
→ outlier_detection(column="Salary", method="iqr")

"Join employee data with department budgets"
→ vlookup(left_worksheet="Employees", right_worksheet="Budgets", on="Department")

"Create a pivot of sales by region and product"
→ pivot_table(index="Region", columns="Product", values="Revenue", aggfunc="sum")
```

## Development

```bash
uv venv && source .venv/bin/activate
uv pip install -e .
google-sheets-mcp  # runs stdio MCP server
```
