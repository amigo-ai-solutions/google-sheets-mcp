# Supersheets — Google Sheets MCP Server

## What This Is
A pandas-powered MCP server that gives Claude Code full data science capabilities
on Google Sheets via stdio transport. Built with `FastMCP` + `gspread` + `pandas` + `numpy`.

Not just CRUD — this is a full analytics engine over spreadsheets.

## Architecture
```
google_sheets_mcp/
  __init__.py
  server.py        # All tools, helpers, and auth (~550 lines)
pyproject.toml     # Package config, dependencies, entry point
```

Single-module server. All tools in `server.py`. Key internal helpers:
- `_get_client()` — authenticated gspread client
- `_sheet_to_df()` — read worksheet → pandas DataFrame (auto-detects numeric types)
- `_df_to_json()` — DataFrame → JSON (truncates at 500 rows for context safety)
- `_df_to_sheet()` — DataFrame → write back to worksheet (replaces all content)

## Available MCP Tools

### Core CRUD (7 tools)
| Tool | Purpose | Mutates? |
|---|---|---|
| `list_spreadsheets` | List all spreadsheets the SA can access | No |
| `get_spreadsheet_info` | Get worksheet names, row/col counts | No |
| `read_sheet` | Read all data or a specific A1 range | No |
| `write_cells` | Write a 2D array to a specific A1 range | **Yes** |
| `append_rows` | Append rows to end of worksheet | **Yes** |
| `create_worksheet` | Add a new tab to a spreadsheet | **Yes** |
| `search_cells` | Find cells matching a text query | No |

### Analytics & Transformation (13 tools)
| Tool | Purpose | Mutates? |
|---|---|---|
| `describe_sheet` | Full statistical summary — dtypes, stats, value counts | No |
| `query_sheet` | Filter rows with pandas query expressions (like SQL WHERE) | No |
| `pivot_table` | Pivot table — rows, columns, values, aggregation | Optional |
| `group_by` | GROUP BY with multiple aggregations (sum, mean, count, etc.) | Optional |
| `vlookup` | Join/merge two worksheets (left/right/inner/outer) — cross-spreadsheet | Optional |
| `add_computed_column` | Add calculated column using pandas expressions | **Yes** |
| `sort_sheet` | Sort by one or more columns | **Yes** |
| `deduplicate` | Remove duplicate rows | **Yes** |
| `fill_missing` | Fill blanks with value, forward fill, mean, median, mode | **Yes** |
| `correlation_matrix` | Correlation between numeric columns | Optional |
| `histogram` | Frequency distribution with stats (mean, median, skew) | No |
| `percentile_rank` | Add percentile rank column (0-100) | **Yes** |
| `cross_tab` | Cross-tabulation / contingency table between two columns | Optional |
| `time_series_resample` | Resample time series (daily→monthly, etc.) | Optional |
| `rolling_window` | Rolling/moving averages, sums, std | **Yes** |
| `outlier_detection` | Detect outliers via IQR or Z-score | No |

"Optional" = writes to a new worksheet tab only if `write_to_worksheet` is provided.

### Tool Patterns

**Read-only analysis** — tools return JSON to Claude, sheet is unchanged:
```
describe_sheet, query_sheet, histogram, outlier_detection, correlation_matrix
```

**Write-back** — tools modify the source sheet (controlled by `write_back=True`):
```
add_computed_column, sort_sheet, deduplicate, fill_missing, percentile_rank, rolling_window
```

**Write-to-new-tab** — tools optionally write results to a separate worksheet:
```
pivot_table, group_by, vlookup, cross_tab, time_series_resample, correlation_matrix
```

### Common Parameters
- `spreadsheet_id` — the ID from a Google Sheets URL: `https://docs.google.com/spreadsheets/d/{THIS_PART}/edit`
- `worksheet` — the tab name (e.g. "Sheet1", "Revenue", "Q1 Data")
- `range` — A1 notation (e.g. "A1:D10", "B2:B")
- `header_row` — which row has column headers (1-indexed, default: 1)
- `values` / `rows` — always a 2D array: `[["a", "b"], ["c", "d"]]`
- `write_to_worksheet` — target tab for results (creates if missing)
- `write_back` — boolean, whether to save changes to the source sheet

### Query Syntax (for `query_sheet`)
Uses pandas `DataFrame.query()`:
- `"Revenue > 10000"`
- `"Status == 'Active' and Region == 'MENA'"`
- `"Age.between(25, 40)"`
- `"Name.str.contains('Ali')"`
- Column names with spaces are auto-converted to underscores

### Aggregation Functions
Used in `group_by`, `pivot_table`, `cross_tab`, `time_series_resample`:
`sum`, `mean`, `count`, `min`, `max`, `median`, `nunique`, `first`, `last`

### Expression Syntax (for `add_computed_column`)
Uses pandas `eval()` with numpy fallback:
- `"Revenue - Cost"` → profit
- `"Quantity * Unit_Price"` → total
- `"Score / Score.max() * 100"` → percentage
- `"Name.str.upper()"` → uppercase

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

## Adding a New Tool
1. Add a new `@mcp.tool()` function in `server.py`
2. For read-only analytics: use `_sheet_to_df()` → compute → return `_df_to_json()`
3. For write-back tools: compute → `_df_to_sheet()` → return status JSON
4. All return values must be JSON strings
5. Update the tool tables above and in `README.md`

## Conventions
- All tool return values are JSON strings (not dicts) — MCP expects string content
- No classes, no abstractions — plain functions with the `@mcp.tool()` decorator
- DataFrames are the internal representation; JSON strings are the external interface
- `_df_to_json()` caps at 500 rows to avoid blowing up Claude's context window
- Numeric columns are auto-detected when reading sheets
- Error handling is left to gspread/pandas/MCP framework (they surface useful errors)
- Python 3.11+, typed, dependencies: mcp, gspread, google-auth, pandas, numpy
