"""Supersheets — pandas-powered Google Sheets MCP server for Claude Code."""

import json
import os

import google.auth
import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("google-sheets")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _get_client() -> gspread.Client:
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path:
        creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=SCOPES
        )
    else:
        creds, _ = google.auth.default(scopes=SCOPES)
    return gspread.authorize(creds)


def _sheet_to_df(
    spreadsheet_id: str,
    worksheet: str = "Sheet1",
    range: str | None = None,
    header_row: int = 1,
) -> pd.DataFrame:
    """Read a worksheet into a pandas DataFrame."""
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)

    if range:
        data = ws.get(range)
    else:
        data = ws.get_all_values()

    if not data:
        return pd.DataFrame()

    if header_row >= 1 and len(data) >= header_row:
        headers = data[header_row - 1]
        rows = data[header_row:]
        df = pd.DataFrame(rows, columns=headers)
    else:
        df = pd.DataFrame(data)

    # Auto-convert numeric columns
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")

    return df


def _df_to_json(df: pd.DataFrame, max_rows: int = 500) -> str:
    """Serialize a DataFrame to JSON, truncating if needed."""
    truncated = len(df) > max_rows
    out = df.head(max_rows)
    result = {
        "columns": list(out.columns),
        "shape": [len(df), len(df.columns)],
        "data": json.loads(out.to_json(orient="records", default_handler=str)),
    }
    if truncated:
        result["truncated"] = True
        result["showing"] = max_rows
    return json.dumps(result, indent=2, default=str)


def _df_to_sheet(
    df: pd.DataFrame,
    spreadsheet_id: str,
    worksheet: str,
    create_if_missing: bool = True,
) -> dict:
    """Write a DataFrame back to a worksheet (replaces all content)."""
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)

    try:
        ws = sh.worksheet(worksheet)
        ws.clear()
    except gspread.WorksheetNotFound:
        if create_if_missing:
            ws = sh.add_worksheet(
                title=worksheet, rows=len(df) + 1, cols=len(df.columns)
            )
        else:
            raise

    # Convert everything to strings for Sheets
    header = [list(df.columns)]
    values = df.astype(str).replace("nan", "").values.tolist()
    ws.update("A1", header + values)
    return {"status": "ok", "worksheet": worksheet, "rows": len(df), "cols": len(df.columns)}


# ---------------------------------------------------------------------------
# Core CRUD tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_spreadsheets() -> str:
    """List all spreadsheets accessible to the service account."""
    gc = _get_client()
    sheets = gc.openall()
    results = [{"title": s.title, "id": s.id, "url": s.url} for s in sheets]
    return json.dumps(results, indent=2)


@mcp.tool()
def get_spreadsheet_info(spreadsheet_id: str) -> str:
    """Get metadata about a spreadsheet including all worksheet names.

    Args:
        spreadsheet_id: The spreadsheet ID (from the URL or list_spreadsheets).
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    worksheets = [
        {"title": ws.title, "rows": ws.row_count, "cols": ws.col_count, "id": ws.id}
        for ws in sh.worksheets()
    ]
    return json.dumps(
        {"title": sh.title, "id": sh.id, "url": sh.url, "worksheets": worksheets},
        indent=2,
    )


@mcp.tool()
def read_sheet(
    spreadsheet_id: str,
    worksheet: str = "Sheet1",
    range: str | None = None,
) -> str:
    """Read data from a Google Sheets worksheet as raw rows.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name (default: Sheet1).
        range: Optional A1 notation range (e.g. "A1:D10"). Reads all data if omitted.
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)

    if range:
        data = ws.get(range)
    else:
        data = ws.get_all_values()

    return json.dumps(data, indent=2)


@mcp.tool()
def write_cells(
    spreadsheet_id: str,
    worksheet: str,
    range: str,
    values: list[list[str]],
) -> str:
    """Write data to a range of cells in a worksheet.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        range: A1 notation range (e.g. "A1:C3").
        values: 2D array of values to write, row by row.
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)
    ws.update(range, values)
    return json.dumps({"status": "ok", "range": range, "rows_written": len(values)})


@mcp.tool()
def append_rows(
    spreadsheet_id: str,
    worksheet: str,
    rows: list[list[str]],
) -> str:
    """Append rows to the end of a worksheet.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        rows: 2D array of rows to append.
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)
    ws.append_rows(rows)
    return json.dumps({"status": "ok", "rows_appended": len(rows)})


@mcp.tool()
def create_worksheet(
    spreadsheet_id: str,
    title: str,
    rows: int = 1000,
    cols: int = 26,
) -> str:
    """Create a new worksheet tab in a spreadsheet.

    Args:
        spreadsheet_id: The spreadsheet ID.
        title: Name for the new worksheet.
        rows: Number of rows (default: 1000).
        cols: Number of columns (default: 26).
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
    return json.dumps({"status": "ok", "title": ws.title, "id": ws.id})


@mcp.tool()
def search_cells(
    spreadsheet_id: str,
    worksheet: str,
    query: str,
) -> str:
    """Search for cells matching a query string in a worksheet.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        query: Text to search for.
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)
    cells = ws.findall(query)
    results = [
        {"row": c.row, "col": c.col, "value": c.value, "address": c.label}
        for c in cells
    ]
    return json.dumps(results, indent=2)


# ---------------------------------------------------------------------------
# DataFrame analytics tools (pandas + numpy)
# ---------------------------------------------------------------------------


@mcp.tool()
def describe_sheet(
    spreadsheet_id: str,
    worksheet: str = "Sheet1",
    header_row: int = 1,
) -> str:
    """Get a statistical summary of a worksheet — row count, column types, descriptive
    stats (mean, median, std, min, max, quartiles) for numeric columns, and value
    counts for categorical columns with few unique values.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        header_row: Row number containing headers (1-indexed, default: 1).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)

    info = {
        "shape": {"rows": len(df), "columns": len(df.columns)},
        "columns": {},
    }

    for col in df.columns:
        col_info: dict = {"dtype": str(df[col].dtype)}
        non_null = df[col].notna().sum()
        col_info["non_null_count"] = int(non_null)
        col_info["null_count"] = int(len(df) - non_null)

        if pd.api.types.is_numeric_dtype(df[col]):
            desc = df[col].describe()
            col_info["stats"] = {
                "mean": round(float(desc["mean"]), 4) if "mean" in desc else None,
                "std": round(float(desc["std"]), 4) if "std" in desc else None,
                "min": float(desc["min"]) if "min" in desc else None,
                "25%": float(desc["25%"]) if "25%" in desc else None,
                "50%": float(desc["50%"]) if "50%" in desc else None,
                "75%": float(desc["75%"]) if "75%" in desc else None,
                "max": float(desc["max"]) if "max" in desc else None,
                "sum": round(float(df[col].sum()), 4),
            }
        else:
            nunique = df[col].nunique()
            col_info["unique_values"] = int(nunique)
            if 0 < nunique <= 20:
                col_info["value_counts"] = (
                    df[col].value_counts().head(20).to_dict()
                )
            else:
                col_info["sample_values"] = df[col].dropna().head(5).tolist()

        info["columns"][str(col)] = col_info

    return json.dumps(info, indent=2, default=str)


@mcp.tool()
def query_sheet(
    spreadsheet_id: str,
    worksheet: str,
    query: str,
    header_row: int = 1,
) -> str:
    """Filter rows using a pandas query expression. Supports full pandas query syntax
    including comparisons, boolean logic, string methods, and arithmetic.

    Examples:
      - "Revenue > 10000"
      - "Status == 'Active' and Region == 'MENA'"
      - "Age.between(25, 40)"
      - "Name.str.contains('Ali')"

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        query: A pandas DataFrame.query() expression.
        header_row: Row number containing headers (1-indexed, default: 1).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    # Normalize column names for query compatibility
    clean_cols = {c: c.strip().replace(" ", "_") for c in df.columns}
    df = df.rename(columns=clean_cols)
    # Also normalize the query
    normalized_query = query
    for orig, clean in clean_cols.items():
        if orig != clean:
            normalized_query = normalized_query.replace(orig, clean)
    result = df.query(normalized_query)
    return _df_to_json(result)


@mcp.tool()
def pivot_table(
    spreadsheet_id: str,
    worksheet: str,
    index: str,
    values: str,
    columns: str | None = None,
    aggfunc: str = "sum",
    header_row: int = 1,
    write_to_worksheet: str | None = None,
) -> str:
    """Create a pivot table from sheet data, like an Excel PivotTable.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Source worksheet name.
        index: Column name(s) for rows. Comma-separated for multiple (e.g. "Region,City").
        values: Column name(s) to aggregate. Comma-separated for multiple (e.g. "Revenue,Cost").
        columns: Optional column name to pivot into columns.
        aggfunc: Aggregation function — sum, mean, count, min, max, median (default: sum).
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes result to this worksheet tab.
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)

    idx = [c.strip() for c in index.split(",")]
    vals = [c.strip() for c in values.split(",")]
    cols = [c.strip() for c in columns.split(",")] if columns else None

    func_map = {
        "sum": "sum", "mean": "mean", "count": "count",
        "min": "min", "max": "max", "median": "median",
    }
    fn = func_map.get(aggfunc, "sum")

    pivot = pd.pivot_table(df, index=idx, values=vals, columns=cols, aggfunc=fn)
    pivot = pivot.reset_index()

    # Flatten MultiIndex columns
    if isinstance(pivot.columns, pd.MultiIndex):
        pivot.columns = ["_".join(str(c) for c in col).strip("_") for col in pivot.columns]

    if write_to_worksheet:
        result = _df_to_sheet(pivot, spreadsheet_id, write_to_worksheet)
        return json.dumps({**result, "preview": json.loads(_df_to_json(pivot.head(20)))}, default=str)

    return _df_to_json(pivot)


@mcp.tool()
def group_by(
    spreadsheet_id: str,
    worksheet: str,
    by: str,
    agg: str,
    header_row: int = 1,
    write_to_worksheet: str | None = None,
) -> str:
    """Group rows and compute aggregations, like SQL GROUP BY.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        by: Column(s) to group by. Comma-separated for multiple (e.g. "Department,Role").
        agg: Aggregation spec as "column:func" pairs, comma-separated.
             Functions: sum, mean, count, min, max, median, nunique, first, last.
             Examples: "Revenue:sum,Cost:mean,Name:count"
                       "Amount:sum,Amount:mean" (multiple aggs on same column)
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes result to this worksheet tab.
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)

    group_cols = [c.strip() for c in by.split(",")]
    agg_dict: dict[str, list[str]] = {}
    for pair in agg.split(","):
        col, func = pair.strip().rsplit(":", 1)
        col, func = col.strip(), func.strip()
        agg_dict.setdefault(col, []).append(func)

    result = df.groupby(group_cols).agg(agg_dict)
    # Flatten column names
    result.columns = ["_".join(col).strip("_") for col in result.columns]
    result = result.reset_index()

    if write_to_worksheet:
        write_result = _df_to_sheet(result, spreadsheet_id, write_to_worksheet)
        return json.dumps({**write_result, "preview": json.loads(_df_to_json(result.head(20)))}, default=str)

    return _df_to_json(result)


@mcp.tool()
def vlookup(
    spreadsheet_id: str,
    left_worksheet: str,
    right_worksheet: str,
    on: str,
    right_spreadsheet_id: str | None = None,
    how: str = "left",
    header_row: int = 1,
    write_to_worksheet: str | None = None,
) -> str:
    """Join/merge two worksheets like VLOOKUP but more powerful. Supports left, right,
    inner, and outer joins across worksheets or even across different spreadsheets.

    Args:
        spreadsheet_id: The spreadsheet ID for the left (primary) sheet.
        left_worksheet: Left worksheet name.
        right_worksheet: Right worksheet name.
        on: Column name(s) to join on. Comma-separated for composite keys (e.g. "ID" or "FirstName,LastName").
        right_spreadsheet_id: Spreadsheet ID for the right sheet if different from left. Omit if same spreadsheet.
        how: Join type — left, right, inner, outer (default: left).
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes result to this worksheet tab.
    """
    left_df = _sheet_to_df(spreadsheet_id, left_worksheet, header_row=header_row)
    right_sid = right_spreadsheet_id or spreadsheet_id
    right_df = _sheet_to_df(right_sid, right_worksheet, header_row=header_row)

    keys = [c.strip() for c in on.split(",")]
    merged = left_df.merge(right_df, on=keys, how=how, suffixes=("", "_right"))

    if write_to_worksheet:
        write_result = _df_to_sheet(merged, spreadsheet_id, write_to_worksheet)
        return json.dumps({**write_result, "preview": json.loads(_df_to_json(merged.head(20)))}, default=str)

    return _df_to_json(merged)


@mcp.tool()
def add_computed_column(
    spreadsheet_id: str,
    worksheet: str,
    new_column: str,
    expression: str,
    header_row: int = 1,
    write_back: bool = True,
) -> str:
    """Add a calculated column using a pandas expression, like a spreadsheet formula
    applied to every row. The expression can reference other columns by name.

    Examples:
      - "Revenue - Cost" → profit column
      - "Quantity * Unit_Price" → total
      - "Score / Score.max() * 100" → percentage
      - "Name.str.upper()" → uppercase names
      - "pd.to_datetime(Date).dt.month" → extract month

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        new_column: Name for the new column.
        expression: Pandas expression using column names. Evaluated with df.eval() or pd.eval().
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes the full sheet with new column back (default: True).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)

    try:
        df[new_column] = df.eval(expression)
    except Exception:
        # Fallback for expressions eval() can't handle (e.g. string methods)
        df[new_column] = eval(expression, {"pd": pd, "np": np, "__builtins__": {}}, df)  # noqa: S307

    if write_back:
        result = _df_to_sheet(df, spreadsheet_id, worksheet)
        return json.dumps({**result, "new_column": new_column}, default=str)

    return _df_to_json(df)


@mcp.tool()
def sort_sheet(
    spreadsheet_id: str,
    worksheet: str,
    by: str,
    ascending: bool = True,
    header_row: int = 1,
    write_back: bool = True,
) -> str:
    """Sort a worksheet by one or more columns.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        by: Column(s) to sort by. Comma-separated for multiple (e.g. "Revenue,Date").
        ascending: Sort ascending (True) or descending (False). Default: True.
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes sorted data back to the sheet (default: True).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    cols = [c.strip() for c in by.split(",")]
    df = df.sort_values(by=cols, ascending=ascending).reset_index(drop=True)

    if write_back:
        result = _df_to_sheet(df, spreadsheet_id, worksheet)
        return json.dumps(result, default=str)

    return _df_to_json(df)


@mcp.tool()
def deduplicate(
    spreadsheet_id: str,
    worksheet: str,
    subset: str | None = None,
    keep: str = "first",
    header_row: int = 1,
    write_back: bool = True,
) -> str:
    """Remove duplicate rows from a worksheet.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        subset: Column(s) to check for duplicates. Comma-separated. Omit to check all columns.
        keep: Which duplicate to keep — "first", "last", or "none" (remove all). Default: "first".
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes deduplicated data back to the sheet (default: True).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    original_count = len(df)

    cols = [c.strip() for c in subset.split(",")] if subset else None
    keep_val = keep if keep != "none" else False
    df = df.drop_duplicates(subset=cols, keep=keep_val).reset_index(drop=True)

    removed = original_count - len(df)

    if write_back:
        result = _df_to_sheet(df, spreadsheet_id, worksheet)
        return json.dumps({**result, "duplicates_removed": removed}, default=str)

    return json.dumps(
        {"duplicates_removed": removed, "data": json.loads(_df_to_json(df))},
        default=str,
    )


@mcp.tool()
def fill_missing(
    spreadsheet_id: str,
    worksheet: str,
    columns: str | None = None,
    method: str = "value",
    value: str = "0",
    header_row: int = 1,
    write_back: bool = True,
) -> str:
    """Fill empty/missing cells in a worksheet.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        columns: Column(s) to fill. Comma-separated. Omit to fill all columns.
        method: Fill method — "value" (constant), "ffill" (forward fill), "bfill" (backward fill),
                "mean", "median", "mode". Default: "value".
        value: Fill value when method is "value". Default: "0".
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes filled data back to the sheet (default: True).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    df = df.replace("", np.nan)

    cols = [c.strip() for c in columns.split(",")] if columns else df.columns.tolist()

    for col in cols:
        if col not in df.columns:
            continue
        if method == "value":
            fill_val = pd.to_numeric(value, errors="ignore")
            df[col] = df[col].fillna(fill_val)
        elif method == "ffill":
            df[col] = df[col].ffill()
        elif method == "bfill":
            df[col] = df[col].bfill()
        elif method == "mean" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].mean())
        elif method == "median" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].median())
        elif method == "mode":
            mode_val = df[col].mode()
            if not mode_val.empty:
                df[col] = df[col].fillna(mode_val.iloc[0])

    if write_back:
        result = _df_to_sheet(df, spreadsheet_id, worksheet)
        return json.dumps(result, default=str)

    return _df_to_json(df)


@mcp.tool()
def correlation_matrix(
    spreadsheet_id: str,
    worksheet: str,
    columns: str | None = None,
    header_row: int = 1,
    write_to_worksheet: str | None = None,
) -> str:
    """Compute the correlation matrix for numeric columns — reveals relationships
    between variables (e.g., does spend correlate with revenue?).

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        columns: Specific columns to include. Comma-separated. Omit for all numeric columns.
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes correlation matrix to this worksheet tab.
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)

    if columns:
        cols = [c.strip() for c in columns.split(",")]
        df = df[cols]

    numeric_df = df.select_dtypes(include="number")
    corr = numeric_df.corr().round(4)

    if write_to_worksheet:
        corr_out = corr.reset_index().rename(columns={"index": ""})
        result = _df_to_sheet(corr_out, spreadsheet_id, write_to_worksheet)
        return json.dumps(result, default=str)

    return json.dumps(
        {"columns": list(corr.columns), "matrix": corr.to_dict()},
        indent=2,
        default=str,
    )


@mcp.tool()
def histogram(
    spreadsheet_id: str,
    worksheet: str,
    column: str,
    bins: int = 10,
    header_row: int = 1,
) -> str:
    """Compute a histogram (frequency distribution) for a numeric column.
    Returns bin edges and counts — useful for understanding data distribution.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        column: The numeric column to analyze.
        bins: Number of bins (default: 10).
        header_row: Row number containing headers (1-indexed, default: 1).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    series = pd.to_numeric(df[column], errors="coerce").dropna()

    counts, edges = np.histogram(series, bins=bins)
    result = {
        "column": column,
        "total_values": int(len(series)),
        "bins": [
            {
                "range": f"{round(float(edges[i]), 2)} - {round(float(edges[i+1]), 2)}",
                "count": int(counts[i]),
            }
            for i in range(len(counts))
        ],
        "stats": {
            "mean": round(float(series.mean()), 4),
            "median": round(float(series.median()), 4),
            "std": round(float(series.std()), 4),
            "skew": round(float(series.skew()), 4),
        },
    }
    return json.dumps(result, indent=2)


@mcp.tool()
def percentile_rank(
    spreadsheet_id: str,
    worksheet: str,
    column: str,
    header_row: int = 1,
    write_back: bool = True,
) -> str:
    """Add a percentile rank column (0-100) for a numeric column — useful for
    identifying top/bottom performers.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        column: The numeric column to rank.
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes the sheet with new percentile column back (default: True).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    rank_col = f"{column}_percentile"
    df[rank_col] = df[column].rank(pct=True).mul(100).round(1)

    if write_back:
        result = _df_to_sheet(df, spreadsheet_id, worksheet)
        return json.dumps({**result, "new_column": rank_col}, default=str)

    return _df_to_json(df)


@mcp.tool()
def cross_tab(
    spreadsheet_id: str,
    worksheet: str,
    row_column: str,
    col_column: str,
    value_column: str | None = None,
    aggfunc: str = "count",
    header_row: int = 1,
    write_to_worksheet: str | None = None,
) -> str:
    """Create a cross-tabulation (contingency table) between two categorical columns.
    Great for analyzing relationships like Department × Status, Region × Product, etc.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        row_column: Column for cross-tab rows.
        col_column: Column for cross-tab columns.
        value_column: Column to aggregate (optional, uses count if omitted).
        aggfunc: Aggregation function — count, sum, mean (default: count).
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes result to this worksheet tab.
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)

    if value_column and aggfunc != "count":
        ct = pd.crosstab(
            df[row_column], df[col_column],
            values=df[value_column], aggfunc=aggfunc,
        )
    else:
        ct = pd.crosstab(df[row_column], df[col_column])

    ct = ct.reset_index()

    if write_to_worksheet:
        result = _df_to_sheet(ct, spreadsheet_id, write_to_worksheet)
        return json.dumps(result, default=str)

    return _df_to_json(ct)


@mcp.tool()
def time_series_resample(
    spreadsheet_id: str,
    worksheet: str,
    date_column: str,
    value_columns: str,
    freq: str = "M",
    aggfunc: str = "sum",
    header_row: int = 1,
    write_to_worksheet: str | None = None,
) -> str:
    """Resample time series data to a different frequency — e.g., aggregate daily
    data into weekly/monthly/quarterly summaries.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        date_column: The column containing dates.
        value_columns: Column(s) to aggregate. Comma-separated for multiple.
        freq: Resample frequency — D (daily), W (weekly), M (monthly), Q (quarterly), Y (yearly). Default: M.
        aggfunc: Aggregation function — sum, mean, count, min, max. Default: sum.
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes result to this worksheet tab.
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df = df.dropna(subset=[date_column])
    df = df.set_index(date_column)

    val_cols = [c.strip() for c in value_columns.split(",")]
    resampled = df[val_cols].resample(freq).agg(aggfunc)
    resampled = resampled.reset_index()
    resampled[date_column] = resampled[date_column].dt.strftime("%Y-%m-%d")

    if write_to_worksheet:
        result = _df_to_sheet(resampled, spreadsheet_id, write_to_worksheet)
        return json.dumps({**result, "preview": json.loads(_df_to_json(resampled.head(20)))}, default=str)

    return _df_to_json(resampled)


@mcp.tool()
def rolling_window(
    spreadsheet_id: str,
    worksheet: str,
    column: str,
    window: int,
    functions: str = "mean",
    date_column: str | None = None,
    header_row: int = 1,
    write_back: bool = True,
) -> str:
    """Compute rolling window calculations (moving averages, etc.) on a column.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        column: The numeric column to compute rolling stats on.
        window: Window size (number of rows).
        functions: Rolling functions, comma-separated — mean, sum, std, min, max. Default: "mean".
        date_column: Optional date column to sort by before computing.
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes results back to the sheet (default: True).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)

    if date_column:
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df = df.sort_values(date_column)

    funcs = [f.strip() for f in functions.split(",")]
    rolling = df[column].rolling(window=window)

    for fn in funcs:
        col_name = f"{column}_rolling_{fn}_{window}"
        df[col_name] = getattr(rolling, fn)().round(4)

    if write_back:
        result = _df_to_sheet(df, spreadsheet_id, worksheet)
        return json.dumps(result, default=str)

    return _df_to_json(df)


@mcp.tool()
def outlier_detection(
    spreadsheet_id: str,
    worksheet: str,
    column: str,
    method: str = "iqr",
    threshold: float = 1.5,
    header_row: int = 1,
) -> str:
    """Detect outliers in a numeric column using IQR or Z-score method.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        column: The numeric column to check for outliers.
        method: Detection method — "iqr" (interquartile range) or "zscore". Default: "iqr".
        threshold: For IQR: multiplier (default 1.5). For Z-score: number of std devs (default 1.5, typically use 2-3).
        header_row: Row number containing headers (1-indexed, default: 1).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    series = pd.to_numeric(df[column], errors="coerce")

    if method == "zscore":
        z = (series - series.mean()) / series.std()
        mask = z.abs() > threshold
    else:  # iqr
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        mask = (series < q1 - threshold * iqr) | (series > q3 + threshold * iqr)

    outliers = df[mask].copy()
    outliers["_outlier_value"] = series[mask]

    return json.dumps(
        {
            "total_rows": int(len(df)),
            "outlier_count": int(mask.sum()),
            "method": method,
            "threshold": threshold,
            "outliers": json.loads(_df_to_json(outliers)),
        },
        indent=2,
        default=str,
    )


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
