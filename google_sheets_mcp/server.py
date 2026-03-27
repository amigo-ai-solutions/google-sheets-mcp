"""Google Sheets MCP server for Claude Code."""

import json
import os

import google.auth
import gspread
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
    """Read data from a Google Sheets worksheet.

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


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
