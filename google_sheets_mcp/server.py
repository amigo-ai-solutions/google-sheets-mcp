"""Supersheets — pandas-powered Google Sheets MCP server for Claude Code.

All MCP tools live in this file. Supports both local (stdio) and hosted (SSE+OAuth) modes.
"""

from __future__ import annotations

import json
import logging
import os

import google.auth
import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as UserCredentials
from googleapiclient.discovery import build
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP("google-sheets")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ---------------------------------------------------------------------------
# Auth helpers — support both local (SA) and hosted (OAuth) modes
# ---------------------------------------------------------------------------


def _get_credentials():
    """Get Google credentials, checking for user OAuth context first."""
    # Hosted mode: use authenticated user's Google credentials
    try:
        from mcp.server.auth.middleware.auth_context import get_access_token

        from google_sheets_mcp.auth import google_token_store

        token_info = get_access_token()
        logger.info(
            "Auth context: token_info=%s, store_size=%d",
            token_info is not None,
            len(google_token_store),
        )
        if token_info:
            google_creds = google_token_store.get(token_info.token)
            if google_creds:
                logger.info(
                    "Using OAuth credentials for user=%s",
                    google_creds.get("email", "unknown"),
                )
                return UserCredentials(
                    token=google_creds.get("access_token"),
                    refresh_token=google_creds.get("refresh_token"),
                    token_uri="https://oauth2.googleapis.com/token",
                    client_id=os.environ.get("GOOGLE_OAUTH_CLIENT_ID"),
                    client_secret=os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET"),
                    scopes=SCOPES,
                )
            else:
                logger.warning(
                    "MCP token found but no Google creds linked — user did not complete OAuth"
                )
        else:
            logger.info("No MCP auth context — falling back to ADC")
    except Exception as e:
        logger.warning("Error checking auth context: %s", e)

    # Local mode: service account or ADC
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path:
        logger.info("Using service account credentials from %s", creds_path)
        return service_account.Credentials.from_service_account_file(
            creds_path, scopes=SCOPES
        )
    logger.info("Using Application Default Credentials (Cloud Run SA)")
    creds, _ = google.auth.default(scopes=SCOPES)
    return creds


def _get_client() -> gspread.Client:
    return gspread.authorize(_get_credentials())


def _get_sheets_service():
    return build("sheets", "v4", credentials=_get_credentials())


def _get_drive_service():
    return build("drive", "v3", credentials=_get_credentials())


# ---------------------------------------------------------------------------
# DataFrame helpers (for analytics tools)
# ---------------------------------------------------------------------------


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
    data = ws.get(range) if range else ws.get_all_values()

    if not data:
        return pd.DataFrame()

    if header_row >= 1 and len(data) >= header_row:
        headers = data[header_row - 1]
        rows = data[header_row:]
        df = pd.DataFrame(rows, columns=headers)
    else:
        df = pd.DataFrame(data)

    for col in df.columns:
        converted = pd.to_numeric(df[col], errors="coerce")
        if not converted.isna().all():
            df[col] = converted
    return df


def _df_to_json(df: pd.DataFrame, max_rows: int = 500) -> str:
    """Serialize a DataFrame to JSON, truncating if needed."""
    truncated = len(df) > max_rows
    out = df.head(max_rows)
    # Replace NaN/inf with None for JSON compatibility
    out = out.where(out.notna(), None)
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

    header = [list(df.columns)]
    values = df.fillna("").astype(str).values.tolist()
    ws.update("A1", header + values)
    return {
        "status": "ok",
        "worksheet": worksheet,
        "rows": len(df),
        "cols": len(df.columns),
    }


def _get_sheet_id(spreadsheet_id: str, sheet_name: str) -> int:
    """Get the numeric sheetId for a named worksheet."""
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(sheet_name)
    return ws.id


# ===================================================================
# CORE CRUD TOOLS
# ===================================================================


@mcp.tool()
def list_spreadsheets(folder_id: str | None = None) -> str:
    """List all spreadsheets accessible to the authenticated user.
    If folder_id is provided, lists spreadsheets in that Google Drive folder.

    Args:
        folder_id: Optional Google Drive folder ID to list spreadsheets from.
    """
    if folder_id:
        service = _get_drive_service()
        q = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        resp = service.files().list(q=q, fields="files(id,name,webViewLink)").execute()
        results = [
            {"title": f["name"], "id": f["id"], "url": f.get("webViewLink", "")}
            for f in resp.get("files", [])
        ]
    else:
        gc = _get_client()
        sheets = gc.openall()
        results = [{"title": s.title, "id": s.id, "url": s.url} for s in sheets]
    return json.dumps(results, indent=2)


@mcp.tool()
def get_spreadsheet_info(spreadsheet_id: str) -> str:
    """Get metadata about a spreadsheet including all worksheet names, row/col counts.

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
    data = ws.get(range) if range else ws.get_all_values()
    return json.dumps(data, indent=2)


@mcp.tool()
def get_sheet_formulas(
    spreadsheet_id: str,
    worksheet: str = "Sheet1",
    range: str | None = None,
) -> str:
    """Read formulas (not computed values) from a worksheet.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name (default: Sheet1).
        range: Optional A1 notation range. Reads all if omitted.
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)
    if range:
        data = ws.get(range, value_render_option="FORMULA")
    else:
        data = ws.get_all_values(value_render_option="FORMULA")
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
def batch_update_cells(
    spreadsheet_id: str,
    worksheet: str,
    ranges: dict[str, list[list]],
) -> str:
    """Update multiple cell ranges in a single call.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        ranges: Map of A1 range → 2D array of values.
                Example: {"A1:B2": [[1,2],[3,4]], "D1:E2": [["a","b"],["c","d"]]}
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)
    batch = [{"range": f"'{worksheet}'!{r}", "values": v} for r, v in ranges.items()]
    sh.values_batch_update(
        body={"value_input_option": "USER_ENTERED", "data": batch}
    )
    return json.dumps(
        {"status": "ok", "ranges_updated": len(ranges)}
    )


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
def search_cells(
    spreadsheet_id: str,
    query: str,
    worksheet: str | None = None,
    case_sensitive: bool = False,
    max_results: int = 50,
) -> str:
    """Search for cells containing a value. Searches all sheets if worksheet is omitted.

    Args:
        spreadsheet_id: The spreadsheet ID.
        query: Text to search for.
        worksheet: Worksheet name to search in. Omit to search all sheets.
        case_sensitive: Case-sensitive search (default: False).
        max_results: Maximum results to return (default: 50).
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    results = []

    sheets = [sh.worksheet(worksheet)] if worksheet else sh.worksheets()
    for ws in sheets:
        if case_sensitive:
            cells = ws.findall(query)
        else:
            import re

            cells = ws.findall(re.compile(re.escape(query), re.IGNORECASE))
        for c in cells:
            results.append(
                {
                    "sheet": ws.title,
                    "row": c.row,
                    "col": c.col,
                    "value": c.value,
                    "address": f"{ws.title}!{c.label}",
                }
            )
            if len(results) >= max_results:
                break
        if len(results) >= max_results:
            break

    return json.dumps(results, indent=2)


# ===================================================================
# STRUCTURE TOOLS
# ===================================================================


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
def add_rows(
    spreadsheet_id: str,
    worksheet: str,
    count: int,
    start_row: int | None = None,
) -> str:
    """Insert rows into a worksheet.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        count: Number of rows to insert.
        start_row: 1-based row index to insert before. Appends at end if omitted.
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)
    if start_row is not None:
        ws.insert_rows([[""] * ws.col_count] * count, row=start_row)
    else:
        ws.add_rows(count)
    return json.dumps({"status": "ok", "rows_added": count})


@mcp.tool()
def add_columns(
    spreadsheet_id: str,
    worksheet: str,
    count: int,
    start_column: int | None = None,
) -> str:
    """Insert columns into a worksheet.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        count: Number of columns to insert.
        start_column: 1-based column index to insert before. Appends at end if omitted.
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)
    if start_column is not None:
        ws.insert_cols([[""] * ws.row_count] * count, col=start_column)
    else:
        ws.add_cols(count)
    return json.dumps({"status": "ok", "columns_added": count})


@mcp.tool()
def copy_sheet(
    src_spreadsheet_id: str,
    src_worksheet: str,
    dst_spreadsheet_id: str,
    dst_worksheet: str | None = None,
) -> str:
    """Copy a worksheet from one spreadsheet to another.

    Args:
        src_spreadsheet_id: Source spreadsheet ID.
        src_worksheet: Source worksheet name.
        dst_spreadsheet_id: Destination spreadsheet ID.
        dst_worksheet: Name for the copy in destination. Uses original name if omitted.
    """
    gc = _get_client()
    src_sh = gc.open_by_key(src_spreadsheet_id)
    src_ws = src_sh.worksheet(src_worksheet)
    src_ws.copy_to(dst_spreadsheet_id)

    # Rename if needed
    if dst_worksheet:
        dst_sh = gc.open_by_key(dst_spreadsheet_id)
        # The copy gets a name like "Copy of OriginalName"
        for ws in dst_sh.worksheets():
            if ws.title.startswith("Copy of "):
                ws.update_title(dst_worksheet)
                break

    return json.dumps({"status": "ok", "copied_to": dst_spreadsheet_id})


@mcp.tool()
def rename_sheet(
    spreadsheet_id: str,
    worksheet: str,
    new_name: str,
) -> str:
    """Rename a worksheet tab.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Current worksheet name.
        new_name: New name for the worksheet.
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)
    ws.update_title(new_name)
    return json.dumps({"status": "ok", "old_name": worksheet, "new_name": new_name})


# ===================================================================
# MULTI-READ TOOLS
# ===================================================================


@mcp.tool()
def get_multiple_sheet_data(
    queries: list[dict],
) -> str:
    """Read data from multiple ranges across spreadsheets in one call.

    Args:
        queries: List of dicts, each with keys: spreadsheet_id, sheet, range.
                 Example: [{"spreadsheet_id": "abc", "sheet": "Sheet1", "range": "A1:D10"}]
    """
    gc = _get_client()
    results = []
    for q in queries:
        sh = gc.open_by_key(q["spreadsheet_id"])
        ws = sh.worksheet(q["sheet"])
        data = ws.get(q.get("range")) if q.get("range") else ws.get_all_values()
        results.append(
            {
                "spreadsheet_id": q["spreadsheet_id"],
                "sheet": q["sheet"],
                "range": q.get("range", "ALL"),
                "data": data,
            }
        )
    return json.dumps(results, indent=2)


@mcp.tool()
def get_multiple_spreadsheet_summary(
    spreadsheet_ids: list[str],
    rows_to_fetch: int = 5,
) -> str:
    """Get a summary of multiple spreadsheets — title, sheets, headers, preview rows.

    Args:
        spreadsheet_ids: List of spreadsheet IDs to summarize.
        rows_to_fetch: Number of rows to preview per sheet (default: 5).
    """
    gc = _get_client()
    results = []
    for sid in spreadsheet_ids:
        sh = gc.open_by_key(sid)
        sheets_info = []
        for ws in sh.worksheets():
            data = ws.get_all_values()
            preview = data[:rows_to_fetch] if data else []
            sheets_info.append(
                {
                    "title": ws.title,
                    "rows": ws.row_count,
                    "cols": ws.col_count,
                    "headers": data[0] if data else [],
                    "preview_rows": preview[1:] if len(preview) > 1 else [],
                }
            )
        results.append(
            {"spreadsheet_id": sid, "title": sh.title, "sheets": sheets_info}
        )
    return json.dumps(results, indent=2)


# ===================================================================
# DRIVE TOOLS
# ===================================================================


@mcp.tool()
def create_spreadsheet(
    title: str,
    folder_id: str | None = None,
) -> str:
    """Create a new Google Spreadsheet.

    Args:
        title: Title for the new spreadsheet.
        folder_id: Optional Google Drive folder ID to create in.
    """
    gc = _get_client()
    sh = gc.create(title, folder_id=folder_id)
    return json.dumps({"status": "ok", "id": sh.id, "title": sh.title, "url": sh.url})


@mcp.tool()
def share_spreadsheet(
    spreadsheet_id: str,
    recipients: list[dict],
    send_notification: bool = True,
) -> str:
    """Share a spreadsheet with users via email.

    Args:
        spreadsheet_id: The spreadsheet ID.
        recipients: List of dicts with "email_address" and "role" keys.
                    Role: "reader", "commenter", or "writer".
        send_notification: Send email notification (default: True).
    """
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    for r in recipients:
        sh.share(
            r["email_address"],
            perm_type="user",
            role=r["role"],
            notify=send_notification,
        )
    return json.dumps(
        {"status": "ok", "shared_with": len(recipients)}
    )


@mcp.tool()
def search_spreadsheets(
    query: str,
    max_results: int = 20,
) -> str:
    """Search Google Drive for spreadsheets by name.

    Args:
        query: Search string to match against spreadsheet names.
        max_results: Maximum results (default: 20, max: 100).
    """
    service = _get_drive_service()
    q = f"mimeType='application/vnd.google-apps.spreadsheet' and name contains '{query}' and trashed=false"
    resp = (
        service.files()
        .list(q=q, pageSize=min(max_results, 100), fields="files(id,name,webViewLink,modifiedTime)")
        .execute()
    )
    results = [
        {
            "id": f["id"],
            "title": f["name"],
            "url": f.get("webViewLink", ""),
            "modified": f.get("modifiedTime", ""),
        }
        for f in resp.get("files", [])
    ]
    return json.dumps(results, indent=2)


@mcp.tool()
def list_folders(
    parent_folder_id: str | None = None,
) -> str:
    """List folders in Google Drive.

    Args:
        parent_folder_id: Parent folder ID. Lists root folders if omitted.
    """
    service = _get_drive_service()
    if parent_folder_id:
        q = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    else:
        q = "mimeType='application/vnd.google-apps.folder' and 'root' in parents and trashed=false"
    resp = (
        service.files()
        .list(q=q, fields="files(id,name,webViewLink)")
        .execute()
    )
    results = [
        {"id": f["id"], "name": f["name"], "url": f.get("webViewLink", "")}
        for f in resp.get("files", [])
    ]
    return json.dumps(results, indent=2)


# ===================================================================
# RAW API TOOLS
# ===================================================================


@mcp.tool()
def batch_update(
    spreadsheet_id: str,
    requests: list[dict],
) -> str:
    """Execute raw Sheets API batchUpdate requests — full power of the Sheets API.
    Supports addSheet, updateSheetProperties, insertDimension, deleteDimension,
    updateCells, updateBorders, addConditionalFormatRule, mergeCells, etc.

    Args:
        spreadsheet_id: The spreadsheet ID.
        requests: List of Sheets API batchUpdate request objects.
    """
    service = _get_sheets_service()
    resp = (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
        .execute()
    )
    return json.dumps(
        {
            "status": "ok",
            "replies": resp.get("replies", []),
            "spreadsheet_id": resp.get("spreadsheetId"),
        },
        indent=2,
        default=str,
    )


@mcp.tool()
def add_chart(
    spreadsheet_id: str,
    worksheet: str,
    chart_type: str,
    data_range: str,
    title: str | None = None,
    x_axis_label: str | None = None,
    y_axis_label: str | None = None,
    width: int = 600,
    height: int = 400,
) -> str:
    """Add a chart to a spreadsheet.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet containing the data.
        chart_type: Chart type — COLUMN, BAR, LINE, AREA, PIE, SCATTER, COMBO, HISTOGRAM.
        data_range: A1 notation range for chart data (e.g. "A1:C10").
        title: Optional chart title.
        x_axis_label: Optional X axis label.
        y_axis_label: Optional Y axis label.
        width: Chart width in pixels (default: 600).
        height: Chart height in pixels (default: 400).
    """
    sheet_id = _get_sheet_id(spreadsheet_id, worksheet)

    # Parse A1 range to grid coordinates
    gc = _get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet)
    range_obj = ws.range(data_range)
    start_row = range_obj[0].row - 1
    end_row = range_obj[-1].row
    start_col = range_obj[0].col - 1
    end_col = range_obj[-1].col

    chart_spec = {
        "title": title or "",
        "basicChart": {
            "chartType": chart_type.upper(),
            "legendPosition": "BOTTOM_LEGEND",
            "domains": [
                {
                    "domain": {
                        "sourceRange": {
                            "sources": [
                                {
                                    "sheetId": sheet_id,
                                    "startRowIndex": start_row,
                                    "endRowIndex": end_row,
                                    "startColumnIndex": start_col,
                                    "endColumnIndex": start_col + 1,
                                }
                            ]
                        }
                    }
                }
            ],
            "series": [
                {
                    "series": {
                        "sourceRange": {
                            "sources": [
                                {
                                    "sheetId": sheet_id,
                                    "startRowIndex": start_row,
                                    "endRowIndex": end_row,
                                    "startColumnIndex": col_idx,
                                    "endColumnIndex": col_idx + 1,
                                }
                            ]
                        }
                    },
                    "targetAxis": "LEFT_AXIS",
                }
                for col_idx in range(start_col + 1, end_col)
            ],
            "headerCount": 1,
        },
    }

    if x_axis_label:
        chart_spec["basicChart"]["axis"] = [
            {"position": "BOTTOM_AXIS", "title": x_axis_label}
        ]
    if y_axis_label:
        axes = chart_spec["basicChart"].get("axis", [])
        axes.append({"position": "LEFT_AXIS", "title": y_axis_label})
        chart_spec["basicChart"]["axis"] = axes

    request = {
        "addChart": {
            "chart": {
                "spec": chart_spec,
                "position": {
                    "overlayPosition": {
                        "anchorCell": {
                            "sheetId": sheet_id,
                            "rowIndex": 0,
                            "columnIndex": end_col + 1,
                        },
                        "widthPixels": width,
                        "heightPixels": height,
                    }
                },
            }
        }
    }

    service = _get_sheets_service()
    resp = (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [request]})
        .execute()
    )
    return json.dumps({"status": "ok", "chart_type": chart_type}, default=str)


# ===================================================================
# ANALYTICS TOOLS (pandas + numpy)
# ===================================================================


@mcp.tool()
def describe_sheet(
    spreadsheet_id: str,
    worksheet: str = "Sheet1",
    header_row: int = 1,
) -> str:
    """Statistical summary of a worksheet — row count, column types, descriptive stats
    for numeric columns, value counts for categorical columns.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        header_row: Row number containing headers (1-indexed, default: 1).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    info: dict = {
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
                k: round(float(desc[k]), 4) if k in desc else None
                for k in ["mean", "std", "min", "25%", "50%", "75%", "max"]
            }
            col_info["stats"]["sum"] = round(float(df[col].sum()), 4)
        else:
            nunique = df[col].nunique()
            col_info["unique_values"] = int(nunique)
            if 0 < nunique <= 20:
                col_info["value_counts"] = df[col].value_counts().head(20).to_dict()
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
    """Filter rows using a pandas query expression. Supports comparisons, boolean logic,
    string methods, and arithmetic.

    Examples: "Revenue > 10000", "Status == 'Active' and Region == 'MENA'",
              "Age.between(25, 40)", "Name.str.contains('Ali')"

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        query: A pandas DataFrame.query() expression.
        header_row: Row number containing headers (1-indexed, default: 1).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    clean_cols = {c: c.strip().replace(" ", "_") for c in df.columns}
    df = df.rename(columns=clean_cols)
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
    """Create a pivot table from sheet data.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Source worksheet name.
        index: Column(s) for rows. Comma-separated for multiple.
        values: Column(s) to aggregate. Comma-separated for multiple.
        columns: Optional column to pivot into columns.
        aggfunc: Aggregation — sum, mean, count, min, max, median (default: sum).
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes result to this worksheet tab.
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    idx = [c.strip() for c in index.split(",")]
    vals = [c.strip() for c in values.split(",")]
    cols = [c.strip() for c in columns.split(",")] if columns else None

    pivot = pd.pivot_table(df, index=idx, values=vals, columns=cols, aggfunc=aggfunc)
    pivot = pivot.reset_index()
    if isinstance(pivot.columns, pd.MultiIndex):
        pivot.columns = [
            "_".join(str(c) for c in col).strip("_") for col in pivot.columns
        ]

    if write_to_worksheet:
        result = _df_to_sheet(pivot, spreadsheet_id, write_to_worksheet)
        return json.dumps(
            {**result, "preview": json.loads(_df_to_json(pivot.head(20)))},
            default=str,
        )
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
    """Group rows and aggregate, like SQL GROUP BY.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        by: Column(s) to group by. Comma-separated.
        agg: Aggregation spec as "column:func" pairs, comma-separated.
             Functions: sum, mean, count, min, max, median, nunique, first, last.
             Example: "Revenue:sum,Cost:mean,Name:count"
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes result to this worksheet tab.
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    group_cols = [c.strip() for c in by.split(",")]
    agg_dict: dict[str, list[str]] = {}
    for pair in agg.split(","):
        col, func = pair.strip().rsplit(":", 1)
        agg_dict.setdefault(col.strip(), []).append(func.strip())

    result = df.groupby(group_cols).agg(agg_dict)
    result.columns = ["_".join(col).strip("_") for col in result.columns]
    result = result.reset_index()

    if write_to_worksheet:
        wr = _df_to_sheet(result, spreadsheet_id, write_to_worksheet)
        return json.dumps(
            {**wr, "preview": json.loads(_df_to_json(result.head(20)))}, default=str
        )
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
    """Join two worksheets like VLOOKUP. Supports left/right/inner/outer joins,
    even across different spreadsheets.

    Args:
        spreadsheet_id: Spreadsheet ID for the left sheet.
        left_worksheet: Left worksheet name.
        right_worksheet: Right worksheet name.
        on: Join column(s). Comma-separated for composite keys.
        right_spreadsheet_id: Spreadsheet ID for right sheet if different.
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
        wr = _df_to_sheet(merged, spreadsheet_id, write_to_worksheet)
        return json.dumps(
            {**wr, "preview": json.loads(_df_to_json(merged.head(20)))}, default=str
        )
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
    """Add a calculated column using a pandas expression applied to every row.

    Examples: "Revenue - Cost", "Quantity * Unit_Price", "Score / Score.max() * 100",
              "Name.str.upper()", "pd.to_datetime(Date).dt.month"

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        new_column: Name for the new column.
        expression: Pandas expression using column names.
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes the full sheet with new column back (default: True).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    try:
        df[new_column] = df.eval(expression)
    except Exception:
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
        by: Column(s) to sort by. Comma-separated for multiple.
        ascending: Sort ascending (True) or descending (False).
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes sorted data back (default: True).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    cols = [c.strip() for c in by.split(",")]
    df = df.sort_values(by=cols, ascending=ascending).reset_index(drop=True)

    if write_back:
        return json.dumps(_df_to_sheet(df, spreadsheet_id, worksheet), default=str)
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
        subset: Column(s) to check for duplicates. Comma-separated. All columns if omitted.
        keep: Which duplicate to keep — "first", "last", or "none". Default: "first".
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes deduplicated data back (default: True).
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
        columns: Column(s) to fill. Comma-separated. All columns if omitted.
        method: Fill method — "value", "ffill", "bfill", "mean", "median", "mode".
        value: Fill value when method is "value" (default: "0").
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes filled data back (default: True).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    df = df.replace("", np.nan)
    cols = [c.strip() for c in columns.split(",")] if columns else df.columns.tolist()

    for col in cols:
        if col not in df.columns:
            continue
        if method == "value":
            try:
                fill_val = float(value)
            except (ValueError, TypeError):
                fill_val = value
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
        return json.dumps(_df_to_sheet(df, spreadsheet_id, worksheet), default=str)
    return _df_to_json(df)


@mcp.tool()
def correlation_matrix(
    spreadsheet_id: str,
    worksheet: str,
    columns: str | None = None,
    header_row: int = 1,
    write_to_worksheet: str | None = None,
) -> str:
    """Compute correlation matrix for numeric columns.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        columns: Specific columns. Comma-separated. All numeric if omitted.
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes matrix to this worksheet tab.
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    if columns:
        df = df[[c.strip() for c in columns.split(",")]]
    numeric_df = df.select_dtypes(include="number")
    corr = numeric_df.corr().round(4)

    if write_to_worksheet:
        corr_out = corr.reset_index().rename(columns={"index": ""})
        return json.dumps(
            _df_to_sheet(corr_out, spreadsheet_id, write_to_worksheet), default=str
        )
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
    return json.dumps(
        {
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
        },
        indent=2,
    )


@mcp.tool()
def percentile_rank(
    spreadsheet_id: str,
    worksheet: str,
    column: str,
    header_row: int = 1,
    write_back: bool = True,
) -> str:
    """Add a percentile rank column (0-100) for a numeric column.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        column: The numeric column to rank.
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes with new column back (default: True).
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
    """Cross-tabulation between two categorical columns.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        row_column: Column for cross-tab rows.
        col_column: Column for cross-tab columns.
        value_column: Column to aggregate (optional, uses count if omitted).
        aggfunc: Aggregation — count, sum, mean (default: count).
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes result to this worksheet tab.
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    if value_column and aggfunc != "count":
        ct = pd.crosstab(
            df[row_column],
            df[col_column],
            values=df[value_column],
            aggfunc=aggfunc,
        )
    else:
        ct = pd.crosstab(df[row_column], df[col_column])
    ct = ct.reset_index()

    if write_to_worksheet:
        return json.dumps(
            _df_to_sheet(ct, spreadsheet_id, write_to_worksheet), default=str
        )
    return _df_to_json(ct)


@mcp.tool()
def time_series_resample(
    spreadsheet_id: str,
    worksheet: str,
    date_column: str,
    value_columns: str,
    freq: str = "ME",
    aggfunc: str = "sum",
    header_row: int = 1,
    write_to_worksheet: str | None = None,
) -> str:
    """Resample time series data — e.g., daily → monthly/quarterly.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        date_column: Column containing dates.
        value_columns: Column(s) to aggregate. Comma-separated.
        freq: Frequency — D (daily), W (weekly), ME (monthly), QE (quarterly), YE (yearly).
        aggfunc: Aggregation — sum, mean, count, min, max (default: sum).
        header_row: Row number containing headers (1-indexed, default: 1).
        write_to_worksheet: If provided, writes result to this worksheet tab.
    """
    # Pandas 3.0: translate legacy freq aliases
    freq_map = {"M": "ME", "Q": "QE", "Y": "YE", "BM": "BME", "BQ": "BQE", "BY": "BYE"}
    freq = freq_map.get(freq, freq)

    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df = df.dropna(subset=[date_column]).set_index(date_column)

    val_cols = [c.strip() for c in value_columns.split(",")]
    resampled = df[val_cols].resample(freq).agg(aggfunc).reset_index()
    resampled[date_column] = resampled[date_column].dt.strftime("%Y-%m-%d")

    if write_to_worksheet:
        wr = _df_to_sheet(resampled, spreadsheet_id, write_to_worksheet)
        return json.dumps(
            {**wr, "preview": json.loads(_df_to_json(resampled.head(20)))},
            default=str,
        )
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
    """Compute rolling window calculations (moving averages, etc.).

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        column: Numeric column to compute rolling stats on.
        window: Window size (number of rows).
        functions: Rolling functions, comma-separated — mean, sum, std, min, max.
        date_column: Optional date column to sort by first.
        header_row: Row number containing headers (1-indexed, default: 1).
        write_back: If True, writes results back (default: True).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    if date_column:
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df = df.sort_values(date_column)

    rolling = df[column].rolling(window=window)
    for fn in [f.strip() for f in functions.split(",")]:
        df[f"{column}_rolling_{fn}_{window}"] = getattr(rolling, fn)().round(4)

    if write_back:
        return json.dumps(_df_to_sheet(df, spreadsheet_id, worksheet), default=str)
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
    """Detect outliers in a numeric column using IQR or Z-score.

    Args:
        spreadsheet_id: The spreadsheet ID.
        worksheet: Worksheet name.
        column: Numeric column to check.
        method: "iqr" or "zscore" (default: "iqr").
        threshold: IQR multiplier or Z-score std devs (default: 1.5).
        header_row: Row number containing headers (1-indexed, default: 1).
    """
    df = _sheet_to_df(spreadsheet_id, worksheet, header_row=header_row)
    series = pd.to_numeric(df[column], errors="coerce")

    if method == "zscore":
        z = (series - series.mean()) / series.std()
        mask = z.abs() > threshold
    else:
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
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


# ---------------------------------------------------------------------------
# Entry point (stdio mode for local use)
# ---------------------------------------------------------------------------


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
