# postgres-tools-mcp

Lightweight PostgreSQL MCP server with practical tools for analytics and bulk export.

## Tools

- `list_schemas`
- `list_objects`
- `execute_sql`
- `export_sql_to_csv`

## Why CSV export tool

For large result sets, returning rows through MCP can produce huge JSON payloads.
`export_sql_to_csv` streams query results to a CSV file via PostgreSQL `COPY`.

## Run

```bash
uv --directory /path/to/postgres-tools-mcp run python -m src.server
```

## Cursor mcp.json example

```json
{
  "mcpServers": {
    "postgres-wb-rent": {
      "command": "uv",
      "args": [
        "--directory",
        "/Users/gleb/base/work/llm_writing/mcp_cli/postgres-tools-mcp",
        "run",
        "python",
        "-m",
        "src.server"
      ],
      "env": {
        "DATABASE_URI": "postgresql://<user>:<password>@<host>:<port>/<db>"
      }
    }
  }
}
```

## export_sql_to_csv arguments

- `sql`: only `SELECT` or `WITH` queries.
- `output_path`: target CSV file path.
- `header`: include header row (default `true`).
- `delimiter`: single character (default `,`).
- `overwrite`: overwrite existing file (default `false`).

## uv.lock

`uv.lock` is intentionally committed for reproducible dependency resolution.
