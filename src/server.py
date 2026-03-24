import os
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
import psycopg
from psycopg.rows import dict_row


mcp = FastMCP("postgres-tools-mcp")


def _database_uri() -> str:
    uri = os.environ.get("DATABASE_URI", "").strip()
    if not uri:
        raise ValueError("DATABASE_URI is not set")
    return uri


def _connect() -> psycopg.Connection:
    return psycopg.connect(_database_uri(), row_factory=dict_row)


def _is_select_like(sql: str) -> bool:
    first = sql.lstrip().lower()
    return first.startswith("select") or first.startswith("with")


@mcp.tool(description="List all schemas in the database")
def list_schemas() -> list[dict[str, Any]]:
    query = """
    SELECT
        schema_name,
        schema_owner,
        CASE
            WHEN schema_name LIKE 'pg_%' THEN 'System Schema'
            WHEN schema_name = 'information_schema' THEN 'System Information Schema'
            ELSE 'User Schema'
        END AS schema_type
    FROM information_schema.schemata
    ORDER BY schema_type, schema_name
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(query)
        return list(cur.fetchall())


@mcp.tool(description="List objects in a schema")
def list_objects(schema_name: str, object_type: str = "table") -> list[dict[str, Any]]:
    with _connect() as conn, conn.cursor() as cur:
        if object_type in ("table", "view"):
            table_type = "BASE TABLE" if object_type == "table" else "VIEW"
            cur.execute(
                """
                SELECT table_schema AS schema, table_name AS name, table_type AS type
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = %s
                ORDER BY table_name
                """,
                (schema_name, table_type),
            )
            return list(cur.fetchall())
        if object_type == "sequence":
            cur.execute(
                """
                SELECT sequence_schema AS schema, sequence_name AS name, data_type
                FROM information_schema.sequences
                WHERE sequence_schema = %s
                ORDER BY sequence_name
                """,
                (schema_name,),
            )
            return list(cur.fetchall())
        if object_type == "extension":
            cur.execute(
                """
                SELECT extname AS name, extversion AS version, extrelocatable AS relocatable
                FROM pg_extension
                ORDER BY extname
                """
            )
            return list(cur.fetchall())
    raise ValueError(f"Unsupported object_type: {object_type}")


@mcp.tool(description="Execute SQL query and return rows (defaults to limit 1000 unless overridden)")
def execute_sql(sql: str, row_limit: int = 1000) -> list[dict[str, Any]] | str:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql)
        if cur.description is None:
            conn.commit()
            return "No tabular result"
        rows = cur.fetchmany(row_limit)
        return list(rows)


@mcp.tool(description="Stream SELECT query results into CSV file via PostgreSQL COPY")
def export_sql_to_csv(
    sql: str,
    output_path: str,
    header: bool = True,
    delimiter: str = ",",
    overwrite: bool = False,
) -> dict[str, Any]:
    if not _is_select_like(sql):
        raise ValueError("Only SELECT/CTE queries are allowed for CSV export")

    target = Path(output_path).expanduser()
    if target.exists() and not overwrite:
        raise ValueError(f"File already exists: {target}")
    if not target.parent.exists():
        raise ValueError(f"Parent directory does not exist: {target.parent}")
    if len(delimiter) != 1:
        raise ValueError("Delimiter must be a single character")
    if delimiter == "'":
        raise ValueError("Single quote delimiter is not supported")

    # Build a safe COPY command for delimiter while keeping query unchanged.
    # We open one connection for export and stream chunks directly to disk.
    with _connect() as conn:
        with conn.cursor() as cur:
            with target.open("wb") as out:
                with cur.copy(
                    f"COPY ({sql}) TO STDOUT WITH (FORMAT CSV, DELIMITER '{delimiter}', HEADER {'TRUE' if header else 'FALSE'})"
                ) as copy:
                    while True:
                        chunk = copy.read()
                        if not chunk:
                            break
                        if isinstance(chunk, memoryview):
                            out.write(chunk.tobytes())
                        elif isinstance(chunk, str):
                            out.write(chunk.encode("utf-8"))
                        else:
                            out.write(chunk)

    return {"status": "ok", "path": str(target), "size_bytes": target.stat().st_size}


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
