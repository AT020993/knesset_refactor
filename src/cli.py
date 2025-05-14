import typer
from typing import Optional, List
import asyncio # <--- Add this import for asyncio

# Assuming 'refresh_tables' is an ASYNC function (async def) in your backend.fetch_table.py
try:
    from backend.fetch_table import refresh_tables
except ImportError:
    # Fallback or placeholder if refresh_tables is not yet defined
    async def refresh_tables(tables: Optional[List[str]] = None): # Dummy is now async
        print(f"CRITICAL: 'refresh_tables' function not found in backend.fetch_table.py. Call with tables: {tables}")
        if tables:
            safe_tables = [str(item) for item in tables if item is not None]
            if safe_tables:
                print(f"Attempted to refresh table(s): {', '.join(safe_tables)}")
        else:
            print("Attempted to refresh all tables.")
        raise NotImplementedError("'refresh_tables' function is not implemented in backend.fetch_table.py")

try:
    from utils.logger_setup import setup_logging
    setup_logging(logger_name="knesset_cli_app")
except ImportError:
    print("Warning: utils.logger_setup.py not found or setup_logging could not be imported. Logging may not be configured.")
    def setup_logging(logger_name: str):
        print(f"Dummy setup_logging called for logger_name='{logger_name}' due to import error.")
        pass

app = typer.Typer(
    name="knesset_cli",
    help="CLI tool to manage Knesset OData tables.",
    add_completion=False
)

@app.command()
async def refresh( # <--- Make the command function ASYNC
    table: Optional[str] = typer.Option(
        None,
        "--table",
        "-t",
        help="Name of the OData table to refresh. If not provided, all relevant tables will be refreshed."
    )
):
    """
    Refresh one specified OData table or all relevant tables.
    """
    if table:
        typer.echo(f"Attempting to refresh specific table: {table}")
        tables_to_process: Optional[List[str]] = [table]
    else:
        typer.echo("Attempting to refresh all relevant tables.")
        tables_to_process: Optional[List[str]] = None

    try:
        # Now we AWAIT the call to the async refresh_tables function
        await refresh_tables(tables=tables_to_process) # <--- AWAIT the call
        typer.secho("Refresh process completed.", fg=typer.colors.GREEN) # Changed from "initiated"
    except NotImplementedError as nie:
        typer.secho(f"Error: {nie}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.secho(f"An unexpected error occurred during refresh: {e}", fg=typer.colors.RED, err=True)
        # import traceback
        # traceback.print_exc() # Uncomment for full traceback during debugging
        raise typer.Exit(code=1)

if __name__ == "__main__":
    # Typer will handle running the async command function correctly
    app()