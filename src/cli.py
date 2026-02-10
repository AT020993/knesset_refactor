import typer
from typing import Optional, List
from pathlib import Path

from core.dependencies import DependencyContainer

app = typer.Typer(
    name="knesset_cli",
    help="CLI tool to manage Knesset OData tables.",
    add_completion=False
)

# Global dependency container
container = DependencyContainer()

@app.command()
def refresh(
    table: Optional[str] = typer.Option(
        None,
        "--table",
        "-t",
        help="Name of the OData table to refresh. If not provided, all relevant tables will be refreshed."
    ),
    db_path: Optional[Path] = typer.Option(
        None,
        "--db",
        help="Path to the DuckDB database file."
    )
):
    """
    Refresh one specified OData table or all relevant tables.
    """
    # Initialize container with custom db path if provided
    if db_path:
        refresh_container = DependencyContainer(db_path=db_path)
    else:
        refresh_container = container
    
    if table:
        typer.echo(f"Attempting to refresh specific table: {table}")
        tables_to_process: Optional[List[str]] = [table]
    else:
        typer.echo("Attempting to refresh all relevant tables.")
        tables_to_process = None

    try:
        # Use the data refresh service from dependency container
        service = refresh_container.data_refresh_service
        # Use sync method - it handles async internally
        success = service.refresh_tables_sync(tables_to_process)

        if success:
            typer.secho("Refresh process completed successfully.", fg=typer.colors.GREEN)
        else:
            typer.secho("Refresh process completed with errors.", fg=typer.colors.YELLOW)
            raise typer.Exit(code=1)

    except Exception as e:
        typer.secho(f"An unexpected error occurred during refresh: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

@app.command()
def refresh_factions(
    db_path: Optional[Path] = typer.Option(
        None,
        "--db",
        help="Path to the DuckDB database file."
    )
):
    """
    Refresh only the faction coalition status from CSV.
    """
    # Initialize container with custom db path if provided
    if db_path:
        refresh_container = DependencyContainer(db_path=db_path)
    else:
        refresh_container = container
    
    try:
        service = refresh_container.data_refresh_service
        success = service.refresh_faction_status_only()
        
        if success:
            typer.secho("Faction status refresh completed successfully.", fg=typer.colors.GREEN)
        else:
            typer.secho("Faction status refresh failed.", fg=typer.colors.RED)
            raise typer.Exit(code=1)
            
    except Exception as e:
        typer.secho(f"Error refreshing faction status: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

if __name__ == "__main__":
    # Typer will handle running the async command function correctly
    app()
