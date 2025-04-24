import typer
from backend.fetch_table import refresh_tables

app = typer.Typer()

@app.command()
def refresh(table: str | None = None):
    """Refresh one table or all."""
    refresh_tables(tables=[table] if table else None)

if __name__ == "__main__":
    app()
