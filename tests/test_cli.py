import pytest
from typer.testing import CliRunner
from unittest import mock

from src.cli import app

runner = CliRunner()


@mock.patch('src.cli.refresh_tables', new_callable=mock.AsyncMock)
def test_refresh_specific_table(mock_refresh_tables):
    """Test refresh command with a specific table."""
    table_name = "MyTable"
    result = runner.invoke(app, ["refresh", "--table", table_name])

    mock_refresh_tables.assert_called_once_with(tables=[table_name])
    assert f"Attempting to refresh specific table: {table_name}" in result.stdout
    assert "Refresh process completed." in result.stdout
    assert result.exit_code == 0


@mock.patch('src.cli.refresh_tables', new_callable=mock.AsyncMock)
def test_refresh_all_tables(mock_refresh_tables):
    """Test refresh command for all tables."""
    result = runner.invoke(app, ["refresh"])

    mock_refresh_tables.assert_called_once_with(tables=None)
    assert "Attempting to refresh all relevant tables." in result.stdout
    assert "Refresh process completed." in result.stdout
    assert result.exit_code == 0


@mock.patch('src.cli.refresh_tables', new_callable=mock.AsyncMock)
def test_refresh_not_implemented_error(mock_refresh_tables):
    """Test refresh command when refresh_tables raises NotImplementedError."""
    error_message = "Feature not implemented"
    mock_refresh_tables.side_effect = NotImplementedError(error_message)
    result = runner.invoke(app, ["refresh"])

    assert error_message in result.stdout
    assert result.exit_code == 1


@mock.patch('src.cli.refresh_tables', new_callable=mock.AsyncMock)
def test_refresh_generic_exception(mock_refresh_tables):
    """Test refresh command when refresh_tables raises a generic Exception."""
    error_message = "Something went wrong"
    mock_refresh_tables.side_effect = Exception(error_message)
    result = runner.invoke(app, ["refresh"])

    assert "An unexpected error occurred during refresh:" in result.stdout
    assert error_message in result.stdout
    assert result.exit_code == 1
