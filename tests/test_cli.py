import pytest
from typer.testing import CliRunner
from unittest import mock

from src.cli import app

runner = CliRunner()


def test_refresh_specific_table():
    """Test refresh command with a specific table."""
    # Mock the service - now uses sync method
    mock_service = mock.Mock()
    mock_service.refresh_tables_sync = mock.Mock(return_value=True)

    # Create a proper mock container that returns our mock service
    mock_container = mock.Mock()
    mock_container.data_refresh_service = mock_service

    with mock.patch('src.cli.container', mock_container):
        table_name = "MyTable"
        result = runner.invoke(app, ["refresh", "--table", table_name])

        mock_service.refresh_tables_sync.assert_called_once_with([table_name])
        assert f"Attempting to refresh specific table: {table_name}" in result.stdout
        assert "Refresh process completed successfully." in result.stdout
        assert result.exit_code == 0


def test_refresh_all_tables():
    """Test refresh command for all tables."""
    # Mock the service - now uses sync method
    mock_service = mock.Mock()
    mock_service.refresh_tables_sync = mock.Mock(return_value=True)

    # Create a proper mock container that returns our mock service
    mock_container = mock.Mock()
    mock_container.data_refresh_service = mock_service

    with mock.patch('src.cli.container', mock_container):
        result = runner.invoke(app, ["refresh"])

        mock_service.refresh_tables_sync.assert_called_once_with(None)
        assert "Attempting to refresh all relevant tables." in result.stdout
        assert "Refresh process completed successfully." in result.stdout
        assert result.exit_code == 0


def test_refresh_not_implemented_error():
    """Test refresh command when refresh_tables_sync raises NotImplementedError."""
    # Mock the service to raise error
    mock_service = mock.Mock()
    error_message = "Feature not implemented"
    mock_service.refresh_tables_sync = mock.Mock(side_effect=NotImplementedError(error_message))

    # Create a proper mock container that returns our mock service
    mock_container = mock.Mock()
    mock_container.data_refresh_service = mock_service

    with mock.patch('src.cli.container', mock_container):
        result = runner.invoke(app, ["refresh"])

        # Use result.output which contains both stdout and stderr (mixed mode is default)
        output = result.output
        assert error_message in output or "An unexpected error occurred" in output
        assert result.exit_code == 1


def test_refresh_generic_exception():
    """Test refresh command when refresh_tables_sync raises a generic Exception."""
    # Mock the service to raise error
    mock_service = mock.Mock()
    error_message = "Something went wrong"
    mock_service.refresh_tables_sync = mock.Mock(side_effect=Exception(error_message))

    # Create a proper mock container that returns our mock service
    mock_container = mock.Mock()
    mock_container.data_refresh_service = mock_service

    with mock.patch('src.cli.container', mock_container):
        result = runner.invoke(app, ["refresh"])

        # Use result.output which contains both stdout and stderr (mixed mode is default)
        output = result.output
        assert "An unexpected error occurred during refresh:" in output
        assert error_message in output
        assert result.exit_code == 1
