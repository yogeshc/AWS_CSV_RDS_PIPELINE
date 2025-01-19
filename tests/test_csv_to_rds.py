#!/usr/bin/env python3
"""
Unit tests for CSV to RDS loader
"""

import os
import pytest
import pandas as pd
from unittest.mock import Mock, patch
from csv_to_rds import (
    CSVtoRDSLoader,
    ConfigManager,
    DatabaseManager,
    DatabaseConfig,
    ConfigurationError,
    DatabaseError,
    ValidationError,
    validate_csv_file,
    clean_column_names
)

# Test data
VALID_CONFIG = """
[RDS]
host = test-host
port = 3306
database = test-db
username = test-user
password = test-pass
"""


@pytest.fixture
def config_manager():
    return ConfigManager('test_config.ini')


@pytest.fixture
def db_config():
    return DatabaseConfig(
        host='test-host',
        port=3306,
        database='test-db',
        username='test-user',
        password='test-pass'
    )


@pytest.fixture
def db_manager(db_config):
    return DatabaseManager(db_config)


@pytest.fixture
def loader():
    return CSVtoRDSLoader('test_config.ini')


class TestConfigManager:
    """Tests for ConfigManager class"""

    def test_load_database_config_valid(self, config_manager):
        """Test loading valid database configuration"""
        with patch('builtins.open', mock_open(read_data=VALID_CONFIG)):
            with patch('os.path.exists') as mock_exists:
                mock_exists.return_value = True
                config = config_manager.load_database_config()

                assert isinstance(config, DatabaseConfig)
                assert config.host == 'test-host'
                assert config.port == 3306
                assert config.database == 'test-db'

    def test_load_database_config_missing_file(self, config_manager):
        """Test loading configuration with missing file"""
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = False
            with pytest.raises(ConfigurationError):
                config_manager.load_database_config()


class TestDatabaseManager:
    """Tests for DatabaseManager class"""

    def test_connect_success(self, db_manager):
        """Test successful database connection"""

        def test_load_file_success(self, loader):

    """Test successful file loading"""
    with patch('csv_to_rds.validate_csv_file') as mock_validate:
        with patch('pandas.read_csv') as mock_read_csv:
            test_data = pd.DataFrame({
                'col1': range(10),
                'col2': range(10)
            })
            mock_validate.return_value = (True, "")
            mock_read_csv.return_value = test_data

            loader.db_manager = Mock()
            loader.db_manager.load_data_in_chunks.return_value = 10

            rows_loaded = loader.load_file('test.csv', 'test_table')
            assert rows_loaded == 10
            loader.db_manager.load_data_in_chunks.assert_called_once()


def test_load_file_invalid_csv(self, loader):
    """Test file loading with invalid CSV"""
    with patch('csv_to_rds.validate_csv_file') as mock_validate:
        mock_validate.return_value = (False, "Invalid CSV")

        with pytest.raises(ValidationError) as exc_info:
            loader.load_file('test.csv', 'test_table')
        assert "Invalid CSV" in str(exc_info.value)


def test_load_file_empty_dataframe(self, loader):
    """Test file loading with empty DataFrame"""
    with patch('csv_to_rds.validate_csv_file') as mock_validate:
        with patch('pandas.read_csv') as mock_read_csv:
            mock_validate.return_value = (True, "")
            mock_read_csv.return_value = pd.DataFrame()

            with pytest.raises(ValidationError) as exc_info:
                loader.load_file('test.csv', 'test_table')
            assert "CSV file contains no data" in str(exc_info.value)


def test_close(self, loader):
    """Test loader cleanup"""
    loader.db_manager = Mock()
    loader.close()
    loader.db_manager.close.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__])
    to_rds.create_engine
    ') as mock_create_engine:
    mock_engine = Mock()
    mock_connection = Mock()
    mock_engine.connect.return_value.__enter__.return_value = mock_connection
    mock_create_engine.return_value = mock_engine

    engine = db_manager.connect()
    assert engine == mock_engine
    mock_connection.execute.assert_called_once_with("SELECT 1")


def test_load_data_chunks_success(self, db_manager):
    """Test successful chunked data loading"""
    test_data = pd.DataFrame({
        'col1': range(100),
        'col2': range(100)
    })

    db_manager._engine = Mock()
    rows_loaded = db_manager.load_data_in_chunks(
        test_data,
        'test_table',
        chunk_size=10
    )
    assert rows_loaded == 100


class TestValidation:
    """Tests for validation functions"""

    def test_validate_csv_file(self):
        """Test CSV file validation"""
        with patch('os.path.exists') as mock_exists:
            with patch('os.path.getsize') as mock_getsize:
                mock_exists.return_value = True
                mock_getsize.return_value = 100

                with patch('pandas.read_csv') as mock_read_csv:
                    mock_read_csv.return_value = pd.DataFrame()
                    is_valid, message = validate_csv_file('test.csv')
                    assert is_valid is True

    def test_clean_column_names(self):
        """Test column name cleaning"""
        test_columns = [
            'Column Name',
            'Column-Name',
            'Column.Name'
        ]
        expected = [
            'column_name',
            'column_name',
            'column_name'
        ]
        assert clean_column_names(test_columns) == expected


class TestCSVtoRDSLoader:
    """Tests for main loader class"""

    def test_initialize_success(self, loader):
        """Test successful loader initialization"""
        with patch('csv_to_rds.ConfigManager') as mock_config_manager:
            with patch('csv_to_rds.DatabaseManager') as mock_db_manager:
                mock_config_manager.return_value.load_database_config.return_value = Mock()
                mock_db_manager.return_value.connect.return_value = Mock()

                loader.initialize()
                assert loader.db_manager is not None

    def test_load_file_success(self, loader):
        """Test successful file loading"""
        with patch('csv_