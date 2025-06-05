#!/usr/bin/env python3
"""
Unit tests for CSV to RDS loader
"""

import os
import pytest
import pandas as pd
import pymysql
from unittest.mock import Mock, patch, MagicMock, mock_open
from csv_to_rds import (
    CSVtoRDSLoader,
    ConfigManager,
    DatabaseManager,
    DatabaseConfig,
    ConfigurationError,
    DatabaseError,
    ValidationError,
    validate_csv_file,
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
def sample_csv_data():
    """Create sample CSV data matching our schema"""
    return pd.DataFrame({
        'uuid': [1, 2, 3],
        'Country': ['US', 'UK', 'CA'],
        'ItemType': ['A', 'B', 'C'],
        'SalesChannel': ['Online', 'Offline', 'Online'],
        'OrderPriority': ['High', 'Medium', 'Low'],
        'OrderDate': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'Region': ['North', 'South', 'East'],
        'ShipDate': ['2024-01-05', '2024-01-06', '2024-01-07'],
        'UnitsSold': [100, 200, 300],
        'UnitPrice': [10.50, 20.75, 15.25],
        'UnitCost': [8.50, 15.75, 12.25],
        'TotalRevenue': [1050.00, 4150.00, 4575.00],
        'TotalCost': [850.00, 3150.00, 3675.00],
        'TotalProfit': [200.00, 1000.00, 900.00]
    })


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
        with patch('pymysql.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_connect.return_value.cursor.return_value = mock_cursor

            engine = db_manager.connect()
            assert engine is not None

    @patch('pymysql.connect')
    def test_insert_dataframe(self, mock_connect, db_manager, sample_csv_data):
        """Test DataFrame insertion"""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor

        result = db_manager._insert_dataframe(sample_csv_data, 'test_table')
        assert result == len(sample_csv_data)
        mock_cursor.executemany.assert_called_once()

    def test_load_data_in_chunks(self, db_manager, sample_csv_data):
        """Test chunked data loading"""
        with patch.object(DatabaseManager, '_insert_dataframe') as mock_insert:
            mock_insert.return_value = len(sample_csv_data)
            db_manager._engine = Mock()  # Mock the engine

            total_rows = db_manager.load_data_in_chunks(
                sample_csv_data,
                'test_table',
                chunk_size=2
            )

            assert total_rows == len(sample_csv_data)
            assert mock_insert.call_count == 2  # Should be called twice with chunk_size=2


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

    def test_load_file_success(self, loader, sample_csv_data):
        """Test successful file loading"""
        with patch('pandas.read_csv') as mock_read_csv:
            with patch('csv_to_rds.validate_csv_file') as mock_validate:
                mock_validate.return_value = (True, "")
                mock_read_csv.return_value = sample_csv_data

                loader.db_manager = Mock()
                loader.db_manager.load_data_in_chunks.return_value = len(sample_csv_data)

                rows_loaded = loader.load_file('test.csv', 'test_table')
                assert rows_loaded == len(sample_csv_data)

    def test_load_file_validation_error(self, loader):
        """Test file loading with validation error"""
        with patch('csv_to_rds.validate_csv_file') as mock_validate:
            mock_validate.return_value = (False, "Invalid CSV")

            with pytest.raises(ValidationError) as exc_info:
                loader.load_file('test.csv', 'test_table')
            assert "Invalid CSV" in str(exc_info.value)


def test_validate_csv_file_success(tmp_path):
    """Test CSV file validation with valid file"""
    file_path = tmp_path / "test.csv"
    with open(file_path, 'w') as f:
        f.write("header1,header2\n1,2\n3,4")

    is_valid, message = validate_csv_file(str(file_path))
    assert is_valid is True
    assert message == ""


def test_validate_csv_file_not_exists():
    """Test validation of non-existent CSV file"""
    is_valid, message = validate_csv_file('nonexistent.csv')
    assert is_valid is False
    assert "File not found" in message


if __name__ == '__main__':
    pytest.main([__file__])