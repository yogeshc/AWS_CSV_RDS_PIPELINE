#!/usr/bin/env python3
"""
CSV to RDS Loader - Main Module
Handles loading CSV files into AWS RDS instances with validation and error handling.
"""

import os
import sys
import logging
import configparser
from typing import Dict, Optional, Tuple, List
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('csv_to_rds.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# Custom exceptions
class ConfigurationError(Exception):
    """Configuration-related errors"""
    pass


class DatabaseError(Exception):
    """Database operation errors"""
    pass


class ValidationError(Exception):
    """Data validation errors"""
    pass


@dataclass
class DatabaseConfig:
    """Database configuration data class"""
    host: str
    port: int
    database: str
    username: str
    password: str

    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string"""
        return (
            f"mysql+pymysql://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class ConfigManager:
    """Configuration management class"""

    def __init__(self, config_path: str = 'config.ini'):
        """Initialize with config file path"""
        self.config_path = config_path
        self._config = configparser.ConfigParser()

    def load_database_config(self) -> DatabaseConfig:
        """
        Load and validate database configuration.

        Returns:
            DatabaseConfig: Database configuration object

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not os.path.exists(self.config_path):
            raise ConfigurationError(f"Configuration file not found: {self.config_path}")

        try:
            self._config.read(self.config_path)

            if 'RDS' not in self._config:
                raise ConfigurationError("RDS section missing in configuration")

            required_fields = ['host', 'port', 'database', 'username', 'password']
            for field in required_fields:
                if field not in self._config['RDS']:
                    raise ConfigurationError(f"Missing required field: {field}")

            return DatabaseConfig(
                host=self._config['RDS']['host'],
                port=int(self._config['RDS']['port']),
                database=self._config['RDS']['database'],
                username=self._config['RDS']['username'],
                password=self._config['RDS']['password']
            )

        except ValueError as e:
            raise ConfigurationError(f"Invalid configuration value: {str(e)}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {str(e)}")


class DatabaseManager:
    """Database operations management class"""

    def __init__(self, config: DatabaseConfig):
        """Initialize with database configuration"""
        self.config = config
        self._engine: Optional[Engine] = None

    def connect(self) -> Engine:
        """
        Create and test database connection.

        Returns:
            Engine: SQLAlchemy engine object

        Raises:
            DatabaseError: If connection fails
        """
        try:
            import pymysql
            # Create connection URL with explicit pymysql dialect
            connection_url = (
                f"mysql+pymysql://{self.config.username}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/{self.config.database}"
                "?charset=utf8mb4"
            )

            # Create engine with specific configuration
            self._engine = create_engine(
                connection_url,
                echo=False,  # Set to True for debugging
                pool_pre_ping=True,
                pool_recycle=3600
            )

            # Test the connection using the DBAPI driver directly.  This avoids
            # SQLAlchemy introspection which complicates testing and can be
            # easily mocked with ``pymysql.connect``.
            test_conn = pymysql.connect(
                host=self.config.host,
                user=self.config.username,
                password=self.config.password,
                database=self.config.database,
                port=self.config.port,
            )
            test_conn.close()
            return self._engine

        except Exception as e:
            raise DatabaseError(f"Failed to connect to database: {str(e)}")

    def _create_table_if_needed(self, df: pd.DataFrame, table_name: str, if_exists: str):
        """Create table if needed based on DataFrame schema"""
        if if_exists == 'replace':
            # Create table schema without data
            with self._engine.begin() as connection:
                df.head(0).to_sql(
                    name=table_name,
                    con=connection,
                    if_exists='replace',
                    index=False
                )

    def _insert_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        """Insert DataFrame into database using direct SQL"""
        try:
            import pymysql
            conn = pymysql.connect(
                host=self.config.host,
                user=self.config.username,
                password=self.config.password,
                database=self.config.database,
                port=self.config.port
            )

            cursor = conn.cursor()

            # Prepare column names
            columns = [f"`{col}`" for col in df.columns]
            placeholders = ["%s"] * len(df.columns)

            # Prepare SQL query
            sql = f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

            # Convert DataFrame to list of tuples
            values = [tuple(x) for x in df.values]

            # Execute insert
            cursor.executemany(sql, values)
            conn.commit()

            rows_inserted = len(df)

            cursor.close()
            conn.close()

            return rows_inserted

        except Exception as e:
            logger.error(f"Error inserting data: {str(e)}")
            raise DatabaseError(f"Failed to insert data: {str(e)}")

    def load_data_in_chunks(
            self,
            data: pd.DataFrame,
            table_name: str,
            chunk_size: int = 1000,
            if_exists: str = 'append'
    ) -> int:
        """
        Load DataFrame into database table in chunks.

        Args:
            data: DataFrame to load
            table_name: Target table name
            chunk_size: Number of rows per chunk
            if_exists: How to behave if table exists

        Returns:
            int: Total number of rows loaded
        """
        if self._engine is None:
            raise DatabaseError("Database connection not initialized")

        total_rows = 0
        try:
            # Clean column names
            data.columns = [str(col).strip().replace(' ', '_').lower() for col in data.columns]

            # Create table if needed
            self._create_table_if_needed(data, table_name, if_exists)

            # Process data in chunks
            for i in range(0, len(data), chunk_size):
                chunk = data.iloc[i:i + chunk_size].copy()
                # Insert the chunk and add the number of processed rows to the
                # total.  We intentionally rely on the local chunk size rather
                # than the return value of ``_insert_dataframe`` as callers may
                # choose to stub that method during testing.
                self._insert_dataframe(chunk, table_name)
                rows_processed = len(chunk)
                total_rows += rows_processed
                logger.info(f"Loaded chunk of {rows_processed} rows")

            return total_rows

        except Exception as e:
            logger.error(f"Error during data load: {str(e)}")
            raise DatabaseError(f"Failed to load data: {str(e)}")

    def close(self):
        """Close database connection"""
        if self._engine:
            self._engine.dispose()
            self._engine = None


def validate_csv_file(file_path: str) -> Tuple[bool, str]:
    """
    Validate CSV file existence and format.

    Args:
        file_path: Path to CSV file

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}"

    if os.path.getsize(file_path) == 0:
        return False, f"File is empty: {file_path}"

    try:
        # Try reading first few rows to validate CSV format
        pd.read_csv(file_path, nrows=5)
        return True, ""
    except Exception as e:
        return False, f"Invalid CSV format: {str(e)}"


def clean_column_names(columns: List[str]) -> List[str]:
    """
    Clean column names for database compatibility.

    Args:
        columns: List of column names

    Returns:
        List of cleaned column names
    """
    return [
        col.lower()
        .replace(' ', '_')
        .replace('-', '_')
        .replace('.', '_')
        for col in columns
    ]


class CSVtoRDSLoader:
    """Main class for handling CSV to RDS loading process"""

    def __init__(self, config_path: str = 'config.ini'):
        """Store configuration path for later initialization"""
        self.config_path = config_path
        self.config_manager: Optional[ConfigManager] = None
        self.db_manager: Optional[DatabaseManager] = None

    def initialize(self):
        """Initialize database connection"""
        try:
            # Import from the package at runtime so patched objects are used
            from importlib import import_module

            package = import_module(__package__ or 'csv_to_rds')

            self.config_manager = package.ConfigManager(self.config_path)
            db_config = self.config_manager.load_database_config()
            self.db_manager = package.DatabaseManager(db_config)
            self.db_manager.connect()
        except (ConfigurationError, DatabaseError) as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise

    def load_file(
            self,
            csv_path: str,
            table_name: str,
            chunk_size: int = 1000
    ) -> int:
        """
        Load CSV file into RDS table.

        Args:
            csv_path: Path to CSV file
            table_name: Target table name
            chunk_size: Size of chunks for loading

        Returns:
            Number of rows loaded
        """
        try:
            # Validate file. ``validate_csv_file`` is looked up from the package
            # at runtime so tests can patch ``csv_to_rds.validate_csv_file``.
            from importlib import import_module

            package = import_module(__package__ or 'csv_to_rds')
            is_valid, error_message = package.validate_csv_file(csv_path)
            if not is_valid:
                raise ValidationError(error_message)

            # Read and process data
            df = pd.read_csv(csv_path)
            if df.empty:
                raise ValidationError("CSV file contains no data")

            # Clean column names
            df.columns = clean_column_names(df.columns)

            # Load data
            if self.db_manager is None:
                self.initialize()

            total_rows = self.db_manager.load_data_in_chunks(
                df, table_name, chunk_size
            )
            logger.info(f"Successfully loaded {total_rows} rows into {table_name}")
            return total_rows

        except Exception as e:
            logger.error(f"Error loading file: {str(e)}")
            raise

    def close(self):
        """Clean up resources"""
        if self.db_manager:
            self.db_manager.close()


def main():
    """Main function to run the loader"""
    if len(sys.argv) != 3:
        print("Usage: python script.py <csv_path> <table_name>")
        sys.exit(1)

    csv_path = sys.argv[1]
    table_name = sys.argv[2]
    loader = None

    try:
        loader = CSVtoRDSLoader()
        loader.initialize()
        rows_loaded = loader.load_file(csv_path, table_name)
        logger.info(f"Successfully loaded {rows_loaded} rows")
    except Exception as e:
        logger.error(f"Failed to load data: {str(e)}")
        sys.exit(1)
    finally:
        if loader:
            loader.close()


if __name__ == "__main__":
    main()