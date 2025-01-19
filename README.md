# CSV to RDS Loader

A Python tool for efficiently loading CSV files into AWS RDS databases with validation and error handling.

## Features

- Efficient chunked loading of large CSV files
- Comprehensive error handling and validation
- Configurable database connections
- Detailed logging
- Clean column name handling
- PEP8 compliant
- Well-tested codebase

## Installation

```bash
# Clone the repository
git clone https://github.com/yogeshc/AWS_CSV_RDS_PIPELINE.git
cd csv_to_rds

# Install the package and dependencies
pip install -r requirements.txt
pip install -e .
```

## Configuration

Create a `config.ini` file with your RDS credentials:

```ini
[RDS]
host = your-rds-endpoint.region.rds.amazonaws.com
port = 3306
database = your_database_name
username = your_username
password = your_password
```

## Usage

### Command Line Interface

```bash
# Using the CLI tool
csv-to-rds path/to/your/file.csv target_table_name
```

### Python API

```python
from csv_to_rds import CSVtoRDSLoader

# Initialize loader
loader = CSVtoRDSLoader('config.ini')
loader.initialize()

try:
    # Load CSV file into RDS table
    rows_loaded = loader.load_file('path/to/your/file.csv', 'target_table_name')
    print(f"Successfully loaded {rows_loaded} rows")
finally:
    loader.close()
```

## Testing

### Prerequisites
Install test dependencies:
```bash
pip install pytest pytest-cov pytest-mock
```

### Running Tests

1. Run all tests:
```bash
pytest
```

2. Run tests with verbose output:
```bash
pytest -v
```

3. Run tests with coverage report:
```bash
pytest --cov=csv_to_rds tests/
```

4. Run specific test file:
```bash
pytest tests/test_csv_to_rds.py
```

5. Run tests matching specific pattern:
```bash
pytest -k "test_validate"  # Runs all tests with "test_validate" in the name
```

### Test Coverage Areas

The test suite covers:
1. **Configuration Management**
   - Config file loading
   - Validation of required fields
   - Error handling for missing/invalid configuration

2. **Database Operations**
   - Connection establishment
   - Data insertion
   - Chunked loading
   - Transaction handling
   - Error recovery

3. **Data Validation**
   - CSV file validation
   - Column name validation
   - Data type checking
   - Empty file handling
   - Error reporting

4. **Integration Testing**
   - End-to-end data loading
   - Config to database flow
   - Error propagation

### Test Structure

The test suite is organized into several test classes:

- `TestConfigManager`: Tests configuration loading and validation
- `TestDatabaseManager`: Tests database operations and connections
- `TestCSVtoRDSLoader`: Tests the main loader functionality
- Standalone test functions for utilities

### Mock Objects

The tests use mock objects to:
- Simulate database connections
- Mock file operations
- Test error conditions
- Verify function calls

### Example Test Run

```bash
$ pytest -v
============================= test session starts ==============================
collecting ... collected 12 items

test_csv_to_rds.py::TestConfigManager::test_load_database_config_valid PASSED
test_csv_to_rds.py::TestConfigManager::test_load_database_config_missing_file PASSED
...
============================== 12 passed in 2.13s =============================
```

## Error Handling

The package includes custom exceptions for different types of errors:

- `ConfigurationError`: Configuration-related errors
- `DatabaseError`: Database operation errors
- `ValidationError`: Data validation errors

## Logging

The package logs all operations to both file (`csv_to_rds.log`) and console output. The logging level is configurable.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
1. Add tests for new features
2. Maintain code coverage above 90%
3. Follow PEP8 style guide
4. Update documentation as needed

## Author

**Yogesh Chaudhari**
- Email: yogesh@cyogesh.com
- GitHub: [@yogeshc](https://github.com/yogeshc)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2024 Yogesh Chaudhari