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
git clone https://github.com/yogeshc/csv_to_rds.git
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

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=csv_to_rds tests/

# Run specific test file
pytest tests/test_loader.py
```

### Code Quality

```bash
# Run linter
flake8 csv_to_rds tests

# Run type checker
mypy csv_to_rds

# Format code
black csv_to_rds tests
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

## Author

**Yogesh Chaudhari**
- Email: yogesh@cyogesh.com
- GitHub: [@yogeshc](https://github.com/yogeshc)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2024 Yogesh Chaudhari