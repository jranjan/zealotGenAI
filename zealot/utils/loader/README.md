# Loader Utilities

This package provides file loading utilities for different data formats.

## Available Loaders

### JSONLoader

A simple JSON file loader that reads files on-demand without caching.

**Features:**
- Load JSON files from a directory
- Access nested attributes using dot notation
- No memory caching (reads from disk each time)
- Memory efficient for large datasets

**Usage:**
```python
from zealot.utils.loader import JSONLoader

# Initialize loader
loader = JSONLoader("path/to/json/files")

# List available files
files = loader.list_files()

# Get file data
data = loader.get_file_data("config")

# Get nested attribute
value = loader.get_attribute("config", "database.host")
```

## Package Structure

```
loader/
├── __init__.py          # Package initialization
├── json.py              # JSON file loader
└── README.md           # This file
```

## Future Loaders

This package can be extended with additional loaders for:
- CSV files
- XML files
- YAML files
- Configuration files
- Database connections
