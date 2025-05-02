# Welcome to Suthing

SUThing /ˈsu.θɪŋ/ or /ˈsʌ.θɪŋ/ (Some Useful Things) is a collection of useful classes and decorators.  

A Python utility package providing tools for file handling, timing, profiling, and data comparison.

![Python](https://img.shields.io/badge/python-3.10-blue.svg) 
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![pre-commit](https://github.com/growgraph/suthing/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/suthing/actions/workflows/pre-commit.yml)
[![pytest](https://github.com/growgraph/suthing/actions/workflows/pytest.yml/badge.svg)](https://github.com/growgraph/suthing/actions/workflows/pytest.yml)
[![PyPI version](https://badge.fury.io/py/suthing.svg)](https://badge.fury.io/py/suthing)
<!-- [![Documentation Status](https://readthedocs.org/projects/suthing/badge/?version=latest)](https://suthing.readthedocs.io/en/latest/?badge=latest) -->

## Features

- **File Handling**: One-line file reading/writing with file format inference from provided extension (YAML, JSON, CSV, pickle, gz etc.)
- **API/Database Config Class**: Normalized representation of API connection configs
- **Performance Measurement**: Simple timer utilities and profiling decorators
- **Data Comparison**: Deep comparison of nested data structures
- **Error Handling**: Decorators for secure function execution and error tracking

## Quick Start

```python
from suthing import FileHandle, Timer, equals

# Read a file
data = FileHandle.load("config.yaml")

# Time your code
with Timer() as t:
    # Your code here
    pass
print(f"Execution took {t.elapsed_str}")

# Compare complex structures
result = equals(dict1, dict2)
```

## Documentation

Explore the documentation to learn more about Suthing's features:

- [Getting Started](getting_started/quickstart.md) - Learn how to install and use Suthing
- [API Reference](reference/index.md) - Complete API documentation
- [Examples](examples.md) - Code examples and usage patterns

## Requirements

- Python 3.10+
- pandas
- PyYAML
- python-dotenv

## Contributing

We welcome contributions! Please check out our [Contributing Guide](contributing.md) for details on how to get started.
