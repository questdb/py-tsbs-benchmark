# Contributing to py-tsbs-benchmark

Thank you for your interest in contributing to py-tsbs-benchmark! This guide will help you get started.

## 🚀 Quick Start for Contributors

1. **Fork the repository** on GitHub
2. **Clone your fork**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/py-tsbs-benchmark.git
   cd py-tsbs-benchmark
   ```
3. **Set up development environment**:
   ```bash
   poetry install --with dev
   ```
4. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/your-improvement
   ```
5. **Make your changes** and test them
6. **Submit a pull request**

## 🧪 Running Tests

Before submitting your changes, make sure all tests pass:

```bash
# Run all tests
poetry run pytest

# Run tests with coverage
poetry run pytest --cov=py_tsbs_benchmark

# Run specific test file
poetry run pytest tests/test_common.py -v
```

## 📝 Code Style

We follow Python best practices:

- **PEP 8** for code formatting
- **Type hints** where possible
- **Docstrings** for all functions and classes (Google/NumPy style)
- **Descriptive variable names** and comments

### Code Formatting

We recommend using Black and flake8:

```bash
# Format code
poetry run black py_tsbs_benchmark/ tests/

# Check code style
poetry run flake8 py_tsbs_benchmark/ tests/

# Type checking
poetry run mypy py_tsbs_benchmark/
```

## 🛠️ Development Guidelines

### Adding New Features

1. **Write tests first** (TDD approach encouraged)
2. **Add comprehensive docstrings** with examples
3. **Include error handling** with proper logging
4. **Update documentation** if needed

### Error Handling

- Use specific exception types
- Add logging for debugging
- Provide helpful error messages
- Handle edge cases gracefully

### Example of Good Error Handling:

```python
import logging

logger = logging.getLogger(__name__)

def process_data(data):
    """Process input data with proper error handling.
    
    Args:
        data: Input data to process
        
    Returns:
        Processed data
        
    Raises:
        ValueError: If data is invalid
        RuntimeError: If processing fails
    """
    try:
        if not data:
            raise ValueError("Input data cannot be empty")
        
        logger.info(f"Processing {len(data)} items")
        result = expensive_operation(data)
        logger.info("Processing completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        raise RuntimeError(f"Failed to process data: {e}") from e
```

## 🎯 Areas Looking for Contributions

### Beginner-Friendly Issues

- **Documentation improvements**: Fix typos, add examples, improve clarity
- **Error messages**: Make them more helpful and user-friendly  
- **Unit tests**: Increase test coverage for edge cases
- **Code comments**: Add explanatory comments for complex logic

### Intermediate Tasks

- **Performance optimizations**: Profile and improve slow operations
- **Logging enhancements**: Add structured logging with appropriate levels
- **Configuration**: Add support for config files (YAML/TOML)
- **CLI improvements**: Better help messages, validation, progress bars

### Advanced Projects

- **New benchmark modes**: Different data patterns or workloads
- **Memory profiling**: Track and report memory usage during benchmarks
- **Async support**: Add async/await patterns where beneficial
- **CI/CD pipeline**: Set up GitHub Actions for testing and releases

## 📋 Pull Request Guidelines

### Before Submitting

- [ ] Tests pass locally (`poetry run pytest`)
- [ ] Code is formatted (`poetry run black .`)
- [ ] No linting errors (`poetry run flake8`)
- [ ] Documentation updated if needed
- [ ] CHANGELOG.md updated (if applicable)

### Pull Request Description

Please include:

1. **What** changes you made
2. **Why** you made them (link to issue if applicable)
3. **How** to test the changes
4. **Screenshots** or examples if relevant

### Example PR Title and Description

```
feat: Add structured logging with configurable levels

- Replaces print statements with proper logging
- Adds --log-level CLI option for controlling verbosity  
- Includes request/response logging for debugging database issues
- Adds logging configuration in main() function

Fixes #123

Testing:
- Run with --log-level DEBUG to see detailed logs
- Run with --log-level INFO for normal operation
- Verify no functionality changes for existing users
```

## 🐛 Reporting Issues

When reporting bugs, please include:

- Python version and OS
- QuestDB version
- Full error traceback
- Minimal reproduction steps
- Expected vs actual behavior

## 📚 Development Environment

### Required Tools

- **Python 3.10+**
- **Poetry** for dependency management
- **QuestDB** for integration testing
- **Git** for version control

### Optional but Recommended

- **VS Code** with Python extension
- **Docker** for running QuestDB consistently
- **pytest-xdist** for parallel test execution

### Environment Setup Script

```bash
#!/bin/bash
# setup_dev.sh - One-time development setup

# Install Poetry if not present
if ! command -v poetry &> /dev/null; then
    curl -sSL https://install.python-poetry.org | python3 -
fi

# Install dependencies
poetry install --with dev

# Setup pre-commit hooks (if available)
poetry run pre-commit install 2>/dev/null || echo "Pre-commit not configured"

# Start QuestDB in background
docker run -d -p 9000:9000 -p 9009:9009 --name questdb-dev questdb/questdb

echo "✅ Development environment ready!"
echo "📝 Run 'poetry run pytest' to verify setup"
```

## 🤝 Code of Conduct

We are committed to providing a welcoming and inclusive environment:

- **Be respectful** and considerate
- **Be collaborative** and help others learn
- **Be patient** with newcomers and different skill levels
- **Give constructive feedback** in reviews

## ❓ Questions?

- **General questions**: Open a GitHub Discussion
- **Bug reports**: Create a GitHub Issue  
- **Feature requests**: Create a GitHub Issue with "enhancement" label
- **Security issues**: Email the maintainers directly

## 🎉 Recognition

Contributors will be:

- Added to the AUTHORS file
- Mentioned in release notes
- Given credit in pull request merges

Thank you for helping improve py-tsbs-benchmark! 🚀
