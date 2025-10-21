# ⚠️ DEPRECATED: Bash/YAML-Based Tests

## Notice

**This directory contains deprecated test infrastructure that has been replaced by a standard pytest suite.**

### Status: 🚫 Deprecated (October 2025)

These bash/YAML-based tests are **no longer maintained** and will be removed in a future release.

## Migration Completed

All test scenarios have been successfully migrated to:

📍 **New Location**: `ingestion/tests/unit/metadata/ingestion/test_owner_config.py`

### Quick Start with New Tests

```bash
# Run all owner config tests
cd ingestion
pytest tests/unit/metadata/ingestion/test_owner_config.py -v

# Run specific test
pytest tests/unit/metadata/ingestion/test_owner_config.py::TestOwnerConfig::test_01_basic_configuration -v
```

## What Was Migrated

| Old File (Deprecated) | New Test Function |
|-----------------------|-------------------|
| test-01-basic-configuration.yaml | `test_01_basic_configuration()` |
| test-02-fqn-matching.yaml | `test_02_fqn_matching()` |
| test-03-multiple-users.yaml | `test_03_multiple_users()` |
| test-04-validation-errors.yaml | `test_04_validation_errors()` |
| test-05-inheritance-enabled.yaml | `test_05_inheritance_enabled()` |
| test-06-inheritance-disabled.yaml | `test_06_inheritance_disabled()` |
| test-07-partial-success.yaml | `test_07_partial_success()` |
| test-08-complex-mixed.yaml | `test_08_complex_mixed()` |
| run-all-tests.sh | Standard pytest execution |
| setup-test-entities.sh | Mocked in test setup |

## Why This Was Deprecated

### Problems with Old Approach
- ❌ Required external OpenMetadata server running
- ❌ Needed docker-compose for database setup
- ❌ Manual entity creation (users/teams) via bash scripts
- ❌ Not following project pytest patterns
- ❌ Difficult to integrate with CI/CD
- ❌ Slow execution (external services startup time)
- ❌ Brittle (dependent on network, service availability)

### Benefits of New Approach
- ✅ Standard pytest suite (follows project conventions)
- ✅ Mocked OpenMetadata API (no external dependencies)
- ✅ Fast execution (pure Python unit tests)
- ✅ CI/CD friendly
- ✅ Type-safe with full type annotations
- ✅ Easy to debug and maintain
- ✅ Self-contained tests

## Code Reviewer Feedback Addressed

> "Overall the idea LGTM, I just think the tests are a bit out of the usual flow we follow here. Could you please review how we are using this testcontainer and create a normal pytest suite to handle the execution of the different scenarios instead of having to work with bash files and separate YAMLs?"

**Resolution**: ✅ Complete

The new pytest suite:
1. Follows project patterns (see `tests/unit/topology/database/test_postgres.py`)
2. Uses standard pytest fixtures and mocking
3. Eliminates bash scripts and separate YAML files
4. Integrates with existing test infrastructure
5. Maintains 100% test coverage of all scenarios

## Documentation

For detailed migration information and usage guide, see:

📚 **Migration Guide**: `../MIGRATION_GUIDE.md`

For feature documentation (owner config feature details), see:

📖 **Feature README**: `README.md` (still relevant for feature understanding)

## Timeline

- **Deprecated**: October 2025
- **Removal Planned**: After verification in production CI runs
- **Migration Status**: ✅ Complete

## Support

If you need to reference the old test scenarios for any reason:
1. The feature logic is documented in `README.md`
2. All scenarios are covered in the new pytest suite
3. Configuration examples are available in both old and new formats

For questions about the new tests, see `../MIGRATION_GUIDE.md`.

---

**Do not use these tests for new development.**  
**Use `test_owner_config.py` instead.**
