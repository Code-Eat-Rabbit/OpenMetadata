# Owner Configuration Test Suite

## Overview

This test suite validates the owner assignment functionality for metadata ingestion with a focus on:
- **Owner type constraints** (multiple users allowed, only ONE team allowed, users and teams are mutually exclusive)
- **Inheritance mechanism** (child entities inherit from parent when no specific config)
- **FQN matching** (exact FQN match vs simple name fallback)
- **Validation and error handling**

## Business Rules

### Owner Type Constraints
1. ✅ **Multiple users allowed**: `["alice", "bob", "charlie"]`
2. ✅ **Only ONE team allowed**: `"sales-team"` (string, not array)
3. ❌ **Users and teams are mutually exclusive**: Cannot mix `["alice", "sales-team"]`

### Resolution Priority
1. **Specific configuration** (exact FQN or name match in current level)
2. **Inherited owner** (from parent entity, if `enableInheritance: true`)
3. **Default owner** (fallback when nothing else matches)

## Test Suite Structure

### 8 Core Test Scenarios

| Test # | Name | Description |
|--------|------|-------------|
| 01 | Basic Configuration | Tests default + hierarchical owner assignment |
| 02 | FQN Matching | Tests FQN exact match vs simple name fallback |
| 03 | Multiple Users | Tests multiple users as owners (valid scenario) |
| 04 | Validation Errors | Tests owner type constraint violations |
| 05 | Inheritance Enabled | Tests inheritance mechanism (critical for Bug 2) |
| 06 | Inheritance Disabled | Tests that `enableInheritance: false` works |
| 07 | Partial Success | Tests resilience to non-existent owners |
| 08 | Complex Mixed | Integration test combining all features |

## Prerequisites

### 1. PostgreSQL Test Database

Start the PostgreSQL container with test data:

```bash
cd ingestion/tests/integration/owner_config_tests
docker-compose up -d
```

This creates:
- **finance_db**: 2 schemas (accounting, treasury), 4 tables, 1 view
- **marketing_db**: 2 schemas (campaigns, analytics), 4 tables, 1 view

### 2. OpenMetadata Instance

Ensure OpenMetadata is running on `http://localhost:8585`

### 3. Create Test Users and Teams

Before running tests, create these users and teams in OpenMetadata:

**Users:**
- alice, bob, charlie, david, emma, frank
- marketing-user-1, marketing-user-2

**Teams:**
- data-platform-team, finance-team, marketing-team
- accounting-team, treasury-team, expense-team
- revenue-team, investment-team, treasury-ops-team
- audit-team, compliance-team

### 4. JWT Token

Update `jwtToken` in each test YAML file with a valid token.

## Running Tests

### Run All Tests
```bash
cd ingestion
for test in tests/integration/owner_config_tests/test-*.yaml; do
    echo "Running $test..."
    metadata ingest -c "$test"
done
```

### Run Single Test
```bash
cd ingestion
metadata ingest -c tests/integration/owner_config_tests/test-01-basic-configuration.yaml
```

### Run with Verbose Logging
```bash
metadata ingest -c tests/integration/owner_config_tests/test-05-inheritance-enabled.yaml 2>&1 | grep -E "INFO|WARNING|DEBUG.*owner"
```

## Validation

### Test 01: Basic Configuration
- ✅ finance_db has owner "finance-team"
- ✅ marketing_db has owner "marketing-team"
- ✅ accounting schema has owner "accounting-team"
- ✅ revenue table has owner "revenue-team"
- ✅ Other entities have owner "data-platform-team"

### Test 02: FQN Matching
- ✅ FQN exact matches work without INFO logs
- ✅ Simple name fallback logs INFO: "FQN match failed..."

### Test 03: Multiple Users
- ✅ finance_db has 2 owners (alice, bob)
- ✅ revenue table has 3 owners (charlie, david, emma)
- ✅ All owners have `type="user"`

### Test 04: Validation Errors
- ✅ Multiple teams: logs WARNING "Only ONE team allowed"
- ✅ Mixed users/teams: logs WARNING "Cannot mix users and teams"
- ✅ Single team string: works normally

### Test 05: Inheritance Enabled ⭐ CRITICAL
- ✅ accounting schema inherits "finance-team" (NOT default)
- ✅ revenue table inherits from accounting schema (NOT default)
- ✅ Priority: specific > inherited > default

### Test 06: Inheritance Disabled
- ✅ accounting schema uses "data-platform-team" (default, NOT inherited)
- ✅ revenue table uses "data-platform-team" (default, NOT inherited)

### Test 07: Partial Success
- ✅ revenue table has 2 owners (alice, bob), skips nonexistent users
- ✅ Logs WARNING: "Could not find owner: nonexistent-user-1"
- ✅ Logs WARNING: "Could not find owner: nonexistent-user-2"

### Test 08: Complex Mixed
- ✅ All features work together without conflicts
- ✅ FQN + simple name matching
- ✅ Multiple users + single teams
- ✅ Inheritance + explicit configs

## Troubleshooting

### Issue: Owner not assigned
- Check that user/team exists in OpenMetadata
- Verify JWT token is valid
- Check logs for validation errors
- Ensure `overrideMetadata: true` is set

### Issue: Inheritance not working
- Verify `enableInheritance: true` in ownerConfig
- Check that parent entity has an owner assigned
- Review logs for inheritance debug messages

### Issue: Validation errors
- Check owner types (user vs team) in OpenMetadata
- Ensure not mixing users and teams in same array
- Verify only one team in array configs

## Database Schema

### finance_db
```
finance_db
├── accounting (schema)
│   ├── revenue (table)
│   ├── expenses (table)
│   ├── budgets (table) - NEW
│   └── monthly_summary (view)
└── treasury (schema)
    ├── cash_flow (table)
    ├── investments (table)
    └── forecasts (table) - NEW
```

### marketing_db
```
marketing_db
├── campaigns (schema)
│   ├── email_campaigns (table)
│   ├── social_media (table)
│   └── social_ads (table) - NEW
└── analytics (schema)
    ├── customer_segments (table)
    ├── conversion_funnel (table)
    ├── web_traffic (table) - NEW
    └── campaign_performance (view)
```

## Expected Log Patterns

### DEBUG Level
```
Resolving owner for table 'revenue'
Found owner: alice (type: user)
Using inherited owner for 'accounting': finance-team
```

### INFO Level
```
FQN match failed for 'finance_db.treasury', matched using simple name 'treasury'
```

### WARNING Level
```
VALIDATION ERROR: Only ONE team allowed as owner, but got 3 teams
VALIDATION ERROR: Cannot mix users and teams in owner list
Could not find owner: nonexistent-user-1
```

## Cleanup

```bash
cd ingestion/tests/integration/owner_config_tests
docker-compose down -v
```

## Related Files

- **Python Implementation**: `ingestion/src/metadata/utils/owner_utils.py`
- **JSON Schema**: `openmetadata-spec/src/main/resources/json/schema/type/ownerConfig.json`
- **Unit Tests**: `ingestion/tests/unit/test_owner_utils.py`
