# Test Scenarios Reference

## Quick Reference Matrix

| Test # | Feature Focus | Validation Type | Critical Bug |
|--------|---------------|-----------------|--------------|
| 01 | Basic hierarchical config | Functionality | - |
| 02 | FQN vs name matching | Logging | - |
| 03 | Multiple users | Validation (positive) | Bug 1 ✅ |
| 04 | Owner type constraints | Validation (negative) | Bug 1 ✅ |
| 05 | Inheritance enabled | Inheritance | Bug 2 ✅ |
| 06 | Inheritance disabled | Inheritance | Bug 2 ✅ |
| 07 | Partial success | Error handling | - |
| 08 | Complex integration | All features | Both ✅ |

## Detailed Test Specifications

### Test 01: Basic Configuration
**Purpose**: Verify fundamental owner assignment at all hierarchy levels

**Configuration**:
```yaml
default: "data-platform-team"
database:
  "finance_db": "finance-team"
  "marketing_db": "marketing-team"
databaseSchema:
  "finance_db.accounting": "accounting-team"
table:
  "finance_db.accounting.revenue": "revenue-team"
enableInheritance: true
```

**Expected Assignments**:
- ✅ finance_db → finance-team
- ✅ marketing_db → marketing-team
- ✅ finance_db.accounting → accounting-team
- ✅ finance_db.treasury → finance-team (inherited from database)
- ✅ finance_db.accounting.revenue → revenue-team
- ✅ finance_db.accounting.expenses → accounting-team (inherited from schema)

---

### Test 02: FQN and Name Matching
**Purpose**: Validate matching priority (FQN exact > simple name fallback)

**Configuration**:
```yaml
databaseSchema:
  "finance_db.accounting": "accounting-team"  # FQN exact
  "treasury": "treasury-team"                 # Simple name
table:
  "finance_db.accounting.expenses": "expense-team"  # FQN exact
  "investments": "investment-team"                  # Simple name
```

**Expected Behavior**:
- ✅ FQN exact match: No INFO log
- ✅ Simple name match: Logs INFO with message "FQN match failed..."

**Expected Logs**:
```
INFO: FQN match failed for 'finance_db.treasury', matched using simple name 'treasury'
INFO: FQN match failed for 'finance_db.treasury.investments', matched using simple name 'investments'
```

---

### Test 03: Multiple Users Valid
**Purpose**: Validate that multiple users can be assigned as owners

**Configuration**:
```yaml
database:
  "finance_db": ["alice", "bob"]
table:
  "finance_db.accounting.revenue": ["charlie", "david", "emma"]
  "finance_db.accounting.expenses": ["frank"]
```

**Expected Assignments**:
- ✅ finance_db: 2 owners (alice, bob) - both type="user"
- ✅ revenue: 3 owners (charlie, david, emma) - all type="user"
- ✅ expenses: 1 owner (frank) - type="user"

**Validation**:
- All owners must have `type="user"` in EntityReference
- Multiple users should be allowed without warnings

---

### Test 04: Owner Type Validation
**Purpose**: Verify that owner type constraints are enforced

**Configuration**:
```yaml
database:
  # CASE 1: Multiple teams (INVALID)
  "finance_db": ["finance-team", "audit-team", "compliance-team"]
table:
  # CASE 2: Mixed users and team (INVALID)
  "finance_db.accounting.revenue": ["alice", "bob", "finance-team"]
  
  # CASE 3: Single team (VALID)
  "finance_db.accounting.expenses": "expense-team"
```

**Expected Behavior**:

**Case 1 - Multiple teams**:
```
WARNING: VALIDATION ERROR: Only ONE team allowed as owner, but got 3 teams. Using only the first team: finance-team
```
- Result: finance_db gets only first team (finance-team) OR fallback to default

**Case 2 - Mixed users and team**:
```
WARNING: VALIDATION ERROR: Cannot mix users and teams in owner list. Found types: {'user', 'team'}. Skipping this owner configuration.
```
- Result: revenue table fallback to inherited owner or default

**Case 3 - Single team**:
- Result: expenses table gets "expense-team" (normal operation, no warnings)

---

### Test 05: Inheritance Enabled ⭐ CRITICAL
**Purpose**: Verify inheritance mechanism works correctly (Bug 2 fix verification)

**Configuration**:
```yaml
default: "data-platform-team"
enableInheritance: true
database:
  "finance_db": "finance-team"
databaseSchema:
  # accounting has NO config - should INHERIT
  "finance_db.treasury": "treasury-team"
table:
  # revenue has NO config - should INHERIT
  "finance_db.accounting.expenses": "expense-team"
```

**Expected Assignments** (Priority: specific > inherited > default):
- ✅ finance_db → finance-team (specific config)
- ✅ accounting schema → finance-team (**INHERITED** from database, NOT default) ⭐
- ✅ revenue table → finance-team (**INHERITED** from accounting, NOT default) ⭐
- ✅ treasury schema → treasury-team (specific config)
- ✅ expenses table → expense-team (specific config)
- ✅ cash_flow table → treasury-team (**INHERITED** from treasury schema)

**Critical Validation** (Bug 2):
If accounting schema or revenue table gets "data-platform-team" instead of "finance-team", Bug 2 is NOT fixed!

---

### Test 06: Inheritance Disabled
**Purpose**: Verify that `enableInheritance: false` prevents inheritance

**Configuration**:
```yaml
default: "data-platform-team"
enableInheritance: false
database:
  "finance_db": "finance-team"
databaseSchema:
  # accounting has NO config
  "finance_db.treasury": "treasury-team"
table:
  # revenue has NO config
  "finance_db.accounting.expenses": "expense-team"
```

**Expected Assignments** (Priority: specific > default, skip inheritance):
- ✅ finance_db → finance-team (specific config)
- ✅ accounting schema → data-platform-team (**DEFAULT**, NOT inherited) ⭐
- ✅ revenue table → data-platform-team (**DEFAULT**, NOT inherited) ⭐
- ✅ treasury schema → treasury-team (specific config)
- ✅ expenses table → expense-team (specific config)

**Critical Validation**:
accounting and revenue must use default, NOT inherit from parent

---

### Test 07: Partial Success
**Purpose**: Verify resilience to non-existent owners (continue ingestion)

**Configuration**:
```yaml
table:
  "finance_db.accounting.revenue": 
    ["alice", "nonexistent-user-1", "bob", "nonexistent-user-2"]
  "finance_db.accounting.expenses":
    ["charlie", "david"]
```

**Expected Behavior**:
- ✅ revenue: 2 owners (alice, bob) - skip nonexistent users
- ✅ expenses: 2 owners (charlie, david) - all found

**Expected Logs**:
```
WARNING: Could not find owner: nonexistent-user-1
WARNING: Could not find owner: nonexistent-user-2
```

**Validation**:
- Ingestion must NOT fail due to missing owners
- Valid owners should still be assigned
- Clear warnings for missing owners

---

### Test 08: Complex Mixed Scenario
**Purpose**: Integration test combining all features

**Configuration**:
```yaml
default: "data-platform-team"
enableInheritance: true

database:
  "finance_db": "finance-team"                                # Single team
  "marketing_db": ["marketing-user-1", "marketing-user-2"]   # Multiple users

databaseSchema:
  "finance_db.accounting": ["alice", "bob"]    # FQN + users
  "treasury": "treasury-team"                  # Simple name + team

table:
  "finance_db.accounting.revenue": ["charlie", "david", "emma"]  # FQN + 3 users
  "expenses": "expense-team"                                      # Simple name + team
  "finance_db.treasury.cash_flow": "treasury-ops-team"           # FQN + team
```

**Tests Combination Of**:
1. FQN exact match vs simple name fallback
2. Multiple users vs single team
3. Inheritance for unconfigured entities
4. All validation rules

**Expected Results**:
- ✅ finance_db → finance-team (single team, no issues)
- ✅ marketing_db → 2 users (marketing-user-1, marketing-user-2)
- ✅ accounting schema → 2 users (alice, bob) via FQN match
- ✅ treasury schema → treasury-team via simple name (log INFO)
- ✅ revenue → 3 users (charlie, david, emma) via FQN match
- ✅ expenses → expense-team via simple name (log INFO)
- ✅ cash_flow → treasury-ops-team via FQN match
- ✅ investments → inherits from treasury schema

---

## Validation Checklist

### Bug 1: Owner Type Constraints
- [ ] Test 03: Multiple users work correctly
- [ ] Test 04: Multiple teams log WARNING and use first or fallback
- [ ] Test 04: Mixed users+teams log WARNING and skip config
- [ ] Test 08: All type combinations work in complex scenario

### Bug 2: Inheritance Priority
- [ ] Test 05: Child inherits from parent (NOT default) ⭐⭐⭐
- [ ] Test 06: Child uses default when inheritance disabled
- [ ] Test 08: Inheritance works in complex mixed config

### General Functionality
- [ ] Test 01: Basic hierarchical config works
- [ ] Test 02: FQN priority over simple name
- [ ] Test 02: INFO logs for simple name fallback
- [ ] Test 07: Partial success handles missing owners
- [ ] Test 08: All features integrate without conflicts

---

## Common Issues and Solutions

### Issue: Test 05 fails (inheritance not working)
**Symptom**: accounting schema gets "data-platform-team" instead of "finance-team"
**Cause**: Bug 2 not fixed - inheritance priority is wrong
**Solution**: Check that inheritance is checked BEFORE default in `owner_utils.py`

### Issue: Test 04 doesn't log warnings
**Symptom**: Multiple teams assigned without warnings
**Cause**: Bug 1 not fixed - validation missing
**Solution**: Add type validation in `_get_owner_refs()` method

### Issue: INFO logs not appearing for Test 02
**Symptom**: No INFO log for simple name fallback
**Cause**: Log level might be set to WARNING or higher
**Solution**: Ensure `loggerLevel: DEBUG` or `INFO` in workflowConfig

### Issue: Partial success test fails completely
**Symptom**: Ingestion stops when owner not found
**Cause**: Error handling not implemented
**Solution**: Ensure `try-except` blocks catch owner lookup failures

---

## Running Specific Scenarios

### Quick Test (Test 05 only)
```bash
metadata ingest -c ingestion/tests/integration/owner_config_tests/test-05-inheritance-enabled.yaml
```

### Validation Tests (03 and 04)
```bash
for test in test-03-multiple-users test-04-validation-errors; do
    metadata ingest -c ingestion/tests/integration/owner_config_tests/${test}.yaml
done
```

### Critical Bug Tests (05, 06, 08)
```bash
for test in test-05-inheritance-enabled test-06-inheritance-disabled test-08-complex-mixed; do
    metadata ingest -c ingestion/tests/integration/owner_config_tests/${test}.yaml
done
```
