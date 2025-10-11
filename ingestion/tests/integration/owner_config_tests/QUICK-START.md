# Owner Configuration Tests - Quick Start Guide

## 🚀 Get Started in 5 Minutes

This guide helps you quickly set up and run the owner configuration tests.

---

## Prerequisites

- Docker and Docker Compose installed
- OpenMetadata instance running on `http://localhost:8585`
- Admin access to OpenMetadata

---

## Step 1: Start PostgreSQL Test Database

```bash
cd /workspace/ingestion/tests/integration/owner_config_tests
docker-compose up -d
```

**What this does**: Creates 2 test databases (finance_db, marketing_db) with sample data.

**Verify it's running**:
```bash
docker ps | grep postgres
```

---

## Step 2: Create Test Users and Teams in OpenMetadata

### Option A: Using OpenMetadata UI

1. Go to `http://localhost:8585`
2. Navigate to **Settings → Users** and create:
   - alice, bob, charlie, david, emma, frank
   - marketing-user-1, marketing-user-2

3. Navigate to **Settings → Teams** and create:
   - data-platform-team
   - finance-team, marketing-team
   - accounting-team, treasury-team, expense-team
   - revenue-team, investment-team, treasury-ops-team
   - audit-team, compliance-team

### Option B: Using API (Faster)

```bash
# Get JWT token
JWT_TOKEN="your_jwt_token_here"

# Create users
for user in alice bob charlie david emma frank marketing-user-1 marketing-user-2; do
  curl -X POST "http://localhost:8585/api/v1/users" \
    -H "Authorization: Bearer $JWT_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"$user\", \"email\": \"${user}@example.com\"}"
done

# Create teams
for team in data-platform-team finance-team marketing-team accounting-team treasury-team expense-team revenue-team investment-team treasury-ops-team audit-team compliance-team; do
  curl -X POST "http://localhost:8585/api/v1/teams" \
    -H "Authorization: Bearer $JWT_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"$team\", \"teamType\": \"Group\"}"
done
```

---

## Step 3: Get Your JWT Token

1. Log in to OpenMetadata UI
2. Go to **Settings → Bots** or **Settings → Personal Access Token**
3. Create a new token or copy existing one
4. Save it for the next step

---

## Step 4: Update Test Configurations

Edit the JWT token in test files:

```bash
cd /workspace/ingestion/tests/integration/owner_config_tests

# Replace JWT_TOKEN in all test files
for test in test-*.yaml; do
  sed -i 's/YOUR_JWT_TOKEN_HERE/your_actual_jwt_token_here/g' "$test"
done
```

Or manually edit each file and replace:
```yaml
jwtToken: "YOUR_JWT_TOKEN_HERE"
```

---

## Step 5: Run Tests

### Run Critical Tests First

```bash
cd /workspace/ingestion

# Test inheritance (Bug 2 verification)
metadata ingest -c tests/integration/owner_config_tests/test-05-inheritance-enabled.yaml

# Test validation (Bug 1 verification)
metadata ingest -c tests/integration/owner_config_tests/test-04-validation-errors.yaml
```

### Run All Tests

```bash
cd /workspace/ingestion

for test in tests/integration/owner_config_tests/test-*.yaml; do
    echo ""
    echo "=========================================="
    echo "Running: $(basename $test)"
    echo "=========================================="
    metadata ingest -c "$test"
done
```

---

## Step 6: Verify Results

### Check Test 05 (Inheritance) ⭐ Critical

**Expected behavior**: Child entities should inherit owner from parent, NOT use default.

**Verify in OpenMetadata UI**:
1. Go to **Explore → Databases**
2. Find `finance_db` database → should have owner "finance-team"
3. Click into `accounting` schema → should have owner "finance-team" (inherited)
4. Click into `revenue` table → should have owner "finance-team" (inherited)

**If "data-platform-team" appears instead**: Bug 2 is NOT fixed ❌

---

### Check Test 04 (Validation) ⭐ Critical

**Expected behavior**: System should log warnings for invalid owner configurations.

**Check logs**:
```bash
# Look for validation warnings
grep -i "VALIDATION ERROR" logs/*

# Expected messages:
# - "Only ONE team allowed as owner, but got 3 teams"
# - "Cannot mix users and teams in owner list"
```

**If no warnings appear**: Bug 1 is NOT fixed ❌

---

### Check Test 03 (Multiple Users)

**Verify in OpenMetadata UI**:
1. Go to `finance_db.accounting.revenue` table
2. Check **Owners** section → should show 3 users (charlie, david, emma)

**Expected**: All 3 users assigned successfully ✅

---

## Troubleshooting

### Issue: "Could not find owner"
**Solution**: Make sure all users/teams are created in Step 2.

### Issue: "Authentication failed"
**Solution**: Check your JWT token is valid and not expired.

### Issue: "Connection refused to PostgreSQL"
**Solution**: Verify PostgreSQL is running: `docker ps | grep postgres`

### Issue: Owner not assigned
**Solution**: 
- Verify `overrideMetadata: true` is set in YAML
- Check logs for validation errors
- Ensure owner exists in OpenMetadata

---

## Quick Test Matrix

| Test # | Focus | Expected Result | Critical |
|--------|-------|-----------------|----------|
| 01 | Basic config | All levels assigned correctly | - |
| 02 | FQN matching | INFO logs for simple name fallback | - |
| 03 | Multiple users | 3 users assigned to revenue table | ✅ Bug 1 |
| 04 | Validation | WARNING logs for invalid configs | ✅ Bug 1 |
| 05 | Inheritance ON | Child inherits parent owner | ✅ Bug 2 |
| 06 | Inheritance OFF | Child uses default (not inherited) | ✅ Bug 2 |
| 07 | Partial success | Skip missing owners, continue | - |
| 08 | Integration | All features work together | ✅ Both |

---

## Cleanup

When done testing:

```bash
# Stop and remove PostgreSQL
cd /workspace/ingestion/tests/integration/owner_config_tests
docker-compose down -v

# Remove test entities from OpenMetadata (optional)
# Use UI or API to delete test users and teams
```

---

## Next Steps

- **For detailed information**: See `README.md` in this directory
- **For code details**: Check `ingestion/src/metadata/utils/owner_utils.py`
- **For schema spec**: Check `openmetadata-spec/.../type/ownerConfig.json`

---

## Quick Reference: Business Rules

✅ **Multiple users allowed**: `["alice", "bob", "charlie"]`  
✅ **Only ONE team allowed**: `"sales-team"` (string, not array)  
❌ **Users and teams mutually exclusive**: Cannot mix `["alice", "team1"]`

**Priority**: Specific Config > Inherited Owner > Default
