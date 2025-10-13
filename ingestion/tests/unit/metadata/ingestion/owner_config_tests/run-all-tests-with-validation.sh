#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# Run all owner configuration tests WITH VALIDATION
# This script not only runs the tests but also verifies the results
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if we're in the correct directory
if [[ ! -f "$SCRIPT_DIR/setup-test-entities.sh" ]]; then
    echo -e "${RED}❌ Error: Script must be run from owner_config_tests directory${NC}"
    exit 1
fi

# Navigate to OpenMetadata root
cd "$SCRIPT_DIR/../../../../../.."
WORKSPACE_ROOT="$(pwd)"

echo "=========================================="
echo "Owner Config Tests - With Validation"
echo "=========================================="
echo "Workspace: $WORKSPACE_ROOT"
echo ""

# Check requirements
if ! command -v metadata &> /dev/null; then
    echo -e "${RED}❌ Error: 'metadata' command not found${NC}"
    exit 1
fi

if ! command -v curl &> /dev/null; then
    echo -e "${RED}❌ Error: 'curl' command not found (needed for validation)${NC}"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo -e "${YELLOW}⚠️  Warning: 'jq' not found. API validation will be limited.${NC}"
    HAS_JQ=false
else
    HAS_JQ=true
fi

# API configuration
API_URL="${OPENMETADATA_URL:-http://localhost:8585/api}"
JWT_TOKEN="${JWT_TOKEN:-eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKzNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg}"

echo "API URL: $API_URL"
echo ""

# Validation function
validate_owners() {
    local entity_type=$1
    local entity_name=$2
    local expected_count=$3
    local service_name=$4
    
    local url="$API_URL/v1/${entity_type}/name/${service_name}.${entity_name}"
    
    # Fetch entity
    local response=$(curl -s -X GET "$url" -H "Authorization: Bearer $JWT_TOKEN" 2>/dev/null)
    
    if [ -z "$response" ]; then
        echo -e "       ${RED}✗${NC} API request failed for $entity_name"
        return 1
    fi
    
    # Check if jq is available
    if [ "$HAS_JQ" = true ]; then
        local owner_count=$(echo "$response" | jq '.owners | length' 2>/dev/null)
        local owner_names=$(echo "$response" | jq -r '.owners[].name' 2>/dev/null | tr '\n' ', ' | sed 's/,$//')
        
        if [ -z "$owner_count" ] || [ "$owner_count" = "null" ]; then
            echo -e "       ${YELLOW}⚠${NC} Could not get owner count for $entity_name"
            return 1
        fi
        
        if [ "$owner_count" -eq "$expected_count" ]; then
            echo -e "       ${GREEN}✓${NC} $entity_name: $owner_count owners ($owner_names)"
            return 0
        else
            echo -e "       ${RED}✗${NC} $entity_name: Expected $expected_count owners, got $owner_count ($owner_names)"
            return 1
        fi
    else
        # Without jq, just check if response contains "owners"
        if echo "$response" | grep -q '"owners"'; then
            echo -e "       ${YELLOW}?${NC} $entity_name: Has owners (cannot verify count without jq)"
            return 0
        else
            echo -e "       ${RED}✗${NC} $entity_name: No owners found"
            return 1
        fi
    fi
}

# Test configurations
declare -A TEST_VALIDATIONS

# Test 3: Multiple users - verify inheritance
TEST_VALIDATIONS["test-03-multiple-users.yaml"]="postgres-test-03-multiple-users:databaseSchemas:finance_db.accounting:2"

# Test 5: Inheritance enabled - critical test
TEST_VALIDATIONS["test-05-inheritance-enabled.yaml"]="postgres-test-05-inheritance-on:databaseSchemas:finance_db.accounting:1:tables:finance_db.accounting.revenue:1"

# Test counters
PASSED=0
FAILED=0
VALIDATION_PASSED=0
VALIDATION_FAILED=0
FAILED_TESTS=()

# Find all test files
TEST_FILES=($SCRIPT_DIR/test-*.yaml)
TOTAL_TESTS=${#TEST_FILES[@]}

echo "Found $TOTAL_TESTS test files"
echo ""

# Run each test
for i in "${!TEST_FILES[@]}"; do
    TEST_FILE="${TEST_FILES[$i]}"
    TEST_NAME=$(basename "$TEST_FILE")
    TEST_NUM=$((i + 1))
    
    REL_PATH="ingestion/tests/unit/metadata/ingestion/owner_config_tests/$TEST_NAME"
    
    echo -e "${BLUE}[$TEST_NUM/$TOTAL_TESTS]${NC} Running: ${TEST_NAME}"
    
    # Run ingestion
    if metadata ingest -c "$REL_PATH" > /tmp/test_output_$$.log 2>&1; then
        echo -e "       ${GREEN}✓${NC} Ingestion completed"
        ((PASSED++))
        
        # Wait for data to be written
        sleep 2
        
        # Run validation if configured
        if [ -n "${TEST_VALIDATIONS[$TEST_NAME]}" ]; then
            echo -e "       ${BLUE}Validating results...${NC}"
            
            # Parse validation config
            IFS=':' read -ra VALIDATE <<< "${TEST_VALIDATIONS[$TEST_NAME]}"
            SERVICE_NAME="${VALIDATE[0]}"
            
            VALIDATION_SUCCESS=true
            
            # Validate each entity
            for ((j=1; j<${#VALIDATE[@]}; j+=3)); do
                ENTITY_TYPE="${VALIDATE[$j]}"
                ENTITY_NAME="${VALIDATE[$j+1]}"
                EXPECTED_COUNT="${VALIDATE[$j+2]}"
                
                if ! validate_owners "$ENTITY_TYPE" "$ENTITY_NAME" "$EXPECTED_COUNT" "$SERVICE_NAME"; then
                    VALIDATION_SUCCESS=false
                fi
            done
            
            if [ "$VALIDATION_SUCCESS" = true ]; then
                ((VALIDATION_PASSED++))
            else
                ((VALIDATION_FAILED++))
                FAILED_TESTS+=("$TEST_NAME (validation failed)")
            fi
        else
            echo -e "       ${YELLOW}⚠${NC} No validation configured for this test"
        fi
    else
        echo -e "       ${RED}✗${NC} Ingestion failed"
        ((FAILED++))
        FAILED_TESTS+=("$TEST_NAME (ingestion failed)")
        
        # Show last few lines of error
        echo -e "${YELLOW}       Last error lines:${NC}"
        tail -3 /tmp/test_output_$$.log | sed 's/^/       /'
    fi
    
    # Clean up temp log
    rm -f /tmp/test_output_$$.log
    echo ""
done

# Print summary
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo "Total:              $TOTAL_TESTS"
echo -e "Ingestion Passed:   ${GREEN}${PASSED}${NC}"
echo -e "Validation Passed:  ${GREEN}${VALIDATION_PASSED}${NC}"

if [ $FAILED -gt 0 ] || [ $VALIDATION_FAILED -gt 0 ]; then
    echo -e "Ingestion Failed:   ${RED}${FAILED}${NC}"
    echo -e "Validation Failed:  ${RED}${VALIDATION_FAILED}${NC}"
fi
echo ""

# List failed tests if any
if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
    echo -e "${RED}Failed tests:${NC}"
    for test in "${FAILED_TESTS[@]}"; do
        echo "  - $test"
    done
    echo ""
    echo -e "${YELLOW}⚠ Some tests failed. Check the output above for details.${NC}"
    exit 1
else
    echo -e "${GREEN}✅ All tests passed with validation!${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Verify results in OpenMetadata UI (http://localhost:8585)"
    echo "  2. Add more validations to TEST_VALIDATIONS array"
    exit 0
fi
