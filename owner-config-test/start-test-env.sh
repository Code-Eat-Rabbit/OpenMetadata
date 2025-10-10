#!/bin/bash

# ============================================
# OpenMetadata Owner Config Test Environment
# Quick Start Script
# ============================================

set -e

echo "========================================"
echo "OpenMetadata Owner Config Test Setup"
echo "========================================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Step 1: Start PostgreSQL
echo -e "${YELLOW}[1/5] Starting PostgreSQL database...${NC}"
docker-compose up -d

echo "Waiting for PostgreSQL to be ready..."
sleep 5

# Check if PostgreSQL is running
if docker-compose ps | grep -q "Up"; then
    echo -e "${GREEN}✓ PostgreSQL is running${NC}"
else
    echo -e "${RED}✗ Failed to start PostgreSQL${NC}"
    exit 1
fi

# Step 2: Verify database connection
echo ""
echo -e "${YELLOW}[2/5] Verifying database connection...${NC}"

if docker exec owner-test-postgres psql -U admin -d finance_db -c "SELECT 1" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ finance_db is accessible${NC}"
else
    echo -e "${RED}✗ Cannot connect to finance_db${NC}"
    exit 1
fi

if docker exec owner-test-postgres psql -U admin -d marketing_db -c "SELECT 1" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ marketing_db is accessible${NC}"
else
    echo -e "${RED}✗ Cannot connect to marketing_db${NC}"
    exit 1
fi

# Step 3: Show database structure
echo ""
echo -e "${YELLOW}[3/5] Database structure:${NC}"
echo ""
echo "finance_db:"
docker exec owner-test-postgres psql -U admin -d finance_db -c "\dn" | grep -E "accounting|treasury" || true
docker exec owner-test-postgres psql -U admin -d finance_db -c "\dt accounting.*" 2>/dev/null | tail -n +3 | head -n 2 || true
docker exec owner-test-postgres psql -U admin -d finance_db -c "\dt treasury.*" 2>/dev/null | tail -n +3 | head -n 2 || true

echo ""
echo "marketing_db:"
docker exec owner-test-postgres psql -U admin -d marketing_db -c "\dn" | grep -E "campaigns|analytics" || true
docker exec owner-test-postgres psql -U admin -d marketing_db -c "\dt campaigns.*" 2>/dev/null | tail -n +3 | head -n 2 || true
docker exec owner-test-postgres psql -U admin -d marketing_db -c "\dt analytics.*" 2>/dev/null | tail -n +3 | head -n 2 || true

# Step 4: Show sample data
echo ""
echo -e "${YELLOW}[4/5] Sample data counts:${NC}"
echo ""
docker exec owner-test-postgres psql -U admin -d finance_db -t -c "
SELECT 
    'accounting.revenue' as table_name, 
    COUNT(*) as row_count 
FROM accounting.revenue
UNION ALL
SELECT 
    'accounting.expenses', 
    COUNT(*) 
FROM accounting.expenses
UNION ALL
SELECT 
    'treasury.cash_flow', 
    COUNT(*) 
FROM treasury.cash_flow
UNION ALL
SELECT 
    'treasury.investments', 
    COUNT(*) 
FROM treasury.investments;
"

docker exec owner-test-postgres psql -U admin -d marketing_db -t -c "
SELECT 
    'campaigns.email_campaigns' as table_name, 
    COUNT(*) as row_count 
FROM campaigns.email_campaigns
UNION ALL
SELECT 
    'campaigns.social_media', 
    COUNT(*) 
FROM campaigns.social_media
UNION ALL
SELECT 
    'analytics.customer_segments', 
    COUNT(*) 
FROM analytics.customer_segments
UNION ALL
SELECT 
    'analytics.conversion_funnel', 
    COUNT(*) 
FROM analytics.conversion_funnel;
"

# Step 5: Next steps
echo ""
echo -e "${YELLOW}[5/5] Setup complete!${NC}"
echo ""
echo "========================================"
echo -e "${GREEN}Next Steps:${NC}"
echo "========================================"
echo ""
echo "1. Update JWT Token in test YAML files:"
echo "   ${YELLOW}find . -name 'test-*.yaml' -exec sed -i 's/YOUR_JWT_TOKEN_HERE/your_actual_token/g' {} \\;${NC}"
echo ""
echo "2. Create Teams in OpenMetadata UI (Settings → Teams):"
echo "   - data-platform-team"
echo "   - finance-team"
echo "   - accounting-team"
echo "   - treasury-team"
echo "   - ... (see README.md for full list)"
echo ""
echo "3. Run individual test:"
echo "   ${YELLOW}cd /workspace/ingestion${NC}"
echo "   ${YELLOW}metadata ingest -c /workspace/owner-config-test/test-01-default-owner-only.yaml${NC}"
echo ""
echo "4. View README for detailed test descriptions:"
echo "   ${YELLOW}cat /workspace/owner-config-test/README.md${NC}"
echo ""
echo "========================================"
echo "Database Info:"
echo "========================================"
echo "Host: localhost"
echo "Port: 5433"
echo "User: admin"
echo "Password: admin123"
echo "Databases: finance_db, marketing_db"
echo ""
echo "To connect manually:"
echo "${YELLOW}docker exec -it owner-test-postgres psql -U admin -d finance_db${NC}"
echo ""
echo "To stop:"
echo "${YELLOW}docker-compose down${NC}"
echo ""
echo "To cleanup completely:"
echo "${YELLOW}docker-compose down -v${NC}"
echo ""
