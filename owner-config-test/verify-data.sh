#!/bin/bash

# ============================================
# Verify Test Database Data
# ============================================

echo "========================================"
echo "Verifying Test Database Structure"
echo "========================================"
echo ""

# Finance DB
echo "📊 FINANCE_DB Structure:"
echo ""
echo "Schemas:"
docker exec owner-test-postgres psql -U admin -d finance_db -c "
SELECT 
    schema_name,
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = schema_name) as table_count
FROM information_schema.schemata 
WHERE schema_name IN ('accounting', 'treasury')
ORDER BY schema_name;
"

echo ""
echo "Tables with row counts:"
docker exec owner-test-postgres psql -U admin -d finance_db -c "
SELECT 
    schemaname || '.' || tablename as full_table_name,
    (SELECT COUNT(*) FROM accounting.revenue) as row_count
FROM pg_tables 
WHERE schemaname = 'accounting' AND tablename = 'revenue'
UNION ALL
SELECT 
    schemaname || '.' || tablename,
    (SELECT COUNT(*) FROM accounting.expenses)
FROM pg_tables 
WHERE schemaname = 'accounting' AND tablename = 'expenses'
UNION ALL
SELECT 
    schemaname || '.' || tablename,
    (SELECT COUNT(*) FROM treasury.cash_flow)
FROM pg_tables 
WHERE schemaname = 'treasury' AND tablename = 'cash_flow'
UNION ALL
SELECT 
    schemaname || '.' || tablename,
    (SELECT COUNT(*) FROM treasury.investments)
FROM pg_tables 
WHERE schemaname = 'treasury' AND tablename = 'investments'
ORDER BY full_table_name;
"

echo ""
echo "========================================"
echo ""

# Marketing DB
echo "📊 MARKETING_DB Structure:"
echo ""
echo "Schemas:"
docker exec owner-test-postgres psql -U admin -d marketing_db -c "
SELECT 
    schema_name,
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = schema_name) as table_count
FROM information_schema.schemata 
WHERE schema_name IN ('campaigns', 'analytics')
ORDER BY schema_name;
"

echo ""
echo "Tables with row counts:"
docker exec owner-test-postgres psql -U admin -d marketing_db -c "
SELECT 
    schemaname || '.' || tablename as full_table_name,
    (SELECT COUNT(*) FROM campaigns.email_campaigns) as row_count
FROM pg_tables 
WHERE schemaname = 'campaigns' AND tablename = 'email_campaigns'
UNION ALL
SELECT 
    schemaname || '.' || tablename,
    (SELECT COUNT(*) FROM campaigns.social_media)
FROM pg_tables 
WHERE schemaname = 'campaigns' AND tablename = 'social_media'
UNION ALL
SELECT 
    schemaname || '.' || tablename,
    (SELECT COUNT(*) FROM analytics.customer_segments)
FROM pg_tables 
WHERE schemaname = 'analytics' AND tablename = 'customer_segments'
UNION ALL
SELECT 
    schemaname || '.' || tablename,
    (SELECT COUNT(*) FROM analytics.conversion_funnel)
FROM pg_tables 
WHERE schemaname = 'analytics' AND tablename = 'conversion_funnel'
ORDER BY full_table_name;
"

echo ""
echo "========================================"
echo "✓ Verification Complete"
echo "========================================"
echo ""
echo "Total: 2 databases, 4 schemas, 8 tables"
echo ""
