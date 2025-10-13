#!/bin/bash

# 验证多owner继承修复
# 用于测试 test-03-multiple-users.yaml 的继承是否正确

echo "======================================"
echo "多Owner继承验证脚本"
echo "======================================"
echo ""

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 测试配置
TEST_FILE="ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml"
SERVICE_NAME="postgres-test-03-multiple-users"
JWT_TOKEN="${JWT_TOKEN:-eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKzNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg}"
API_URL="http://localhost:8585/api"

# 检查是否在正确的目录
if [ ! -f "$TEST_FILE" ]; then
    echo -e "${RED}❌ 错误：找不到测试文件 $TEST_FILE${NC}"
    echo "请在 OpenMetadata 根目录运行此脚本"
    exit 1
fi

echo "步骤 1: 运行 ingestion 测试..."
echo "--------------------------------------"
metadata ingest -c "$TEST_FILE"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Ingestion 失败！${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✅ Ingestion 成功${NC}"
echo ""

# 等待数据写入
echo "等待数据写入完成..."
sleep 3

echo ""
echo "步骤 2: 验证 owner 配置..."
echo "--------------------------------------"
echo ""

# 辅助函数：检查 owner 数量
check_owners() {
    local entity_type=$1
    local entity_name=$2
    local expected_count=$3
    local expected_owners=$4
    
    echo "检查 $entity_type: $entity_name"
    
    local url="$API_URL/v1/${entity_type}/name/${SERVICE_NAME}.${entity_name}"
    local response=$(curl -s -X GET "$url" -H "Authorization: Bearer $JWT_TOKEN")
    
    if [ -z "$response" ]; then
        echo -e "  ${RED}❌ API 请求失败${NC}"
        return 1
    fi
    
    # 检查 owner 数量
    local owner_count=$(echo "$response" | jq '.owners | length' 2>/dev/null)
    
    if [ -z "$owner_count" ] || [ "$owner_count" = "null" ]; then
        echo -e "  ${RED}❌ 无法获取 owner 信息${NC}"
        return 1
    fi
    
    # 获取 owner 名字
    local owner_names=$(echo "$response" | jq -r '.owners[].name' 2>/dev/null | tr '\n' ', ' | sed 's/,$//')
    
    if [ "$owner_count" -eq "$expected_count" ]; then
        echo -e "  ${GREEN}✅ Owner 数量正确: $owner_count ($owner_names)${NC}"
        
        # 检查具体的 owner 名字
        if echo "$owner_names" | grep -q "$expected_owners"; then
            echo -e "  ${GREEN}✅ Owner 名字正确${NC}"
            return 0
        else
            echo -e "  ${YELLOW}⚠️  Owner 名字不完全匹配，期望包含: $expected_owners${NC}"
            return 1
        fi
    else
        echo -e "  ${RED}❌ Owner 数量错误: 期望 $expected_count, 实际 $owner_count ($owner_names)${NC}"
        return 1
    fi
}

# 测试结果计数
total_tests=0
passed_tests=0

# Test 1: finance_db 应该有2个owners (alice, bob)
total_tests=$((total_tests + 1))
echo "【测试 1】Database: finance_db"
if check_owners "databases" "finance_db" 2 "alice.*bob"; then
    passed_tests=$((passed_tests + 1))
fi
echo ""

# Test 2: accounting schema 应该继承2个owners (alice, bob)
total_tests=$((total_tests + 1))
echo "【测试 2】Schema: finance_db.accounting (继承)"
if check_owners "databaseSchemas" "finance_db.accounting" 2 "alice.*bob"; then
    passed_tests=$((passed_tests + 1))
    echo -e "  ${GREEN}🎉 多owner继承成功！${NC}"
else
    echo -e "  ${RED}💔 多owner继承失败 - 这是之前的bug${NC}"
fi
echo ""

# Test 3: treasury schema 应该继承2个owners (alice, bob)
total_tests=$((total_tests + 1))
echo "【测试 3】Schema: finance_db.treasury (继承)"
if check_owners "databaseSchemas" "finance_db.treasury" 2 "alice.*bob"; then
    passed_tests=$((passed_tests + 1))
    echo -e "  ${GREEN}🎉 多owner继承成功！${NC}"
else
    echo -e "  ${RED}💔 多owner继承失败${NC}"
fi
echo ""

# Test 4: revenue table 应该有3个owners (charlie, david, emma) - 有配置
total_tests=$((total_tests + 1))
echo "【测试 4】Table: finance_db.accounting.revenue (配置)"
if check_owners "tables" "finance_db.accounting.revenue" 3 "charlie.*david.*emma"; then
    passed_tests=$((passed_tests + 1))
fi
echo ""

# Test 5: expenses table 应该有1个owner (frank) - 有配置
total_tests=$((total_tests + 1))
echo "【测试 5】Table: finance_db.accounting.expenses (配置)"
if check_owners "tables" "finance_db.accounting.expenses" 1 "frank"; then
    passed_tests=$((passed_tests + 1))
fi
echo ""

# Test 6: cash_flow table 应该继承2个owners (alice, bob) from treasury schema
total_tests=$((total_tests + 1))
echo "【测试 6】Table: finance_db.treasury.cash_flow (继承 from schema)"
if check_owners "tables" "finance_db.treasury.cash_flow" 2 "alice.*bob"; then
    passed_tests=$((passed_tests + 1))
    echo -e "  ${GREEN}🎉 Schema→Table 多owner继承成功！${NC}"
else
    echo -e "  ${RED}💔 Schema→Table 多owner继承失败${NC}"
fi
echo ""

# 总结
echo "======================================"
echo "测试结果汇总"
echo "======================================"
echo ""

if [ $passed_tests -eq $total_tests ]; then
    echo -e "${GREEN}✅ 所有测试通过！ ($passed_tests/$total_tests)${NC}"
    echo ""
    echo -e "${GREEN}🎉 多owner继承功能完全正常！${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠️  部分测试失败 ($passed_tests/$total_tests)${NC}"
    echo ""
    
    if [ $passed_tests -ge 4 ]; then
        echo -e "${YELLOW}配置的owners工作正常，但继承功能可能有问题${NC}"
    fi
    
    echo ""
    echo "建议检查："
    echo "1. 确保修改了 common_db_source.py"
    echo "2. 确保 OpenMetadata 服务正在运行"
    echo "3. 查看详细日志了解失败原因"
    exit 1
fi
