#!/bin/bash

# 多Owner继承修复 - 完整运行和验证脚本

set -e  # 遇到错误立即退出

echo "======================================"
echo "多Owner继承修复 - 运行和验证"
echo "======================================"
echo ""

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 检查工作目录
if [ ! -d "ingestion" ]; then
    echo -e "${RED}❌ 请在 OpenMetadata 根目录运行此脚本${NC}"
    exit 1
fi

echo -e "${BLUE}步骤 1: 清除 Python 缓存${NC}"
echo "--------------------------------------"

# 清除 .pyc 文件
find ingestion/src -type f -name "*.pyc" -delete 2>/dev/null || true
find ingestion/src -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

echo -e "${GREEN}✅ Python 缓存已清除${NC}"
echo ""

echo -e "${BLUE}步骤 2: 验证代码修改${NC}"
echo "--------------------------------------"

# 检查关键修改
if grep -q "database_owner_names = \[owner.name for owner in database_owner_ref.root\]" ingestion/src/metadata/ingestion/source/database/common_db_source.py; then
    echo -e "${GREEN}✅ common_db_source.py 修改正确${NC}"
else
    echo -e "${RED}❌ common_db_source.py 修改不正确${NC}"
    exit 1
fi

# 检查 parent_owner 类型声明（应该有2处）
PARENT_OWNER_COUNT=$(grep -c "parent_owner: Optional\[Union\[str, List\[str\]\]\]" ingestion/src/metadata/utils/owner_utils.py || true)
if [ "$PARENT_OWNER_COUNT" -ge 2 ]; then
    echo -e "${GREEN}✅ owner_utils.py 类型声明正确（找到 $PARENT_OWNER_COUNT 处）${NC}"
else
    echo -e "${RED}❌ owner_utils.py 类型声明不正确（只找到 $PARENT_OWNER_COUNT 处，应该至少2处）${NC}"
    echo "实际内容："
    grep -n "parent_owner: Optional" ingestion/src/metadata/utils/owner_utils.py || true
    exit 1
fi

echo ""

echo -e "${BLUE}步骤 3: 运行 Test 03 (Multiple Users)${NC}"
echo "--------------------------------------"

TEST_FILE="ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml"
LOG_FILE="/tmp/test-03-debug.log"

if [ ! -f "$TEST_FILE" ]; then
    echo -e "${RED}❌ 找不到测试文件: $TEST_FILE${NC}"
    exit 1
fi

echo "运行 ingestion (带DEBUG日志)..."
echo "日志文件: $LOG_FILE"
echo ""

# 运行 ingestion
metadata ingest -c "$TEST_FILE" 2>&1 | tee "$LOG_FILE"

if [ $? -ne 0 ]; then
    echo ""
    echo -e "${RED}❌ Ingestion 失败！${NC}"
    echo "请检查日志: $LOG_FILE"
    exit 1
fi

echo ""
echo -e "${GREEN}✅ Ingestion 完成${NC}"
echo ""

echo -e "${BLUE}步骤 4: 分析日志${NC}"
echo "--------------------------------------"

echo "【4.1】检查 Database owner 解析："
if grep -q "finance_db.*alice.*bob" "$LOG_FILE"; then
    echo -e "${GREEN}✅ Database 配置了2个owners (alice, bob)${NC}"
else
    echo -e "${YELLOW}⚠️  Database owners 信息未在日志中找到${NC}"
fi

echo ""
echo "【4.2】检查继承日志："
INHERIT_LOGS=$(grep -i "inherited owner" "$LOG_FILE" | head -5)

if [ -z "$INHERIT_LOGS" ]; then
    echo -e "${YELLOW}⚠️  未找到继承相关日志${NC}"
else
    echo "找到继承日志："
    echo "$INHERIT_LOGS" | while read line; do
        # 检查是否包含列表
        if echo "$line" | grep -q "\['alice', 'bob'\]"; then
            echo -e "${GREEN}  ✅ $line${NC}"
        elif echo "$line" | grep -q "alice.*bob"; then
            echo -e "${GREEN}  ✅ $line${NC}"
        else
            echo -e "${YELLOW}  ⚠️  $line${NC}"
        fi
    done
fi

echo ""

echo -e "${BLUE}步骤 5: 验证 API 结果${NC}"
echo "--------------------------------------"

# 检查环境变量
if [ -z "$JWT_TOKEN" ]; then
    echo -e "${YELLOW}⚠️  JWT_TOKEN 环境变量未设置${NC}"
    echo "使用默认 token（仅本地开发环境）"
    JWT_TOKEN="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKzNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
fi

API_URL="http://localhost:8585/api"
SERVICE_NAME="postgres-test-03-multiple-users"

# 等待数据写入
echo "等待数据写入完成（3秒）..."
sleep 3
echo ""

# 函数：检查 entity 的 owners
check_entity_owners() {
    local entity_type=$1
    local entity_name=$2
    local expected_count=$3
    
    local url="$API_URL/v1/${entity_type}/name/${SERVICE_NAME}.${entity_name}"
    
    echo "【检查】$entity_type: $entity_name"
    
    # 发送请求
    local response=$(curl -s -X GET "$url" -H "Authorization: Bearer $JWT_TOKEN" 2>/dev/null)
    
    if [ -z "$response" ] || echo "$response" | grep -q "error"; then
        echo -e "${RED}  ❌ API 请求失败或实体不存在${NC}"
        echo "  URL: $url"
        return 1
    fi
    
    # 检查是否有 jq
    if ! command -v jq &> /dev/null; then
        echo -e "${YELLOW}  ⚠️  jq 未安装，无法解析 JSON${NC}"
        echo "  响应: $(echo "$response" | head -c 200)..."
        return 1
    fi
    
    # 解析 owners
    local owner_count=$(echo "$response" | jq '.owners | length' 2>/dev/null)
    local owner_names=$(echo "$response" | jq -r '.owners[].name' 2>/dev/null | tr '\n' ', ' | sed 's/,$//')
    
    if [ -z "$owner_count" ] || [ "$owner_count" = "null" ]; then
        echo -e "${YELLOW}  ⚠️  无法获取 owner 信息${NC}"
        return 1
    fi
    
    echo "  Owner数量: $owner_count"
    echo "  Owner名字: $owner_names"
    
    if [ "$owner_count" -eq "$expected_count" ]; then
        echo -e "${GREEN}  ✅ Owner 数量正确！${NC}"
        return 0
    else
        echo -e "${RED}  ❌ Owner 数量错误（期望: $expected_count, 实际: $owner_count）${NC}"
        return 1
    fi
}

# 测试计数
total=0
passed=0

# Test 5.1: finance_db (应该有2个owners)
total=$((total + 1))
if check_entity_owners "databases" "finance_db" 2; then
    passed=$((passed + 1))
fi
echo ""

# Test 5.2: accounting schema (继承，应该有2个owners)
total=$((total + 1))
if check_entity_owners "databaseSchemas" "finance_db.accounting" 2; then
    passed=$((passed + 1))
    echo -e "${GREEN}  🎉 多owner继承成功！${NC}"
else
    echo -e "${RED}  💔 多owner继承失败 - 这是问题所在${NC}"
fi
echo ""

# Test 5.3: treasury schema (继承，应该有2个owners)
total=$((total + 1))
if check_entity_owners "databaseSchemas" "finance_db.treasury" 2; then
    passed=$((passed + 1))
fi
echo ""

echo "======================================"
echo "验证结果"
echo "======================================"

if [ $passed -eq $total ]; then
    echo -e "${GREEN}✅ 所有验证通过！ ($passed/$total)${NC}"
    echo ""
    echo -e "${GREEN}🎉 多owner继承功能完全正常！${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠️  部分验证失败 ($passed/$total)${NC}"
    echo ""
    
    if [ $passed -eq 1 ]; then
        echo -e "${RED}问题：Schema 继承失败${NC}"
        echo ""
        echo "可能原因："
        echo "1. 查看日志中的继承信息："
        echo "   grep -i 'inherited' $LOG_FILE"
        echo ""
        echo "2. 检查是否真的传递了列表："
        echo "   grep -C 3 'accounting' $LOG_FILE | grep -i parent"
        echo ""
        echo "3. 添加调试输出（见 CHECK_MULTI_OWNER_ISSUE.md 的深度调试部分）"
    fi
    
    exit 1
fi
