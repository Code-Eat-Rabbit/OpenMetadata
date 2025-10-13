# 测试验证指南

## 🎯 问题分析

### 原始脚本的问题

`run-all-tests.sh` 只检查 `metadata ingest` 的退出码：

```bash
if metadata ingest -c "$REL_PATH" > /tmp/test_output_$$.log 2>&1; then
    echo "✓ Test completed successfully"  # ← 只要没报错就算成功
```

**问题**：即使owner配置错误（继承失败、多owner丢失），只要ingestion运行完成，就显示"成功"。

### 为什么会这样？

`metadata ingest` 命令在以下情况下**不会**返回错误码：
1. Owner查找失败（只打印WARNING）
2. Owner继承不工作（静默失败）
3. 多owner只保留了一个（没有验证机制）
4. Owner配置被忽略（使用了default）

## ✅ 解决方案

### 方案1: 使用增强版脚本（推荐）

新脚本 `run-all-tests-with-validation.sh` 会：
1. 运行 ingestion
2. **调用 API 验证实际结果**
3. 检查 owner 数量和名称

#### 使用方法

```bash
cd ~/workspaces/OpenMetadata/ingestion/tests/unit/metadata/ingestion/owner_config_tests

# 运行带验证的脚本
./run-all-tests-with-validation.sh
```

#### 添加验证规则

编辑脚本中的 `TEST_VALIDATIONS` 数组：

```bash
# 格式: "测试文件"="service_name:entity_type:entity_name:expected_count:..."
TEST_VALIDATIONS["test-03-multiple-users.yaml"]="postgres-test-03-multiple-users:databaseSchemas:finance_db.accounting:2"
```

**示例**:
```bash
# Test 3: 验证 accounting schema 有2个owners
TEST_VALIDATIONS["test-03-multiple-users.yaml"]="postgres-test-03-multiple-users:databaseSchemas:finance_db.accounting:2"

# Test 5: 验证继承（schema和table都应该有finance-team）
TEST_VALIDATIONS["test-05-inheritance-enabled.yaml"]="postgres-test-05-inheritance-on:databaseSchemas:finance_db.accounting:1:tables:finance_db.accounting.revenue:1"

# Test 8: 验证多个实体
TEST_VALIDATIONS["test-08-complex-mixed.yaml"]="postgres-test-08-complex:databaseSchemas:finance_db.accounting:2:tables:finance_db.accounting.revenue:3"
```

---

### 方案2: 修改原始脚本

如果要修改 `run-all-tests.sh`，添加日志检查：

```bash
# 在第79行后添加
if metadata ingest -c "$REL_PATH" > /tmp/test_output_$$.log 2>&1; then
    # 检查日志中的WARNING
    WARNING_COUNT=$(grep -c "Could not find owner\|VALIDATION ERROR" /tmp/test_output_$$.log || true)
    
    if [ $WARNING_COUNT -gt 0 ]; then
        echo -e "       ${YELLOW}⚠${NC} Test completed with $WARNING_COUNT warnings"
        echo -e "${YELLOW}       Check validation warnings:${NC}"
        grep "Could not find owner\|VALIDATION ERROR" /tmp/test_output_$$.log | head -3 | sed 's/^/       /'
    else
        echo -e "       ${GREEN}✓${NC} Test completed successfully"
    fi
    ((PASSED++))
else
    # ... 错误处理
fi
```

---

### 方案3: 手动验证

运行测试后，手动检查结果：

```bash
# 设置环境变量
export JWT_TOKEN="your_token"

# 验证 Test 3 - accounting schema 应该有2个owners
curl -s "http://localhost:8585/api/v1/databaseSchemas/name/postgres-test-03-multiple-users.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners | length'

# 期望输出: 2

# 验证 Test 5 - accounting schema 应该继承 finance-team
curl -s "http://localhost:8585/api/v1/databaseSchemas/name/postgres-test-05-inheritance-on.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners[].name'

# 期望输出: "finance-team"（不是 "data-platform-team"）
```

---

## 📊 完整验证清单

### Test 1: Basic Configuration
```bash
# finance_db → data-platform-team
curl -s "$API/v1/databases/name/postgres-test-01-basic.finance_db" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners[].name'
# 期望: "data-platform-team"
```

### Test 2: FQN Matching
```bash
# treasury schema → treasury-team (FQN match)
curl -s "$API/v1/databaseSchemas/name/postgres-test-02-fqn.finance_db.treasury" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners[].name'
# 期望: "treasury-team"
```

### Test 3: Multiple Users ⭐
```bash
# accounting schema → ["alice", "bob"] (2个owners)
curl -s "$API/v1/databaseSchemas/name/postgres-test-03-multiple-users.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners | length'
# 期望: 2

curl -s "$API/v1/databaseSchemas/name/postgres-test-03-multiple-users.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners[].name'
# 期望: "alice", "bob"
```

### Test 5: Inheritance Enabled ⭐
```bash
# accounting schema → "finance-team" (继承自database)
curl -s "$API/v1/databaseSchemas/name/postgres-test-05-inheritance-on.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners[].name'
# 期望: "finance-team"（不是 "data-platform-team"）

# revenue table → "finance-team" (继承自schema)
curl -s "$API/v1/tables/name/postgres-test-05-inheritance-on.finance_db.accounting.revenue" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners[].name'
# 期望: "finance-team"
```

### Test 8: Complex Mixed
```bash
# accounting schema → ["alice", "bob"]
curl -s "$API/v1/databaseSchemas/name/postgres-test-08-complex.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners | length'
# 期望: 2
```

---

## 🔧 创建自动验证脚本

创建一个简单的验证脚本：

```bash
#!/bin/bash
# verify-test-results.sh

API="http://localhost:8585/api"
TOKEN="${JWT_TOKEN:-default_token}"

echo "验证 Test 3: Multiple Users"
COUNT=$(curl -s "$API/v1/databaseSchemas/name/postgres-test-03-multiple-users.finance_db.accounting" \
  -H "Authorization: Bearer $TOKEN" | jq '.owners | length')

if [ "$COUNT" -eq 2 ]; then
    echo "✅ Test 3: accounting schema 有2个owners"
else
    echo "❌ Test 3: 期望2个owners，实际$COUNT个"
fi

echo ""
echo "验证 Test 5: Inheritance"
OWNER=$(curl -s "$API/v1/databaseSchemas/name/postgres-test-05-inheritance-on.finance_db.accounting" \
  -H "Authorization: Bearer $TOKEN" | jq -r '.owners[0].name')

if [ "$OWNER" = "finance-team" ]; then
    echo "✅ Test 5: 继承正常工作"
else
    echo "❌ Test 5: 期望finance-team，实际$OWNER"
fi
```

---

## 🎯 推荐做法

1. **使用增强版脚本**:
   ```bash
   ./run-all-tests-with-validation.sh
   ```

2. **为关键测试添加验证规则**:
   - Test 3: 多owner
   - Test 5: 继承
   - Test 8: 复杂场景

3. **手动验证重要测试**:
   ```bash
   # 运行测试后
   ./verify-test-results.sh
   ```

4. **查看日志中的WARNING**:
   ```bash
   metadata ingest -c test-03.yaml 2>&1 | grep -i "warning\|error\|validation"
   ```

这样才能确保测试真正成功！
