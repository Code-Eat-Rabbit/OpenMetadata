# Owner Config Tests - 故障排查指南

## 🔍 针对 Test 3、4、7、8 报错的排查

如果这些测试失败，请按照以下步骤排查：

### 步骤 1: 查看具体错误信息

```bash
# 从 OpenMetadata 根目录运行单个测试，查看完整错误
cd ~/path/to/OpenMetadata

# Test-03
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml 2>&1 | tee test-03-error.log

# Test-04
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-04-validation-errors.yaml 2>&1 | tee test-04-error.log

# Test-07
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-07-partial-success.yaml 2>&1 | tee test-07-error.log

# Test-08
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-08-complex-mixed.yaml 2>&1 | tee test-08-error.log
```

### 步骤 2: 检查常见问题

#### 问题 1: 用户或团队不存在

**症状**：
```
WARNING: Could not find owner: alice
WARNING: Could not find owner: finance-team
```

**原因**：测试所需的用户/团队未创建

**解决**：
```bash
# 确保运行了setup脚本
cd ingestion/tests/unit/metadata/ingestion/owner_config_tests
export OPENMETADATA_JWT_TOKEN="your_token"
./setup-test-entities.sh
```

**Test-03 需要的用户**：
- alice, bob, charlie, david, emma, frank ✓

**Test-04 需要的团队**：
- finance-team, audit-team, compliance-team, expense-team ✓

**Test-07 需要的用户**（部分不存在是预期的）：
- alice, bob, charlie, david ✓
- nonexistent-user-1, nonexistent-user-2 ❌ （预期不存在）

**Test-08 需要的用户和团队**：
- 用户：alice, bob, charlie, david, emma, marketing-user-1, marketing-user-2 ✓
- 团队：finance-team, treasury-team, expense-team, treasury-ops-team ✓

#### 问题 2: 数据库连接失败

**症状**：
```
Error: Connection refused
Error: database "finance_db" does not exist
```

**解决**：
```bash
# 检查 PostgreSQL 是否运行
cd ingestion/tests/unit/metadata/ingestion/owner_config_tests
docker ps | grep postgres

# 如果没有运行，启动它
docker-compose up -d

# 验证数据库已创建
docker-compose exec postgres psql -U admin -c "\l"
```

#### 问题 3: JWT Token 无效或未更新

**症状**：
```
Error: Unauthorized
Error: 401 Authentication failed
```

**解决**：
```bash
# 更新所有测试文件中的 JWT Token
cd ingestion/tests/unit/metadata/ingestion/owner_config_tests

# macOS
for test in test-*.yaml; do
  sed -i '' 's/YOUR_JWT_TOKEN_HERE/your_actual_jwt_token/g' "$test"
done

# Linux
for test in test-*.yaml; do
  sed -i 's/YOUR_JWT_TOKEN_HERE/your_actual_jwt_token/g' "$test"
done
```

#### 问题 4: metadata 命令未找到

**症状**：
```
bash: metadata: command not found
```

**解决**：
```bash
# 激活虚拟环境
cd ~/path/to/OpenMetadata
source env/bin/activate

# 安装 OpenMetadata ingestion
cd ingestion
pip install -e '.[postgres]'
```

### 步骤 3: 特定测试的预期行为

#### Test-03: Multiple Users (应该成功 ✅)

- **目的**：测试多个用户作为owners
- **预期**：全部成功，无错误
- **如果失败**：检查alice, bob, charlie, david, emma, frank是否存在

#### Test-04: Validation Errors (应该成功但有WARNING ⚠️)

- **目的**：测试验证错误处理
- **预期行为**：
  ```
  WARNING: Only ONE team allowed, using first team: finance-team
  WARNING: Cannot mix users and teams in owner list. Skipping this owner configuration.
  ```
- **结果**：ingestion应该**成功完成**（退出码 0），但有WARNING日志
- **如果失败**：
  - 检查是否所有teams存在（finance-team, audit-team, compliance-team）
  - 检查是否所有users存在（alice, bob）

#### Test-07: Partial Success (应该成功但有WARNING ⚠️)

- **目的**：测试部分owner不存在时的容错
- **预期行为**：
  ```
  WARNING: Could not find owner: nonexistent-user-1
  WARNING: Could not find owner: nonexistent-user-2
  ```
- **结果**：ingestion应该**成功完成**，跳过不存在的owners
- **如果失败**：
  - 检查alice, bob, charlie, david是否存在
  - 确认nonexistent-user-1和nonexistent-user-2确实不存在（这是预期的）

#### Test-08: Complex Mixed (应该成功 ✅)

- **目的**：综合测试所有特性
- **预期**：全部成功，可能有简单名称匹配的INFO日志
- **如果失败**：
  - 检查所有用户和团队是否存在
  - 检查finance_db的所有schema和table是否存在

### 步骤 4: 使用 DEBUG 日志排查

```bash
# 运行测试并开启 DEBUG 日志
metadata ingest \
  -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml \
  --log-level DEBUG 2>&1 | tee debug.log

# 搜索关键信息
grep -i "owner" debug.log | grep -E "WARNING|ERROR"
grep -i "resolving owner" debug.log
grep -i "validation" debug.log
```

### 步骤 5: 验证 OpenMetadata 连接

```bash
# 测试 API 连接
JWT_TOKEN="your_token"
API_URL="http://localhost:8585/api/v1"

# 检查用户
curl -X GET "${API_URL}/users/name/alice" \
  -H "Authorization: Bearer ${JWT_TOKEN}" | jq

# 检查团队
curl -X GET "${API_URL}/teams/name/finance-team" \
  -H "Authorization: Bearer ${JWT_TOKEN}" | jq

# 检查数据库服务
curl -X GET "${API_URL}/services/databaseServices" \
  -H "Authorization: Bearer ${JWT_TOKEN}" | jq '.data[] | {name: .name}'
```

## 🐛 已知问题和解决方案

### Issue: "Empty owner list" 或 "IndexError"

**原因**：某些验证逻辑返回了空的owner列表

**解决**：已在最新代码中修复，确保使用最新版本

### Issue: Test-08 配置了 marketing_db 但连接的是 finance_db

**状态**：这是配置问题，test-08的ownerConfig中包含了marketing_db的配置，但实际连接的是finance_db

**影响**：marketing_db的owner配置不会生效，但不影响测试结果

**修复**（可选）：修改test-08连接到marketing_db或移除marketing_db的配置

## 📋 完整检查清单

运行测试前，确保：

- [ ] PostgreSQL 测试数据库运行中
- [ ] 所有8个用户已创建（alice, bob, charlie, david, emma, frank, marketing-user-1, marketing-user-2）
- [ ] 所有11个团队已创建
- [ ] JWT Token 有效且已更新到测试文件中
- [ ] metadata 命令可用（虚拟环境已激活）
- [ ] 从 OpenMetadata 根目录运行测试
- [ ] OpenMetadata 服务器运行在 http://localhost:8585

## 🔧 快速诊断脚本

```bash
#!/bin/bash
# 保存为 diagnose.sh

echo "======================================"
echo "Owner Config Tests - Quick Diagnosis"
echo "======================================"

# 检查 PostgreSQL
echo -n "PostgreSQL: "
if docker ps | grep -q postgres; then
    echo "✓ Running"
else
    echo "✗ Not running"
fi

# 检查 metadata 命令
echo -n "metadata command: "
if command -v metadata &> /dev/null; then
    echo "✓ Available"
else
    echo "✗ Not found"
fi

# 检查JWT Token
echo -n "JWT Token in test files: "
if grep -q "YOUR_JWT_TOKEN_HERE" test-01-basic-configuration.yaml 2>/dev/null; then
    echo "⚠ Not updated"
else
    echo "✓ Updated"
fi

# 检查用户
echo -n "Test users: "
JWT_TOKEN="${OPENMETADATA_JWT_TOKEN:-}"
if [ -n "$JWT_TOKEN" ]; then
    if curl -s -H "Authorization: Bearer $JWT_TOKEN" \
       http://localhost:8585/api/v1/users/name/alice &>/dev/null; then
        echo "✓ alice exists"
    else
        echo "✗ alice not found"
    fi
else
    echo "⚠ JWT_TOKEN not set, cannot check"
fi

echo ""
echo "Run './setup-test-entities.sh' if users/teams are missing"
echo "Run 'docker-compose up -d' if PostgreSQL is not running"
```

## 💡 获取帮助

如果以上步骤无法解决问题，请提供以下信息：

1. 具体的错误消息（完整日志）
2. 失败的测试编号（3、4、7、8）
3. DEBUG 日志输出
4. 运行环境信息（OS, Python版本, OpenMetadata版本）
