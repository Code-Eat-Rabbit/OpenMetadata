# OpenMetadata Owner Config 测试环境

## 📋 概述

这是一个完整的测试环境，用于验证 OpenMetadata 的 owner 配置功能（方案 A + A1 + L1）。

### 测试数据结构

```
PostgreSQL (localhost:5433)
├── finance_db
│   ├── accounting (schema)
│   │   ├── revenue (table)
│   │   ├── expenses (table)
│   │   └── monthly_summary (view)
│   └── treasury (schema)
│       ├── cash_flow (table)
│       ├── investments (table)
│
└── marketing_db
    ├── campaigns (schema)
    │   ├── email_campaigns (table)
    │   └── social_media (table)
    └── analytics (schema)
        ├── customer_segments (table)
        ├── conversion_funnel (table)
        └── campaign_performance (view)
```

---

## 🚀 快速开始

### 1. 启动 PostgreSQL 数据库

```bash
cd /workspace/owner-config-test

# 启动数据库
docker-compose up -d

# 检查数据库是否启动成功
docker-compose ps

# 查看初始化日志
docker-compose logs postgres
```

### 2. 验证数据库连接

```bash
# 连接到 finance_db
docker exec -it owner-test-postgres psql -U admin -d finance_db

# 验证数据
\dt accounting.*
\dt treasury.*
SELECT COUNT(*) FROM accounting.revenue;
\q

# 连接到 marketing_db
docker exec -it owner-test-postgres psql -U admin -d marketing_db

# 验证数据
\dt campaigns.*
\dt analytics.*
SELECT COUNT(*) FROM campaigns.email_campaigns;
\q
```

### 3. 准备 OpenMetadata 环境

在运行测试前，你需要：

1. **启动 OpenMetadata 服务**（如果还没有运行）
2. **获取 JWT Token**
3. **创建测试用的 Teams/Users**

#### 创建测试 Teams（在 OpenMetadata UI 中）

```
Settings → Teams → Add Team
```

创建以下 Teams：
- data-platform-team
- finance-team
- accounting-team
- treasury-team
- expense-team
- revenue-team
- treasury-ops-team
- investment-team
- compliance-team
- audit-team
- marketing-team
- creative-team
- analytics-team
- data-science-team
- email-team
- social-team
- schema-admin-team
- table-steward-team

#### 获取 JWT Token

```bash
# 在 OpenMetadata UI 中：
# Settings → Bots → ingestion-bot → Token → Copy
```

#### 更新配置文件

```bash
# 替换所有 YAML 文件中的 JWT Token
find . -name "test-*.yaml" -exec sed -i 's/YOUR_JWT_TOKEN_HERE/你的实际token/g' {} \;
```

---

## 🧪 测试用例说明

### Test 01: Default Owner Only
**文件**: `test-01-default-owner-only.yaml`

**测试内容**：
- 只配置 default owner
- 所有实体应该使用 "data-platform-team"

**运行**：
```bash
cd /workspace/ingestion
metadata ingest -c /workspace/owner-config-test/test-01-default-owner-only.yaml
```

**验证**：检查所有 database/schema/table 的 owner 是否都是 "data-platform-team"

---

### Test 02: Hierarchical Owners
**文件**: `test-02-hierarchical-owners.yaml`

**测试内容**：
- database 层级 → "finance-team"
- databaseSchema 层级 → "schema-admin-team"
- table 层级 → "table-steward-team"

**预期结果**：
- finance_db → "finance-team"
- accounting, treasury schemas → "schema-admin-team"
- 所有 tables → "table-steward-team"

---

### Test 03: Specific Database Owners
**文件**: `test-03-specific-database-owners.yaml`

**测试内容**：
- 为不同的 database 配置不同的 owner

**预期结果**：
- finance_db → "finance-team"
- 其他 databases → "data-platform-team" (default)

---

### Test 04: FQN Schema Matching
**文件**: `test-04-fqn-schema-matching.yaml`

**测试内容**：
- 使用完整 FQN 精确匹配 schema

**预期结果**：
- finance_db.accounting → "accounting-team"
- finance_db.treasury → "treasury-team"

**日志关键词**：`Matched owner using FQN`

---

### Test 05: Simple Name Schema Matching (Fallback)
**文件**: `test-05-simple-name-schema-matching.yaml`

**测试内容**：
- 使用简单名称匹配，测试 fallback 机制

**配置**：
```yaml
databaseSchema:
  "accounting": "accounting-team"    # 简单名称
  "treasury": "treasury-team"        # 简单名称
```

**预期结果**：
- accounting schema → "accounting-team"
- treasury schema → "treasury-team"

**日志关键词**：`INFO: FQN match failed, matched using simple name` (L1 策略)

---

### Test 06: FQN Table Matching
**文件**: `test-06-fqn-table-matching.yaml`

**测试内容**：
- 使用完整 FQN 精确匹配 table

**预期结果**：
- finance_db.accounting.revenue → "revenue-team"
- finance_db.accounting.expenses → "expense-team"
- finance_db.treasury.cash_flow → "treasury-ops-team"
- finance_db.treasury.investments → "investment-team"

---

### Test 07: Simple Name Table Matching (Fallback)
**文件**: `test-07-simple-name-table-matching.yaml`

**测试内容**：
- 使用简单名称匹配 table，测试 fallback

**配置**：
```yaml
table:
  "revenue": "revenue-team"
  "expenses": "expense-team"
```

**日志关键词**：`INFO: FQN match failed, matched using simple name`

---

### Test 08: Multiple Owners ⭐ 新功能
**文件**: `test-08-multiple-owners.yaml`

**测试内容**：
- 为实体配置多个 owners（数组格式）
- 单个和多个混合使用

**配置示例**：
```yaml
database:
  "finance_db": ["finance-team", "audit-team"]

databaseSchema:
  "finance_db.accounting": ["accounting-team", "compliance-team"]
  "finance_db.treasury": "treasury-team"    # 单个

table:
  "finance_db.accounting.revenue": ["revenue-team", "finance-team", "audit-team"]
  "finance_db.accounting.expenses": ["expense-team", "finance-team"]
```

**预期结果**：
- revenue table 应该有 3 个 owners
- expenses table 应该有 2 个 owners

---

### Test 09: Inheritance Enabled
**文件**: `test-09-inheritance-enabled.yaml`

**测试内容**：
- 测试继承机制 (enableInheritance: true)

**配置**：
```yaml
database:
  "finance_db": "finance-team"

databaseSchema:
  # accounting 没有配置
  "finance_db.treasury": "treasury-team"

table:
  # revenue 没有配置
  "finance_db.accounting.expenses": "expense-team"
```

**预期结果**：
- accounting schema → 继承 "finance-team"
- revenue table → 继承 accounting schema 的 owner (finance-team)
- expenses table → "expense-team" (自己的配置)

---

### Test 10: Inheritance Disabled
**文件**: `test-10-inheritance-disabled.yaml`

**测试内容**：
- 测试禁用继承 (enableInheritance: false)

**预期结果**：
- accounting schema (无配置) → "data-platform-team" (default，不继承)
- revenue table (无配置) → "data-platform-team" (default，不继承)

**对比 Test 09**：验证继承开关是否生效

---

### Test 11: Marketing Database
**文件**: `test-11-marketing-database.yaml`

**测试内容**：
- 测试 marketing_db 的完整配置
- 多 owners + FQN 匹配

**运行前先切换数据库**：
```bash
# 修改 YAML 中的 database: marketing_db
```

---

### Test 12: Partial Success Scenario ⭐ A1 策略
**文件**: `test-12-partial-success-scenario.yaml`

**测试内容**：
- 测试部分成功策略
- 配置中有不存在的 owner

**配置示例**：
```yaml
table:
  "finance_db.accounting.revenue":
    - "finance-team"           # 存在
    - "nonexistent-team-1"     # 不存在
    - "audit-team"             # 存在
```

**预期结果**：
- revenue table 应该有 2 个 owners (跳过不存在的)
- 日志应该显示：`WARNING: Could not find owner: nonexistent-team-1`

---

### Test 13: Complex Mixed Scenario
**文件**: `test-13-complex-mixed-scenario.yaml`

**测试内容**：
- 综合测试所有功能
- FQN + 简单名称 + 单个 + 多个 + 继承

**配置**：
```yaml
databaseSchema:
  "finance_db.accounting": ["accounting-team", "compliance-team"]  # FQN + 多 owner
  "treasury": "treasury-team"                                      # 简单名称 + 单 owner

table:
  "finance_db.accounting.revenue": ["revenue-team", "finance-team", "audit-team"]  # FQN + 多
  "expenses": "expense-team"                                                       # 简单名称 + 单
```

---

## 📊 测试执行计划

### 方式 1：逐个测试
```bash
cd /workspace/ingestion

# 测试 01
metadata ingest -c /workspace/owner-config-test/test-01-default-owner-only.yaml

# 测试 02
metadata ingest -c /workspace/owner-config-test/test-02-hierarchical-owners.yaml

# ... 依次执行其他测试
```

### 方式 2：批量测试脚本
```bash
# 创建测试脚本
cat > /workspace/owner-config-test/run-all-tests.sh << 'EOF'
#!/bin/bash

TESTS=(
  "test-01-default-owner-only"
  "test-02-hierarchical-owners"
  "test-03-specific-database-owners"
  "test-04-fqn-schema-matching"
  "test-05-simple-name-schema-matching"
  "test-06-fqn-table-matching"
  "test-07-simple-name-table-matching"
  "test-08-multiple-owners"
  "test-09-inheritance-enabled"
  "test-10-inheritance-disabled"
  "test-13-complex-mixed-scenario"
)

cd /workspace/ingestion

for test in "${TESTS[@]}"; do
  echo "========================================"
  echo "Running: $test"
  echo "========================================"
  metadata ingest -c "/workspace/owner-config-test/${test}.yaml" 2>&1 | tee "/workspace/owner-config-test/logs/${test}.log"
  echo ""
  sleep 5
done

echo "All tests completed!"
EOF

chmod +x /workspace/owner-config-test/run-all-tests.sh
mkdir -p /workspace/owner-config-test/logs
```

---

## 🔍 验证结果

### 方式 1：OpenMetadata UI
1. 访问 `http://localhost:8585`
2. 导航到 `Databases → postgres-finance-test-XX`
3. 检查每个 database/schema/table 的 Owners 字段

### 方式 2：API 查询
```bash
# 获取 table 的 owners
curl -X GET "http://localhost:8585/api/v1/tables/name/postgres-finance-test-01.finance_db.accounting.revenue" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" | jq '.owners'
```

### 方式 3：检查日志
```bash
# 查看 DEBUG 日志
grep -i "owner" /workspace/owner-config-test/logs/test-05-simple-name-schema-matching.log

# 查找 INFO 级别的 fallback 消息
grep "FQN match failed" /workspace/owner-config-test/logs/test-05-simple-name-schema-matching.log

# 查找 WARNING 消息
grep "Could not find owner" /workspace/owner-config-test/logs/test-12-partial-success-scenario.log
```

---

## 🧹 清理环境

```bash
# 停止并删除容器
cd /workspace/owner-config-test
docker-compose down -v

# 删除测试数据
rm -rf logs/
```

---

## 📝 测试检查清单

- [ ] Test 01: 默认 owner 生效
- [ ] Test 02: 层级 owner 配置正确
- [ ] Test 04: FQN 精确匹配成功
- [ ] Test 05: 简单名称 fallback 工作，日志显示 INFO
- [ ] Test 08: 多 owner 正确显示（新功能）
- [ ] Test 09: 继承机制工作
- [ ] Test 10: 禁用继承后使用 default
- [ ] Test 12: 部分成功策略工作，跳过不存在的 owner
- [ ] Test 13: 综合场景所有功能正常

---

## 🎯 重点验证项

### 1. FQN 匹配优先级
- Test 04 vs Test 05：验证 FQN 匹配优先于简单名称

### 2. Fallback 日志 (L1)
- Test 05, 07：验证简单名称 fallback 时记录 INFO 日志

### 3. 多 Owner 功能
- Test 08：验证数组格式配置，UI 显示多个 owners

### 4. 部分成功策略 (A1)
- Test 12：验证部分 owner 不存在时不影响其他 owner 添加

### 5. 继承机制
- Test 09 vs Test 10：验证 enableInheritance 开关效果

---

## 🐛 调试技巧

### 1. 查看详细日志
```bash
metadata ingest -c test-XX.yaml --debug
```

### 2. 只运行 metadata 生成，不发送到服务器
```bash
# 修改 sink 为 file 类型
sink:
  type: file
  config:
    filename: /tmp/test-output.json
```

### 3. 检查数据库连接
```bash
docker exec -it owner-test-postgres psql -U admin -d finance_db -c "\dt *.*"
```

### 4. 查看 OpenMetadata API
```bash
# 列出所有 databases
curl -X GET "http://localhost:8585/api/v1/databases" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" | jq '.data[].name'
```

---

## 📚 参考资料

- [OpenMetadata Ingestion Documentation](https://docs.open-metadata.org/connectors/ingestion)
- [Owner Configuration Guide](../path/to/guide.md)
- [重构 PR](link-to-pr)
