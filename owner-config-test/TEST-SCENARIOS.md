# 测试场景对照表

## 📋 测试覆盖矩阵

| 测试编号 | 测试名称 | 测试功能 | 配置特点 | 预期验证点 |
|---------|---------|---------|---------|-----------|
| **01** | Default Owner Only | 默认 owner | 只配置 `default` | 所有实体使用同一 owner |
| **02** | Hierarchical Owners | 层级 owner | database/schema/table 各不同 | 每层级使用各自配置 |
| **03** | Specific Database | 特定 database | database dict 映射 | 不同 database 不同 owner |
| **04** | FQN Schema | FQN 精确匹配 | `"db.schema": "owner"` | FQN 优先匹配 |
| **05** | Simple Name Schema | 简单名称 fallback | `"schema": "owner"` | 触发 INFO 日志 |
| **06** | FQN Table | FQN 精确匹配 | `"db.schema.table": "owner"` | FQN 优先匹配 |
| **07** | Simple Name Table | 简单名称 fallback | `"table": "owner"` | 触发 INFO 日志 |
| **08** | Multiple Owners ⭐ | 多 owner（新功能） | 数组格式 `["owner1", "owner2"]` | UI 显示多个 owners |
| **09** | Inheritance Enabled | 继承机制 | `enableInheritance: true` | 子实体继承父 owner |
| **10** | Inheritance Disabled | 禁用继承 | `enableInheritance: false` | 子实体使用 default |
| **11** | Marketing Database | 完整配置 | marketing_db 多 owner | 另一个数据库验证 |
| **12** | Partial Success ⭐ | A1 部分成功策略 | 包含不存在的 owner | 跳过失败，继续其他 |
| **13** | Complex Mixed | 综合场景 | FQN+简单+单个+多个 | 所有功能组合 |

---

## 🎯 重点测试场景

### 1. FQN vs 简单名称匹配
- **Test 04 + 05**: Schema 层级
- **Test 06 + 07**: Table 层级
- **验证点**: FQN 优先，fallback 时记录 INFO 日志

### 2. 多 Owner 新功能 (方案 A)
- **Test 08**: 单个和多个混合
- **Test 11**: 实际业务场景
- **验证点**: UI 正确显示所有 owners

### 3. 部分成功策略 (A1)
- **Test 12**: 包含不存在的 owner
- **验证点**: 不影响其他 owner，记录 WARNING

### 4. 继承机制
- **Test 09 vs 10**: 对比测试
- **验证点**: 开关控制继承行为

---

## 📊 配置模式对照

### 配置格式示例

```yaml
# 模式 1: String - 所有实体统一
database: "team-name"

# 模式 2: Dict + String - 特定实体单个 owner
database:
  "finance_db": "finance-team"
  "marketing_db": "marketing-team"

# 模式 3: Dict + Array - 特定实体多个 owner (新功能)
database:
  "finance_db": ["finance-team", "audit-team"]
  "shared_db": ["team1", "team2", "team3"]

# 模式 4: FQN 精确匹配
databaseSchema:
  "finance_db.accounting": "accounting-team"

# 模式 5: 简单名称匹配（fallback）
databaseSchema:
  "accounting": "accounting-team"

# 模式 6: 混合使用
table:
  "finance_db.accounting.revenue": ["team1", "team2"]  # FQN + 多个
  "expenses": "expense-team"                           # 简单名称 + 单个
```

---

## 🔍 验证检查清单

### 基础功能
- [ ] Test 01: Default owner 应用到所有实体
- [ ] Test 02: 三个层级的 owner 各不相同
- [ ] Test 03: 不同 database 有不同 owner

### FQN 匹配 (优先级)
- [ ] Test 04: FQN 匹配 schema，日志显示 "Matched using FQN"
- [ ] Test 05: 简单名称匹配 schema，日志显示 "FQN match failed, matched using simple name"
- [ ] Test 06: FQN 匹配 table
- [ ] Test 07: 简单名称匹配 table，日志显示 INFO

### 新功能：多 Owner
- [ ] Test 08: revenue table 有 3 个 owners
- [ ] Test 08: expenses table 有 2 个 owners
- [ ] Test 08: treasury schema 只有 1 个 owner（向后兼容）
- [ ] Test 11: marketing tables 正确显示多个 owners

### 继承机制
- [ ] Test 09: accounting schema 继承 database owner
- [ ] Test 09: revenue table 继承 schema owner
- [ ] Test 10: 禁用继承后使用 default owner

### 部分成功策略 (A1)
- [ ] Test 12: revenue table 有 2 个 owners（跳过不存在的 1 个）
- [ ] Test 12: 日志显示 "WARNING: Could not find owner: nonexistent-team-1"
- [ ] Test 12: 日志显示 "WARNING: Could not find owner: nonexistent-team-2"
- [ ] Test 12: expenses table 有 2 个 owners（跳过不存在的 2 个）

### 综合场景
- [ ] Test 13: 所有功能组合使用无冲突

---

## 📝 预期日志关键词

### DEBUG 级别
```
Resolving owner for table 'revenue'
Matched owner for 'finance_db.accounting.revenue' using FQN: ['revenue-team', 'finance-team']
Found owner: revenue-team
Found owner: finance-team
```

### INFO 级别 (L1)
```
FQN match failed for 'finance_db.accounting.revenue', matched using simple name 'revenue': revenue-team
```

### WARNING 级别
```
Could not find owner: nonexistent-team-1
Could not find owner: nonexistent-team-2
```

---

## 🧪 快速测试命令

### 运行单个测试
```bash
cd /workspace/ingestion
metadata ingest -c /workspace/owner-config-test/test-08-multiple-owners.yaml
```

### 查看特定测试的日志
```bash
metadata ingest -c /workspace/owner-config-test/test-05-simple-name-schema-matching.yaml 2>&1 | grep -E "INFO|WARNING|owner"
```

### 验证 API 结果
```bash
# 查询 table 的 owners
curl -X GET "http://localhost:8585/api/v1/tables/name/postgres-finance-test-08.finance_db.accounting.revenue" \
  -H "Authorization: Bearer YOUR_TOKEN" | jq '.owners'
```

---

## 🎯 测试通过标准

### 必须通过的核心测试
1. **Test 01**: 基础功能验证
2. **Test 05/07**: L1 日志策略验证
3. **Test 08**: 多 owner 新功能验证（核心）
4. **Test 09/10**: 继承机制验证
5. **Test 12**: A1 部分成功策略验证（核心）

### 推荐通过的扩展测试
6. Test 04/06: FQN 匹配验证
7. Test 13: 综合场景验证

---

## 💡 调试提示

### 问题：owner 没有被应用
- 检查 `overrideMetadata: true` 是否配置
- 检查 owner 名称在 OpenMetadata 中是否存在
- 查看 DEBUG 日志确认配置被正确解析

### 问题：日志中没有 INFO/WARNING
- 确认 `loggerLevel: DEBUG` 已配置
- 使用 `grep` 过滤日志查看

### 问题：多 owner 只显示一个
- 检查 JSON Schema 是否已更新
- 确认使用的是修改后的代码
- 查看 API 返回的 `owners` 数组

### 问题：继承不生效
- 确认 `enableInheritance: true`
- 检查父实体是否已经有 owner
- 查看日志确认继承逻辑执行

---

## 📞 获取帮助

如果测试遇到问题：
1. 查看 `/workspace/owner-config-test/README.md` 详细说明
2. 检查日志文件 `/workspace/owner-config-test/logs/`
3. 验证数据库连接 `./verify-data.sh`
4. 重新启动环境 `docker-compose down -v && ./start-test-env.sh`
