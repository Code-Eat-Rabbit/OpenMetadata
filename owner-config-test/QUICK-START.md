# 🚀 Quick Start Guide

## ⚡ 5 分钟快速开始

### Step 1: 启动测试环境 (30 秒)
```bash
cd /workspace/owner-config-test
./start-test-env.sh
```

### Step 2: 在 OpenMetadata 中创建测试 Teams (2 分钟)
访问 http://localhost:8585 → Settings → Teams → Add Team

**最小必需 Teams**:
```
data-platform-team
finance-team
accounting-team
treasury-team
```

### Step 3: 获取并更新 JWT Token (1 分钟)
```bash
# 在 OpenMetadata UI: Settings → Bots → ingestion-bot → Token → Copy

# 更新所有配置文件
cd /workspace/owner-config-test
find . -name "test-*.yaml" -exec sed -i 's/YOUR_JWT_TOKEN_HERE/你的实际token/g' {} \;
```

### Step 4: 运行第一个测试 (1 分钟)
```bash
cd /workspace/ingestion
metadata ingest -c /workspace/owner-config-test/test-01-default-owner-only.yaml
```

### Step 5: 验证结果 (30 秒)
访问 OpenMetadata UI:
- Databases → postgres-finance-test-01
- 点击任意 database/schema/table
- 查看 Owners 字段

---

## 🎯 推荐测试顺序

### 第一阶段：基础功能 (5 分钟)
```bash
cd /workspace/ingestion

# 1. 默认 owner
metadata ingest -c /workspace/owner-config-test/test-01-default-owner-only.yaml

# 2. 层级 owner
metadata ingest -c /workspace/owner-config-test/test-02-hierarchical-owners.yaml
```

### 第二阶段：新功能验证 (10 分钟)
```bash
# 3. 多 owner（⭐ 新功能）
metadata ingest -c /workspace/owner-config-test/test-08-multiple-owners.yaml

# 验证: revenue table 应该有 3 个 owners
# UI: postgres-finance-test-08 → finance_db → accounting → revenue → Owners

# 4. 部分成功策略（⭐ A1 策略）
metadata ingest -c /workspace/owner-config-test/test-12-partial-success-scenario.yaml

# 检查日志: 应该看到 WARNING 但 ingestion 继续
```

### 第三阶段：高级功能 (10 分钟)
```bash
# 5. FQN 匹配
metadata ingest -c /workspace/owner-config-test/test-04-fqn-schema-matching.yaml

# 6. 简单名称 fallback（⭐ L1 日志）
metadata ingest -c /workspace/owner-config-test/test-05-simple-name-schema-matching.yaml 2>&1 | grep "INFO"

# 应该看到: INFO: FQN match failed, matched using simple name

# 7. 继承机制
metadata ingest -c /workspace/owner-config-test/test-09-inheritance-enabled.yaml
```

---

## 📊 测试用例速查

| 测试 | 关键功能 | 预期结果 |
|-----|---------|---------|
| 01 | 默认 owner | 所有实体 = data-platform-team |
| 02 | 层级配置 | database/schema/table 各不同 |
| 04 | FQN 匹配 | 精确匹配 FQN |
| 05 | 简单名称 fallback | 日志显示 INFO |
| 08 | **多 owner** | revenue table 有 3 个 owners |
| 09 | 继承开启 | 子实体继承父 owner |
| 10 | 继承关闭 | 子实体用 default |
| 12 | **部分成功** | 跳过不存在的 owner |
| 13 | 综合场景 | 所有功能混合使用 |

---

## 🔍 验证方法

### 方法 1: UI 查看
```
http://localhost:8585
→ Databases 
→ postgres-finance-test-XX
→ 点击任意实体
→ 查看 Owners 字段
```

### 方法 2: 查看日志
```bash
# FQN 匹配成功
grep "Matched owner.*using FQN" logs/test-04*.log

# 简单名称 fallback
grep "FQN match failed" logs/test-05*.log

# 找不到 owner
grep "Could not find owner" logs/test-12*.log
```

### 方法 3: API 查询
```bash
curl -X GET "http://localhost:8585/api/v1/tables/name/postgres-finance-test-08.finance_db.accounting.revenue" \
  -H "Authorization: Bearer YOUR_TOKEN" | jq '.owners'
```

---

## 🐛 常见问题

### Q: ingestion 失败，提示连接错误
```bash
# 检查数据库是否运行
docker-compose ps

# 重启数据库
docker-compose restart

# 测试连接
docker exec owner-test-postgres psql -U admin -d finance_db -c "SELECT 1"
```

### Q: owner 没有被应用
**检查清单**:
- [ ] OpenMetadata 中是否创建了对应的 Team/User？
- [ ] JWT Token 是否正确？
- [ ] 配置文件中是否有 `overrideMetadata: true`？

### Q: 多 owner 只显示一个
**原因**: 可能使用了旧版本的 OpenMetadata 或未应用代码修改

**解决**:
```bash
# 确认修改已应用
cd /workspace/ingestion
python3 -c "from metadata.utils.owner_utils import OwnerResolver; import inspect; print(inspect.getsource(OwnerResolver._get_owner_refs))"
```

### Q: 日志中看不到 INFO 消息
```bash
# 确认 loggerLevel 设置
grep "loggerLevel" test-05-simple-name-schema-matching.yaml

# 应该是: loggerLevel: DEBUG
```

---

## 📞 需要帮助？

1. **查看详细文档**: `cat README.md`
2. **测试场景对照**: `cat TEST-SCENARIOS.md`
3. **验证数据库**: `./verify-data.sh`
4. **重置环境**:
   ```bash
   docker-compose down -v
   ./start-test-env.sh
   ```

---

## 🎯 核心验证点

运行这 3 个测试，确保核心功能正常：

```bash
cd /workspace/ingestion

# ✅ 测试 1: 多 owner 新功能
metadata ingest -c /workspace/owner-config-test/test-08-multiple-owners.yaml
# 验证: revenue table 有 3 个 owners

# ✅ 测试 2: L1 日志策略
metadata ingest -c /workspace/owner-config-test/test-05-simple-name-schema-matching.yaml 2>&1 | grep "INFO.*FQN match failed"
# 验证: 看到 INFO 级别的 fallback 日志

# ✅ 测试 3: A1 部分成功策略
metadata ingest -c /workspace/owner-config-test/test-12-partial-success-scenario.yaml 2>&1 | grep "WARNING.*Could not find"
# 验证: 看到 WARNING 但 ingestion 继续执行
```

---

## 🧹 清理环境

```bash
# 停止数据库（保留数据）
cd /workspace/owner-config-test
docker-compose down

# 完全清理（删除数据）
docker-compose down -v
```

---

**完成时间**: 约 30 分钟（包括所有测试）

**最小测试时间**: 5 分钟（仅测试 01, 08, 12）
