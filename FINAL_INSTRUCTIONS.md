# 最终执行指令

## ✅ 代码修改确认

您的代码修改已经**完全正确**！

验证：
```bash
cd ~/workspaces/OpenMetadata

# 检查修改（应该看到2行）
grep -n "parent_owner: Optional\[Union\[str, List\[str\]\]\]" ingestion/src/metadata/utils/owner_utils.py
```

**期望输出**：
```
56:        parent_owner: Optional[Union[str, List[str]]] = None,
234:    parent_owner: Optional[Union[str, List[str]]] = None,
```

如果看到这两行，说明修改完全正确！✅

## 🚀 立即运行测试

### 方法 1: 使用更新后的验证脚本（推荐）

```bash
cd ~/workspaces/OpenMetadata

# 从 /workspace 复制更新后的脚本
cp /workspace/RUN_AND_VERIFY.sh ./RUN_AND_VERIFY.sh

# 运行
bash RUN_AND_VERIFY.sh
```

### 方法 2: 手动运行测试

```bash
cd ~/workspaces/OpenMetadata

# 清除缓存
find ingestion/src -name "*.pyc" -delete
find ingestion/src -name "__pycache__" -exec rm -rf {} + 2>/dev/null

# 运行测试
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml 2>&1 | tee /tmp/test-03.log

# 检查继承日志
grep -i "inherited owner" /tmp/test-03.log
```

**期望看到**（关键！）：
```
DEBUG ... Using inherited owner for 'accounting': ['alice', 'bob']
或
DEBUG ... Using inherited owner for 'accounting': alice, bob
```

如果看到列表或两个名字，说明继承正常！

### 方法 3: 直接验证 API

等 ingestion 完成后：

```bash
# 设置 JWT token（如果未设置）
export JWT_TOKEN="your_token_here"

# 检查 accounting schema 的 owners
curl -s -X GET "http://localhost:8585/api/v1/databaseSchemas/name/postgres-test-03-multiple-users.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners | length'
```

**期望输出**: `2`（而不是 `1`）

## 🔍 如果仍然只有1个owner

### 步骤1: 检查日志中的详细信息

```bash
# 查看所有 owner 相关的日志
grep -i "owner\|parent" /tmp/test-03.log | grep -v "password"

# 特别关注 accounting schema 的日志
grep -C 5 "accounting" /tmp/test-03.log | grep -i owner
```

### 步骤2: 添加临时调试输出

编辑 `ingestion/src/metadata/ingestion/source/database/common_db_source.py`，在第228行后添加：

```python
self.context.get().upsert("database_owner", database_owner)

# 🔍 临时调试
import sys
print(f"🔍 DEBUG [database]: database_owner_names = {database_owner_names}", file=sys.stderr)
print(f"🔍 DEBUG [database]: database_owner (context) = {database_owner}", file=sys.stderr)
print(f"🔍 DEBUG [database]: type = {type(database_owner)}", file=sys.stderr)
```

编辑 `ingestion/src/metadata/utils/owner_utils.py`，在第117行后添加：

```python
if self.enable_inheritance and parent_owner:
    # 🔍 临时调试
    import sys
    print(f"🔍 DEBUG [resolve]: parent_owner = {parent_owner}", file=sys.stderr)
    print(f"🔍 DEBUG [resolve]: type = {type(parent_owner)}", file=sys.stderr)
    
    owner_ref = self._get_owner_refs(parent_owner)
    
    # 🔍 临时调试
    if owner_ref and owner_ref.root:
        print(f"🔍 DEBUG [resolve]: returned {len(owner_ref.root)} owners: {[o.name for o in owner_ref.root]}", file=sys.stderr)
```

然后运行：

```bash
metadata ingest -c test-03-multiple-users.yaml 2>&1 | grep "🔍 DEBUG"
```

**期望看到**：
```
🔍 DEBUG [database]: database_owner_names = ['alice', 'bob']
🔍 DEBUG [database]: database_owner (context) = ['alice', 'bob']
🔍 DEBUG [database]: type = <class 'list'>
🔍 DEBUG [resolve]: parent_owner = ['alice', 'bob']
🔍 DEBUG [resolve]: type = <class 'list'>
🔍 DEBUG [resolve]: returned 2 owners: ['alice', 'bob']
```

如果看到的不是这样，请告诉我具体输出是什么。

### 步骤3: 检查 OpenMetadata 服务端

可能性：OpenMetadata 服务端有限制或bug，即使我们发送了2个owners，服务端也只保存了1个。

验证方法：

```bash
# 检查 database 的 owners（这个应该肯定是2个，因为是直接配置的）
curl -s "http://localhost:8585/api/v1/databases/name/postgres-test-03-multiple-users.finance_db" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners'
```

如果 **database** 只有1个owner，说明问题在服务端或网络层。

如果 **database** 有2个owner，但 **schema** 只有1个，说明继承逻辑有问题。

## 📊 预期的完整流程

### 正确的数据流：

1. **配置解析**:
   ```yaml
   database:
     "finance_db": ["alice", "bob"]  # 数组
   ```

2. **Database 层级**:
   ```python
   # resolve_owner 返回
   EntityReferenceList(root=[
       EntityReference(name="alice", type="user"),
       EntityReference(name="bob", type="user")
   ])
   
   # 存储到 context
   database_owner = ["alice", "bob"]  # 列表
   ```

3. **Schema 层级（继承）**:
   ```python
   # 从 context 获取
   parent_owner = ["alice", "bob"]  # 列表
   
   # 调用 resolve_owner
   owner_ref = self._get_owner_refs(["alice", "bob"])
   
   # 返回
   EntityReferenceList(root=[
       EntityReference(name="alice", type="user"),
       EntityReference(name="bob", type="user")
   ])
   ```

4. **API 存储**:
   ```json
   {
     "owners": [
       {"name": "alice", "type": "user"},
       {"name": "bob", "type": "user"}
     ]
   }
   ```

## 🆘 需要更多帮助

如果上述步骤都正常，但还是只有1个owner，请提供：

1. **调试日志**:
   ```bash
   grep "🔍 DEBUG" /tmp/test-03.log
   ```

2. **继承日志**:
   ```bash
   grep "inherited owner" /tmp/test-03.log
   ```

3. **API 返回**:
   ```bash
   curl ... | jq '.owners'
   ```

我会根据这些信息进一步诊断！
