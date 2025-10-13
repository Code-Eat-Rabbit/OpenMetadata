# 检查多Owner继承问题

## 🔍 问题诊断步骤

您说仍然只有一个人，让我们逐步检查问题：

### 步骤 1: 确认代码修改已生效

```bash
cd ~/workspaces/OpenMetadata

# 检查 common_db_source.py 的修改
grep -A 5 "Store ALL owner names" ingestion/src/metadata/ingestion/source/database/common_db_source.py

# 应该看到：
# database_owner_names = [owner.name for owner in database_owner_ref.root]
```

**期望输出**:
```python
# Store ALL owner names (support multiple owners for inheritance)
database_owner_names = [owner.name for owner in database_owner_ref.root]
# If only one owner, store as string; otherwise store as list
database_owner = database_owner_names[0] if len(database_owner_names) == 1 else database_owner_names
```

如果**没有看到**这个，说明修改没有保存，请重新应用修改。

### 步骤 2: 检查 owner_utils.py 的类型声明

```bash
grep "parent_owner: Optional" ingestion/src/metadata/utils/owner_utils.py

# 应该看到（2处）：
# parent_owner: Optional[Union[str, List[str]]] = None,
```

**期望输出**:
```python
parent_owner: Optional[Union[str, List[str]]] = None,  # 第56行
parent_owner: Optional[Union[str, List[str]]] = None,  # 第234行
```

如果还是 `Optional[str]`，说明类型声明没有更新。

### 步骤 3: 运行带调试日志的 ingestion

```bash
cd ~/workspaces/OpenMetadata

# 运行测试，开启DEBUG日志
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml --debug 2>&1 | tee /tmp/ingestion_debug.log

# 搜索继承相关的日志
grep -i "inherited\|parent_owner" /tmp/ingestion_debug.log
```

**关键日志要点**:

1. **Database 层级**（应该看到2个owners）:
```
DEBUG ... Matched owner for 'finance_db' using FQN: ['alice', 'bob']
```

2. **Schema 层级**（应该继承列表）:
```
DEBUG ... Using inherited owner for 'accounting': ['alice', 'bob']
或
DEBUG ... Using inherited owner for 'accounting': alice, bob
```

❌ **如果看到的是**:
```
DEBUG ... Using inherited owner for 'accounting': alice
或
DEBUG ... Using inherited owner for 'accounting': ['alice']
```
说明继承时只传递了一个owner。

### 步骤 4: 检查实际创建的请求

在日志中搜索 `CreateDatabaseSchemaRequest`:

```bash
grep -A 20 "CreateDatabaseSchemaRequest" /tmp/ingestion_debug.log | grep -A 5 "accounting"
```

**期望看到**:
```
owners: [
  EntityReference(name='alice', type='user'),
  EntityReference(name='bob', type='user')
]
```

### 步骤 5: 检查 API 实际存储的数据

```bash
# 获取 schema 的 owners
JWT_TOKEN="your_jwt_token"

curl -s -X GET "http://localhost:8585/api/v1/databaseSchemas/name/postgres-test-03-multiple-users.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners'
```

**期望输出**（2个owners）:
```json
[
  {
    "id": "...",
    "name": "alice",
    "type": "user"
  },
  {
    "id": "...",
    "name": "bob",
    "type": "user"
  }
]
```

❌ **如果只看到1个**:
```json
[
  {
    "id": "...",
    "name": "alice",
    "type": "user"
  }
]
```

## 🐛 常见问题排查

### 问题 A: 代码修改没有生效

**症状**: 检查代码文件，发现还是旧的

**解决**:
```bash
# 重新应用修改
cd ~/workspaces/OpenMetadata

# 确认 common_db_source.py 第225-228行
sed -n '225,228p' ingestion/src/metadata/ingestion/source/database/common_db_source.py

# 如果不对，重新修改
```

### 问题 B: Python 缓存的 .pyc 文件

**症状**: 代码改了但运行还是旧逻辑

**解决**:
```bash
cd ~/workspaces/OpenMetadata/ingestion

# 清除所有 .pyc 缓存
find . -type f -name "*.pyc" -delete
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# 重新运行
metadata ingest -c tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml
```

### 问题 C: OpenMetadata 服务端限制

**症状**: 日志显示传递了2个owners，但API只返回1个

**可能原因**: OpenMetadata 服务端可能有限制或bug

**检查**:
```bash
# 直接测试 database 的 owners（这个应该是2个）
curl -s -X GET "http://localhost:8585/api/v1/databases/name/postgres-test-03-multiple-users.finance_db" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners | length'

# 期望输出: 2
```

如果 database 只有1个owner，说明问题在更早的阶段。

### 问题 D: 旧数据残留

**症状**: 之前运行过测试，数据库中有旧的owner信息

**解决**:
```bash
# 方法1: 删除旧的 service（重新ingestion）
# 需要通过 UI 或 API 删除 postgres-test-03-multiple-users service

# 方法2: 使用 overrideMetadata (test-03 已配置)
# 检查 yaml 文件
grep overrideMetadata ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml

# 应该看到: overrideMetadata: true
```

## 📊 快速诊断脚本

创建一个脚本自动检查：

```bash
cat > /tmp/check_multi_owner.sh << 'EOF'
#!/bin/bash

echo "多Owner继承快速诊断"
echo "===================="
echo ""

# 1. 检查代码修改
echo "【1】检查 common_db_source.py 修改："
if grep -q "database_owner_names = \[owner.name for owner in database_owner_ref.root\]" ~/workspaces/OpenMetadata/ingestion/src/metadata/ingestion/source/database/common_db_source.py; then
    echo "✅ Database owner 存储逻辑已修改"
else
    echo "❌ Database owner 存储逻辑未修改（问题在这里！）"
fi

if grep -q "schema_owner_names = \[owner.name for owner in schema_owner_ref.root\]" ~/workspaces/OpenMetadata/ingestion/src/metadata/ingestion/source/database/common_db_source.py; then
    echo "✅ Schema owner 存储逻辑已修改"
else
    echo "❌ Schema owner 存储逻辑未修改（问题在这里！）"
fi

echo ""

# 2. 检查类型声明
echo "【2】检查 owner_utils.py 类型声明："
if grep -q "parent_owner: Optional\[Union\[str, List\[str\]\]\]" ~/workspaces/OpenMetadata/ingestion/src/metadata/utils/owner_utils.py; then
    echo "✅ parent_owner 类型已更新为 Union[str, List[str]]"
else
    echo "❌ parent_owner 类型还是 str（问题在这里！）"
fi

echo ""
echo "【3】建议操作："
echo "  1. 如果上面有 ❌，重新应用修改"
echo "  2. 清除 Python 缓存: find ingestion -name '*.pyc' -delete"
echo "  3. 运行: metadata ingest -c test-03-multiple-users.yaml --debug"
echo "  4. 检查日志: grep 'inherited' /tmp/ingestion_debug.log"
EOF

chmod +x /tmp/check_multi_owner.sh
bash /tmp/check_multi_owner.sh
```

## 🔬 深度调试

如果上面都正常，但还是只有1个owner，添加调试输出：

### 临时修改 common_db_source.py（添加打印）

在第225行后添加：

```python
# Store ALL owner names (support multiple owners for inheritance)
database_owner_names = [owner.name for owner in database_owner_ref.root]
# If only one owner, store as string; otherwise store as list
database_owner = database_owner_names[0] if len(database_owner_names) == 1 else database_owner_names

# 🔍 临时调试输出
print(f"🔍 DEBUG: database_owner_names = {database_owner_names}")
print(f"🔍 DEBUG: database_owner (stored in context) = {database_owner}")
print(f"🔍 DEBUG: type = {type(database_owner)}")

self.context.get().upsert("database_owner", database_owner)
```

### 临时修改 owner_utils.py（添加打印）

在第117行后添加：

```python
if self.enable_inheritance and parent_owner:
    # 🔍 临时调试输出
    print(f"🔍 DEBUG: resolve_owner called with parent_owner = {parent_owner}")
    print(f"🔍 DEBUG: parent_owner type = {type(parent_owner)}")
    
    owner_ref = self._get_owner_refs(parent_owner)
    
    # 🔍 临时调试输出
    if owner_ref and owner_ref.root:
        print(f"🔍 DEBUG: _get_owner_refs returned {len(owner_ref.root)} owners")
        print(f"🔍 DEBUG: owners = {[o.name for o in owner_ref.root]}")
```

然后运行：

```bash
metadata ingest -c test-03-multiple-users.yaml 2>&1 | grep "🔍 DEBUG"
```

**期望看到**:
```
🔍 DEBUG: database_owner_names = ['alice', 'bob']
🔍 DEBUG: database_owner (stored in context) = ['alice', 'bob']
🔍 DEBUG: type = <class 'list'>
🔍 DEBUG: resolve_owner called with parent_owner = ['alice', 'bob']
🔍 DEBUG: parent_owner type = <class 'list'>
🔍 DEBUG: _get_owner_refs returned 2 owners
🔍 DEBUG: owners = ['alice', 'bob']
```

## ✅ 最终验证

完成所有修改后：

```bash
# 1. 清除缓存
find ~/workspaces/OpenMetadata/ingestion -name "*.pyc" -delete

# 2. 运行测试
metadata ingest -c test-03-multiple-users.yaml --debug 2>&1 | tee /tmp/test.log

# 3. 检查关键日志
echo "=== 检查继承日志 ==="
grep "inherited owner" /tmp/test.log

echo ""
echo "=== 检查 API 结果 ==="
curl -s "http://localhost:8585/api/v1/databaseSchemas/name/postgres-test-03-multiple-users.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners | length'
```

期望输出: `2`

---

## 🆘 如果还是不行

请提供以下信息：

1. **代码检查结果**:
```bash
grep -n "database_owner_names" ingestion/src/metadata/ingestion/source/database/common_db_source.py
```

2. **日志片段**:
```bash
grep -C 3 "inherited" /tmp/ingestion_debug.log
```

3. **API 返回**:
```bash
curl ... | jq '.owners'
```

我会根据这些信息进一步诊断！
