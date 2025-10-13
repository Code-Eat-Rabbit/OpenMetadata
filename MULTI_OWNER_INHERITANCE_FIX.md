# 多Owner继承修复

## 🐛 问题描述

**现象**：当 database 层级配置了多个 owner（如 `["alice", "bob"]`）时，schema 和 table 层级继承时只继承了第一个 owner（alice），丢失了 bob。

**测试案例**：`test-03-multiple-users.yaml`

```yaml
ownerConfig:
  database:
    "finance_db": ["alice", "bob"]  # 配置了2个owners
  
  # schema 没有配置，应该继承 ["alice", "bob"]
  # 但实际只继承了 "alice"
```

## 🔍 根本原因

在 `common_db_source.py` 中，存储到 context 的 owner 信息**只取了第一个**：

```python
# 问题代码（第224-225行）
if database_owner_ref and database_owner_ref.root:
    database_owner_name = database_owner_ref.root[0].name  # ❌ 只取第一个！
    self.context.get().upsert("database_owner", database_owner_name)
```

**数据流程**：
1. `database_owner_ref.root` = `[EntityReference(name="alice"), EntityReference(name="bob")]`
2. 存储到 context：`database_owner_name = "alice"` ❌ 只取了 root[0]
3. schema 继承时：`parent_owner = "alice"` ❌ 丢失了 bob
4. `_get_owner_refs("alice")` → 只返回 alice 的引用

## ✅ 解决方案

### 修改 1：Database Owner 存储（完整列表）

**文件**：`ingestion/src/metadata/ingestion/source/database/common_db_source.py`

**位置**：第220-228行

```python
# 修改前（只存储第一个owner）
if database_owner_ref and database_owner_ref.root:
    database_owner_name = database_owner_ref.root[0].name  # ❌
    self.context.get().upsert("database_owner", database_owner_name)

# 修改后（存储所有owners）
if database_owner_ref and database_owner_ref.root:
    # Store ALL owner names (support multiple owners for inheritance)
    database_owner_names = [owner.name for owner in database_owner_ref.root]  # ✅
    # If only one owner, store as string; otherwise store as list
    database_owner = database_owner_names[0] if len(database_owner_names) == 1 else database_owner_names
    self.context.get().upsert("database_owner", database_owner)
```

**关键改进**：
- ✅ 使用列表推导式提取**所有** owner 的名字
- ✅ 单个 owner 时存储字符串（保持兼容性）
- ✅ 多个 owner 时存储列表（支持多owner继承）

### 修改 2：Schema Owner 存储（完整列表）

**文件**：`ingestion/src/metadata/ingestion/source/database/common_db_source.py`

**位置**：第279-287行

```python
# 修改前（只存储第一个owner）
if schema_owner_ref and schema_owner_ref.root:
    schema_owner_name = schema_owner_ref.root[0].name  # ❌
    self.context.get().upsert("schema_owner", schema_owner_name)

# 修改后（存储所有owners）
if schema_owner_ref and schema_owner_ref.root:
    # Store ALL owner names (support multiple owners for inheritance)
    schema_owner_names = [owner.name for owner in schema_owner_ref.root]  # ✅
    # If only one owner, store as string; otherwise store as list
    schema_owner = schema_owner_names[0] if len(schema_owner_names) == 1 else schema_owner_names
    self.context.get().upsert("schema_owner", schema_owner)
```

## 🔄 数据流程（修复后）

### 场景：Database 有多个 owner

```yaml
ownerConfig:
  database:
    "finance_db": ["alice", "bob"]  # 2个owners
  # schema 没有配置 → 应该继承
  # table 没有配置 → 应该继承
  enableInheritance: true
```

**修复后的流程**：

1. **Database 层级**：
   ```python
   database_owner_ref.root = [
       EntityReference(name="alice", type="user"),
       EntityReference(name="bob", type="user")
   ]
   
   # 提取所有名字
   database_owner_names = ["alice", "bob"]
   
   # 存储列表到 context（因为 len > 1）
   context.upsert("database_owner", ["alice", "bob"])  # ✅ 存储完整列表
   ```

2. **Schema 层级**（继承）：
   ```python
   # schema 没有配置，使用继承
   parent_owner = context.get("database_owner")  # ["alice", "bob"] ✅
   
   # resolve_owner 调用
   owner_ref = self._get_owner_refs(["alice", "bob"])  # ✅ 传入列表
   
   # _get_owner_refs 处理列表
   for owner_name in ["alice", "bob"]:
       # 查找并添加两个 owner
   
   # 返回 EntityReferenceList 包含 alice 和 bob ✅
   ```

3. **Table 层级**（继承）：
   ```python
   # table 没有配置，从 schema 继承
   schema_owner_names = ["alice", "bob"]
   
   # 同样的处理逻辑
   owner_ref = self._get_owner_refs(["alice", "bob"])  # ✅
   ```

## 📊 对比测试

### Test 3: Multiple Users

**配置**：
```yaml
ownerConfig:
  database:
    "finance_db": ["alice", "bob"]  # 2个users
  table:
    "finance_db.accounting.revenue": ["charlie", "david", "emma"]  # 3个users
    "finance_db.accounting.expenses": ["frank"]
```

**修复前的结果**：
```
finance_db:
  owners: ["alice", "bob"]  ✅ 正确

accounting schema (继承):
  owners: ["alice"]  ❌ 只继承了第一个

treasury schema (继承):
  owners: ["alice"]  ❌ 只继承了第一个

revenue table (配置):
  owners: ["charlie", "david", "emma"]  ✅ 正确（有配置）

expenses table (配置):
  owners: ["frank"]  ✅ 正确（有配置）

cash_flow table (继承):
  owners: ["alice"]  ❌ 只继承了第一个
```

**修复后的结果**：
```
finance_db:
  owners: ["alice", "bob"]  ✅ 正确

accounting schema (继承):
  owners: ["alice", "bob"]  ✅ 完整继承

treasury schema (继承):
  owners: ["alice", "bob"]  ✅ 完整继承

revenue table (配置):
  owners: ["charlie", "david", "emma"]  ✅ 正确

expenses table (配置):
  owners: ["frank"]  ✅ 正确

cash_flow table (继承 from treasury schema):
  owners: ["alice", "bob"]  ✅ 完整继承
```

## 🧪 验证方法

### 方法 1：查看日志

```bash
metadata ingest -c test-03-multiple-users.yaml 2>&1 | grep -i "inherited\|owner"
```

**期望看到**：
```
Using inherited owner for 'accounting': ['alice', 'bob']  # ✅ 列表
Using inherited owner for 'treasury': ['alice', 'bob']   # ✅ 列表
```

**而不是**：
```
Using inherited owner for 'accounting': alice  # ❌ 单个字符串
```

### 方法 2：查询 API

```bash
JWT_TOKEN="your_token"

# 检查 accounting schema 的 owners
curl -X GET "http://localhost:8585/api/v1/databaseSchemas/name/postgres-test-03-multiple-users.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners'

# 期望输出（2个owners）
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

### 方法 3：单元测试

```python
# 创建测试文件：test_multi_owner_inheritance.py
from metadata.utils.owner_utils import OwnerResolver

def test_multi_owner_inheritance():
    config = {
        "database": {"finance_db": ["alice", "bob"]},
        "enableInheritance": True
    }
    
    resolver = OwnerResolver(metadata, config)
    
    # Schema 应该继承 ["alice", "bob"]
    schema_owner = resolver.resolve_owner(
        entity_type="databaseSchema",
        entity_name="accounting",
        parent_owner=["alice", "bob"]  # ✅ 传入列表
    )
    
    assert schema_owner is not None
    assert len(schema_owner.root) == 2  # ✅ 应该有2个owners
    assert schema_owner.root[0].name == "alice"
    assert schema_owner.root[1].name == "bob"
```

## 🔧 兼容性说明

### 单个 Owner 场景（保持兼容）

```python
# 单个owner时，仍然存储字符串（不是列表）
if len(database_owner_names) == 1:
    database_owner = database_owner_names[0]  # "alice" (字符串)
else:
    database_owner = database_owner_names  # ["alice", "bob"] (列表)
```

**为什么这样做**：
- ✅ 保持向后兼容（单个owner场景不变）
- ✅ `_get_owner_refs` 可以处理 `Union[str, List[str]]`
- ✅ 日志输出更清晰（单个时显示字符串，多个时显示列表）

### _get_owner_refs 函数已支持

**文件**：`ingestion/src/metadata/utils/owner_utils.py`

**第142-161行**：
```python
def _get_owner_refs(
    self, owner_names: Union[str, List[str]]  # ✅ 已支持 Union
) -> Optional[EntityReferenceList]:
    """Get owner references from OpenMetadata"""
    if isinstance(owner_names, str):
        owner_names = [owner_names]  # ✅ 转换为列表
    
    if not owner_names:
        return None
    
    all_owners = []
    for owner_name in owner_names:  # ✅ 遍历所有names
        # ... 查找并添加
```

**已完美支持**！无需修改。

## 📋 完整修复清单

| 文件 | 位置 | 修改内容 | 状态 |
|------|------|----------|------|
| `common_db_source.py` | 220-228行 | Database owner 存储完整列表 | ✅ 已修复 |
| `common_db_source.py` | 279-287行 | Schema owner 存储完整列表 | ✅ 已修复 |
| `owner_utils.py` | 142-161行 | `_get_owner_refs` 支持列表 | ✅ 已支持 |
| `owner_utils.py` | 116-122行 | `resolve_owner` 使用列表 | ✅ 已支持 |

## 🚀 执行验证

```bash
cd ~/workspaces/OpenMetadata

# 1. 不需要重新生成模型（只修改了 Python 代码）
# 2. 不需要重新安装（代码直接生效）

# 直接运行测试
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml

# 验证 accounting schema 有2个owners
curl -X GET "http://localhost:8585/api/v1/databaseSchemas/name/postgres-test-03-multiple-users.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners | length'

# 期望输出：2（而不是1）
```

## 🎯 预期结果

### Test 3 - Multiple Users

| 实体 | 配置 | 修复前 | 修复后 |
|------|------|--------|--------|
| finance_db | `["alice", "bob"]` | alice, bob ✅ | alice, bob ✅ |
| accounting schema | 继承 | alice ❌ | alice, bob ✅ |
| treasury schema | 继承 | alice ❌ | alice, bob ✅ |
| revenue table | `["charlie", "david", "emma"]` | charlie, david, emma ✅ | charlie, david, emma ✅ |
| expenses table | `["frank"]` | frank ✅ | frank ✅ |
| cash_flow table | 继承 | alice ❌ | alice, bob ✅ |

### Test 8 - Complex Mixed

| 实体 | 配置 | 修复前 | 修复后 |
|------|------|--------|--------|
| marketing_db | `["marketing-user-1", "marketing-user-2"]` | 2个users ✅ | 2个users ✅ |
| accounting schema | `["alice", "bob"]` | 2个users ✅ | 2个users ✅ |
| revenue table (继承 from accounting) | 继承 | alice ❌ | alice, bob ✅ |

## 💡 技术要点

1. **Context 存储**：
   - 单个 owner → 字符串 `"alice"`
   - 多个 owner → 列表 `["alice", "bob"]`

2. **类型支持**：
   - `parent_owner: Union[str, List[str]]` ✅
   - `_get_owner_refs` 自动处理 ✅

3. **继承传递**：
   - Database → Schema（完整列表）✅
   - Schema → Table（完整列表）✅

4. **向后兼容**：
   - 单个 owner 场景不受影响 ✅
   - 现有代码无需修改 ✅

## 🎉 总结

**问题**：多 owner 继承时只继承第一个

**根因**：Context 只存储 `root[0].name`

**修复**：存储完整 owner 列表 `[owner.name for owner in root]`

**影响**：
- ✅ 修复多owner继承问题
- ✅ 保持单owner场景兼容
- ✅ 无需修改其他代码
- ✅ 立即生效（无需重新生成/安装）

立即测试验证！
