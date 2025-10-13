# 简单调试指南

## 🎯 快速定位问题

### 方法 1: 手动添加调试输出（推荐）

#### 步骤 1: 编辑 common_db_source.py

在第 **228行后** 添加（database owner 存储后）：

```python
self.context.get().upsert("database_owner", database_owner)

# 🔍 临时调试
import sys
print(f"🔍 [DB] names={database_owner_names}, stored={database_owner}, type={type(database_owner).__name__}", file=sys.stderr)
```

在第 **290行后** 添加（schema owner 存储后）：

```python
self.context.get().upsert("schema_owner", schema_owner)

# 🔍 临时调试  
import sys
print(f"🔍 [SCHEMA] names={schema_owner_names}, stored={schema_owner}, type={type(schema_owner).__name__}", file=sys.stderr)
```

#### 步骤 2: 编辑 owner_utils.py

在第 **117行后** 添加（继承逻辑中）：

```python
if self.enable_inheritance and parent_owner:
    # 🔍 临时调试
    import sys
    print(f"🔍 [RESOLVE] entity={entity_name}, parent={parent_owner}, type={type(parent_owner).__name__}", file=sys.stderr)
    
    owner_ref = self._get_owner_refs(parent_owner)
    
    # 🔍 临时调试
    if owner_ref and owner_ref.root:
        print(f"🔍 [RESOLVE] got {len(owner_ref.root)} owners: {[o.name for o in owner_ref.root]}", file=sys.stderr)
```

在 **_get_owner_refs** 函数开始（第160行后）添加：

```python
def _get_owner_refs(self, owner_names: Union[str, List[str]]) -> Optional[EntityReferenceList]:
    # 🔍 临时调试
    import sys
    print(f"🔍 [GET_REFS] input={owner_names}, type={type(owner_names).__name__}", file=sys.stderr)
    
    if isinstance(owner_names, str):
        owner_names = [owner_names]
    ...
```

在 **_get_owner_refs** 返回前（第226行前）添加：

```python
        return EntityReferenceList(root=all_owners)
        
    # 🔍 临时调试（在return前）
    import sys
    if all_owners:
        print(f"🔍 [GET_REFS] returning {len(all_owners)} owners: {[o.name for o in all_owners]}", file=sys.stderr)
    
    return EntityReferenceList(root=all_owners)
```

#### 步骤 3: 运行测试

```bash
cd ~/workspaces/OpenMetadata

# 清除缓存
find ingestion/src -name "*.pyc" -delete

# 运行并过滤调试输出
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml 2>&1 | grep "🔍"
```

### 期望的调试输出

**正确的输出应该是**：

```
🔍 [DB] names=['alice', 'bob'], stored=['alice', 'bob'], type=list
🔍 [RESOLVE] entity=accounting, parent=['alice', 'bob'], type=list
🔍 [GET_REFS] input=['alice', 'bob'], type=list
🔍 [GET_REFS] returning 2 owners: ['alice', 'bob']
🔍 [RESOLVE] got 2 owners: ['alice', 'bob']
```

**如果输出有问题，可能看到**：

```
🔍 [DB] names=['alice', 'bob'], stored=alice, type=str  ← 问题！只存储了字符串
或
🔍 [RESOLVE] entity=accounting, parent=alice, type=str  ← 问题！只传递了字符串
或  
🔍 [GET_REFS] returning 1 owners: ['alice']  ← 问题！只返回了1个
```

### 分析结果

根据输出的不同位置，可以定位问题：

1. **如果 `[DB] stored` 是字符串而不是列表**：
   - 问题在 `common_db_source.py` 的存储逻辑
   - 检查第225-228行的代码

2. **如果 `[RESOLVE] parent` 是字符串而不是列表**：
   - 问题在从 context 获取值的过程
   - 检查 `database_service.py` 的 `get_schema_owner_ref` 函数

3. **如果 `[GET_REFS] input` 是字符串**：
   - 问题在调用 `_get_owner_refs` 时的参数传递

4. **如果 `[GET_REFS] returning` 只有1个owner**：
   - 问题在 `_get_owner_refs` 内部逻辑
   - 可能是查找失败或验证逻辑问题

---

## 方法 2: 使用自动脚本添加调试（如果不想手动编辑）

```bash
cd ~/workspaces/OpenMetadata

# 运行自动添加脚本
bash /workspace/add_debug_output.sh

# 运行测试
metadata ingest -c test-03-multiple-users.yaml 2>&1 | grep "🔍"

# 恢复原文件（调试完成后）
mv ingestion/src/metadata/ingestion/source/database/common_db_source.py.bak \
   ingestion/src/metadata/ingestion/source/database/common_db_source.py
   
mv ingestion/src/metadata/utils/owner_utils.py.bak \
   ingestion/src/metadata/utils/owner_utils.py
```

---

## 🔍 其他可能的问题点

### 检查 database_service.py

查看 `get_schema_owner_ref` 函数如何获取 `parent_owner`：

```bash
grep -A 10 "def get_schema_owner_ref" ingestion/src/metadata/ingestion/source/database/database_service.py
```

**关键代码**（应该在第620-630行左右）：

```python
def get_schema_owner_ref(self, schema_name: str) -> Optional[EntityReferenceList]:
    try:
        # Get parent owner from context
        parent_owner = getattr(self.context.get(), "database_owner", None)
        
        # ...
        owner_ref = get_owner_from_config(
            # ...
            parent_owner=parent_owner,  # ← 这里应该传递列表
        )
```

确认 `parent_owner` 传递时是完整的列表。

---

## 📋 完整调试清单

请运行调试后，告诉我：

1. **Database 存储**: `🔍 [DB]` 显示什么？
2. **Schema 继承**: `🔍 [RESOLVE] parent=` 是什么？
3. **查找结果**: `🔍 [GET_REFS] returning` 是多少个？

根据这些信息，我们可以精确定位问题！
