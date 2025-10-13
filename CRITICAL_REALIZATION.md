# 💡 关键发现

## 🎯 真正的问题所在

您说：
> "我只修改了json文件，没有修改datamodel_generation.py"

**这就是问题！**

### 问题分析

1. **您修改了 JSON Schema** (`ownerConfig.json`)
   - 添加了对数组的支持
   - 使用 `$ref` 和 `definitions`

2. **但是 Pydantic 模型没有重新生成！**
   - 旧的 Pydantic 模型还是 `Dict[str, str]`（不支持数组）
   - 新的 JSON Schema 定义是 `Dict[str, Union[str, List[str]]]`

3. **结果**：
   - YAML 配置：`database: {"finance_db": ["alice", "bob"]}`
   - Pydantic 验证：**把数组转换成了字符串** `"alice"` 或报错
   - 所以 ownerConfig.database 里就只有字符串形式的值

### 为什么会转换成 "alice"？

当 Pydantic 模型期望 `str` 但收到 `List[str]` 时：
- 可能取列表的第一个元素
- 或者调用 `str(["alice", "bob"])` 得到字符串表示
- 或者直接报错（但可能被捕获了）

## ✅ 解决方案

### 步骤 1: 重新生成 Pydantic 模型（必须！）

```bash
cd ~/workspaces/OpenMetadata/openmetadata-spec

# 这一步会根据 JSON Schema 重新生成 Pydantic 模型
mvn clean install
```

**这会做什么**：
- 读取 `ownerConfig.json`（您修改过的版本）
- 使用 `datamodel-code-generator` 生成 Python 代码
- 生成的模型会支持 `Union[str, List[str]]`

### 步骤 2: 重新安装 ingestion

```bash
cd ~/workspaces/OpenMetadata/ingestion

# 强制重新安装，使用新生成的模型
pip install -e . --force-reinstall --no-deps
```

### 步骤 3: 验证

```bash
# 运行测试
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml

# 检查结果
curl -s "http://localhost:8585/api/v1/databaseSchemas/name/postgres-test-03-multiple-users.finance_db.accounting" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners | length'

# 期望：2（而不是1）
```

## 🔍 为什么之前的修改没用？

### 我们修改的代码（`common_db_source.py`）：

```python
database_owner_names = [owner.name for owner in database_owner_ref.root]
database_owner = database_owner_names[0] if len(database_owner_names) == 1 else database_owner_names
```

**这段代码是正确的！**

### 但是它依赖于：

```python
database_owner_ref = self.get_database_owner_ref(database_name)
```

这个函数调用：

```python
owner_ref = get_owner_from_config(
    metadata=self.metadata,
    owner_config=self.source_config.ownerConfig,  # ← 这里！
    ...
)
```

### 关键：`self.source_config.ownerConfig`

这是一个 **Pydantic 模型实例**！

如果 Pydantic 模型定义是：
```python
class OwnerConfig(BaseModel):
    database: Optional[Union[str, Dict[str, str]]]  # ← 旧模型，不支持 List
```

那么当配置是：
```yaml
database:
  "finance_db": ["alice", "bob"]
```

Pydantic 验证时会：
- **拒绝这个配置**（ValidationError）
- 或者**转换成字符串**（取第一个元素）
- 导致 `ownerConfig.database` 实际上是 `{"finance_db": "alice"}`

所以后续代码获取到的就只有1个owner！

## 📊 数据流示意图

### 当前状态（错误）

```
YAML配置: ["alice", "bob"]
    ↓
Pydantic验证（旧模型，不支持List）
    ↓
转换/丢失: "alice"  ← 问题在这里！
    ↓
ownerConfig.database = {"finance_db": "alice"}
    ↓
get_owner_from_config 只能拿到1个owner
    ↓
database_owner_ref.root = [EntityReference(alice)]  ← 只有1个
    ↓
context 存储 "alice"
    ↓
schema 继承 "alice"
```

### 修复后（正确）

```
YAML配置: ["alice", "bob"]
    ↓
Pydantic验证（新模型，支持List）✅
    ↓
保持原样: ["alice", "bob"]  ← 正确！
    ↓
ownerConfig.database = {"finance_db": ["alice", "bob"]}
    ↓
get_owner_from_config 拿到2个owner
    ↓
database_owner_ref.root = [EntityReference(alice), EntityReference(bob)]  ← 2个
    ↓
context 存储 ["alice", "bob"]
    ↓
schema 继承 ["alice", "bob"]  ← 2个owner！
```

## 🎯 总结

**问题根源**：Pydantic 模型没有重新生成，配置解析时就丢失了数据。

**解决方法**：运行 `mvn clean install` 重新生成模型。

**我们之前的修改**（`common_db_source.py`, `owner_utils.py`）都是**正确且必要的**，但它们需要配合重新生成的 Pydantic 模型才能工作！
