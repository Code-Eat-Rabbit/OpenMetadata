# 多Owner配置 - Pydantic 2.11.9 完整解决方案

## 🎯 目标

保持多owner配置功能，同时完全兼容 Pydantic 2.11.9，避免 RootModel 错误。

## ✅ 解决方案：使用 $ref 和 definitions

### 核心思路

**问题根源**：嵌套的 `oneOf` 导致 datamodel-code-generator 生成 RootModel

**解决方案**：使用 JSON Schema 的 `definitions` 和 `$ref` 机制

### 优化后的 Schema 结构

```json
{
  "definitions": {
    "ownerValue": {
      "anyOf": [
        { "type": "string" },           // 单个owner
        { 
          "type": "array",              // 多个owner
          "items": { "type": "string" },
          "minItems": 1
        }
      ]
    }
  },
  "properties": {
    "database": {
      "anyOf": [
        { "type": "string" },           // 所有database用一个owner
        {
          "type": "object",             // 每个database不同owner
          "additionalProperties": {
            "$ref": "#/definitions/ownerValue"  // ← 引用definition
          }
        }
      ]
    }
  }
}
```

**为什么这样可以避免 RootModel**：
- `$ref` 引用会被展开为普通的类型定义
- 避免了嵌套的 `oneOf` 结构
- datamodel-code-generator 生成 `Union[str, List[str]]` 而不是 RootModel

### 预期生成的 Pydantic 模型

```python
from typing import Union, Dict, List, Optional, Any
from pydantic import BaseModel, Field

# 这个可能会生成，也可能被内联
OwnerValue = Union[str, List[str]]

class OwnerConfig(BaseModel):
    default: Optional[str] = Field(None, description="...")
    database: Optional[Union[str, Dict[str, Union[str, List[str]]]]] = Field(None)
    databaseSchema: Optional[Union[str, Dict[str, Union[str, List[str]]]]] = Field(None)
    table: Optional[Union[str, Dict[str, Union[str, List[str]]]]] = Field(None)
    enableInheritance: Optional[bool] = Field(True)
```

**关键**：不会生成 RootModel！

## 🚀 执行步骤

### 第 1 步：应用新 Schema（已完成）

我已经修改了 `ownerConfig.json`，使用 `$ref` 和 `definitions`。

### 第 2 步：重新生成 Pydantic 模型

```bash
cd ~/workspaces/OpenMetadata/openmetadata-spec

# 清理并重新生成
mvn clean install
```

**观察输出**：
- 应该**不再**出现 RootModel 相关的修复信息（或者只修复其他文件）
- BUILD SUCCESS

### 第 3 步：验证生成的模型

```bash
cd ~/workspaces/OpenMetadata

# 查看生成的 ownerConfig.py
cat ingestion/src/metadata/generated/schema/type/ownerConfig.py | head -100
```

**检查要点**：
- ✅ 应该看到 `class OwnerConfig(BaseModel):` 而不是 `RootModel`
- ✅ 应该看到 `Union[str, List[str]]` 类型
- ❌ **不应该**看到 `class Database(RootModel...)`
- ❌ **不应该**看到 `model_config = ConfigDict(extra="forbid")` 在任何类中

### 第 4 步：重新安装 ingestion

```bash
cd ingestion
pip install -e . --force-reinstall --no-deps
```

### 第 5 步：验证多owner配置支持

```bash
# 测试 Python 代码能否解析多owner配置
python3 << 'EOF'
from metadata.generated.schema.type.ownerConfig import OwnerConfig
import json

# 测试1：单个owner（字符串）
config1 = OwnerConfig(
    default="data-team",
    database="db-admin"
)
print(f"✅ Test 1 (single string): {config1.database}")

# 测试2：字典+单个owner
config2 = OwnerConfig(
    default="data-team",
    database={
        "sales_db": "sales-team",
        "finance_db": "finance-team"
    }
)
print(f"✅ Test 2 (dict with string): {config2.database}")

# 测试3：字典+数组（多个owner）
config3 = OwnerConfig(
    default="data-team",
    database={
        "shared_db": ["alice", "bob", "charlie"]
    },
    table={
        "orders": ["user1", "user2"],
        "customers": "customer-team"
    }
)
print(f"✅ Test 3 (dict with array): {config3.database}")
print(f"✅ Test 3 (table mixed): {config3.table}")

# 测试4：model_dump 能正确序列化
dumped = config3.model_dump(exclude_none=True)
print(f"✅ Test 4 (model_dump): {json.dumps(dumped, indent=2)}")

print("\n🎉 All Pydantic validation tests passed!")
print("Multiple owners are fully supported!")
EOF
```

**如果成功**，应该看到：
```
✅ Test 1 (single string): db-admin
✅ Test 2 (dict with string): {'sales_db': 'sales-team', 'finance_db': 'finance-team'}
✅ Test 3 (dict with array): {'shared_db': ['alice', 'bob', 'charlie']}
✅ Test 3 (table mixed): {'orders': ['user1', 'user2'], 'customers': 'customer-team'}
✅ Test 4 (model_dump): {...}

🎉 All Pydantic validation tests passed!
Multiple owners are fully supported!
```

### 第 6 步：运行完整测试套件

```bash
cd ~/workspaces/OpenMetadata

# Test 3 - 多个users（应该完全工作）
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml

# Test 4 - 验证错误（多个teams、混合类型）
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-04-validation-errors.yaml

# Test 7 - 部分成功
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-07-partial-success.yaml

# Test 8 - 复杂混合
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-08-complex-mixed.yaml
```

## 📋 支持的配置格式

### ✅ 完全支持所有格式

```yaml
ownerConfig:
  # 格式1：字符串（所有实体同一个owner）
  database: "database-admin"
  
  # 格式2：字典+字符串（每个实体单个owner）
  database:
    "sales_db": "sales-team"
    "finance_db": "finance-team"
  
  # 格式3：字典+数组（多个owner）
  database:
    "shared_db": ["alice", "bob", "charlie"]  # ✅ 多个users
  
  # 格式4：混合使用
  table:
    "orders": ["user1", "user2"]              # ✅ 多个users
    "customers": "customer-team"              # ✅ 单个team
    "products": ["alice"]                     # ✅ 单个user（数组形式）
  
  enableInheritance: true
```

### ✅ 业务规则验证（在运行时）

```yaml
# ✅ 允许：多个users
database:
  "shared_db": ["alice", "bob", "charlie"]

# ⚠️  警告：多个teams（只用第一个）
database:
  "finance_db": ["finance-team", "audit-team", "compliance-team"]
# WARNING: Only ONE team allowed, using first team: finance-team

# ❌ 错误：混合users和teams（跳过配置）
table:
  "orders": ["alice", "bob", "sales-team"]
# WARNING: Cannot mix users and teams, skipping configuration
```

## 🔍 验证 Schema 正确性

### 测试 JSON Schema

```bash
cd ~/workspaces/OpenMetadata

# 使用 jsonschema 验证
python3 << 'EOF'
import json
import jsonschema

# 加载 schema
with open('openmetadata-spec/src/main/resources/json/schema/type/ownerConfig.json') as f:
    schema = json.load(f)

# 测试数据1：单个owner
data1 = {
    "default": "data-team",
    "database": "db-admin"
}
jsonschema.validate(data1, schema)
print("✅ Single owner validated")

# 测试数据2：字典+数组
data2 = {
    "default": "data-team",
    "database": {
        "sales_db": "sales-team",
        "shared_db": ["alice", "bob", "charlie"]
    },
    "table": {
        "orders": ["user1", "user2"],
        "customers": "customer-team"
    },
    "enableInheritance": True
}
jsonschema.validate(data2, schema)
print("✅ Multiple owners validated")

print("\n🎉 JSON Schema is valid and supports all formats!")
EOF
```

## 🐛 故障排查

### 如果仍然出现 RootModel 错误

**原因**: datamodel-code-generator 可能仍然生成 RootModel

**解决**：

#### 方案 A：检查生成的代码

```bash
# 查看生成的 ownerConfig.py
cat ingestion/src/metadata/generated/schema/type/ownerConfig.py | grep -A 10 "class.*RootModel"

# 如果仍然有 RootModel，使用自动修复脚本
python3 scripts/datamodel_generation.py
```

#### 方案 B：手动修复（如果自动修复失败）

```bash
# 备份
cp ingestion/src/metadata/generated/schema/type/ownerConfig.py \
   ingestion/src/metadata/generated/schema/type/ownerConfig.py.bak

# 编辑文件，移除 RootModel 的 model_config
vi ingestion/src/metadata/generated/schema/type/ownerConfig.py
```

#### 方案 C：使用完全自定义的模型（最后手段）

如果自动生成无法满足需求，可以创建自定义模型：

```python
# 文件：ingestion/src/metadata/ingestion/models/owner_config.py
from typing import Union, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator

OwnerValue = Union[str, List[str]]
OwnerMapping = Dict[str, OwnerValue]

class OwnerConfig(BaseModel):
    """Custom OwnerConfig model with full array support"""
    
    default: Optional[str] = Field(None, description="Default owner")
    service: Optional[str] = Field(None)
    database: Optional[Union[str, OwnerMapping]] = Field(None)
    databaseSchema: Optional[Union[str, OwnerMapping]] = Field(None)
    table: Optional[Union[str, OwnerMapping]] = Field(None)
    enableInheritance: Optional[bool] = Field(True)
    
    model_config = {"extra": "forbid"}  # ← 这里可以设置，因为不是RootModel
```

然后在代码中使用自定义模型而不是生成的模型。

## 📊 方案对比

| 方案 | 多owner支持 | RootModel问题 | 实施难度 | 推荐度 |
|------|------------|--------------|----------|--------|
| **使用 $ref + definitions** | ✅ 完全支持 | ✅ 应该避免 | ⭐ 简单 | ⭐⭐⭐⭐⭐ |
| **自动修复脚本** | ✅ 完全支持 | ⚠️ 需要修复 | ⭐⭐ 中等 | ⭐⭐⭐⭐ |
| **自定义模型** | ✅ 完全支持 | ✅ 完全避免 | ⭐⭐⭐ 复杂 | ⭐⭐⭐ |
| **简化Schema** | ❌ 不支持 | ✅ 完全避免 | ⭐ 简单 | ⭐⭐ |

## 🎯 推荐执行

### 当前方案（$ref + definitions）

我已经修改了 `ownerConfig.json`，使用 `$ref` 引用 `definitions/ownerValue`。

**现在执行**：

```bash
cd ~/workspaces/OpenMetadata/openmetadata-spec
mvn clean install

cd ../ingestion
pip install -e . --force-reinstall --no-deps

# 验证
python3 -c "
from metadata.generated.schema.type.ownerConfig import OwnerConfig

config = OwnerConfig(
    default='team1',
    database={'shared_db': ['alice', 'bob', 'charlie']}
)
print(f'✅ Multiple owners supported: {config.database}')
"
```

**如果成功**：
- ✅ 无 RootModel 错误
- ✅ 支持数组形式的owner
- ✅ 完全兼容原始设计

**如果仍有问题**：执行方案B（下面）

## 🛡️ 备用方案：自动修复 + 自定义处理

如果 datamodel-code-generator 仍然生成 RootModel，我们有双重保险：

### 保险1：datamodel_generation.py 自动修复

我已经在 `scripts/datamodel_generation.py` 中添加了自动修复逻辑（第102-131行）：

```python
# Fix RootModel model_config issue for Pydantic 2.x
# 自动扫描并修复所有 RootModel
```

每次运行 `mvn clean install` 都会自动修复。

### 保险2：运行时类型处理

`owner_utils.py` 已经正确处理 `Union[str, List[str]]`：

```python
# owner_utils.py 第159-160行
if isinstance(owner_names, str):
    owner_names = [owner_names]
```

无论 Pydantic 模型如何定义，只要能传递 `str` 或 `List[str]`，代码都能正确处理。

## 🧪 完整测试验证

### 测试1：验证 Pydantic 模型

```bash
cd ~/workspaces/OpenMetadata

python3 << 'EOF'
from metadata.generated.schema.type.ownerConfig import OwnerConfig
import traceback

test_cases = [
    ("Single string", {"default": "team1", "database": "db-owner"}),
    ("Dict with string", {"database": {"sales_db": "sales-team"}}),
    ("Dict with array", {"database": {"shared": ["alice", "bob"]}}),
    ("Mixed", {
        "database": {"db1": "team1", "db2": ["user1", "user2"]},
        "table": {"t1": "owner1", "t2": ["owner2", "owner3"]}
    }),
]

passed = 0
failed = 0

for name, config_dict in test_cases:
    try:
        config = OwnerConfig(**config_dict)
        print(f"✅ {name}: OK")
        passed += 1
    except Exception as e:
        print(f"❌ {name}: {e}")
        traceback.print_exc()
        failed += 1

print(f"\n{'='*60}")
print(f"Results: {passed} passed, {failed} failed")
if failed == 0:
    print("🎉 All tests passed! Multiple owners fully supported!")
else:
    print("⚠️  Some tests failed. Check errors above.")
EOF
```

### 测试2：运行实际ingestion

```bash
# Test 3 - 多个users
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml

# 检查结果
JWT_TOKEN="your_token"
curl -X GET "http://localhost:8585/api/v1/databases/name/postgres-test-03-multiple-users.finance_db" \
  -H "Authorization: Bearer $JWT_TOKEN" | jq '.owners[]'

# 期望看到 alice 和 bob 两个owners
```

## 🎓 技术细节

### JSON Schema $ref 的优势

**使用 $ref**：
```json
{
  "definitions": {
    "ownerValue": { "anyOf": [...] }
  },
  "properties": {
    "database": {
      "additionalProperties": {
        "$ref": "#/definitions/ownerValue"  // ← 引用
      }
    }
  }
}
```

**生成的代码**（预期）：
```python
# 不会生成 RootModel
OwnerValue = Union[str, List[str]]  # 可能是这样

class OwnerConfig(BaseModel):
    database: Optional[Union[str, Dict[str, Union[str, List[str]]]]]
    # 或者
    database: Optional[Union[str, Dict[str, OwnerValue]]]
```

### 为什么 $ref 能避免 RootModel

1. **引用定义**而不是内联 `oneOf`
2. datamodel-code-generator 将 `$ref` 展开为类型别名或直接内联
3. 不会为 `anyOf` 创建单独的 RootModel 类

## ⚠️ 如果方案仍然失败

### 最终方案：完全自定义模型

创建文件：`ingestion/src/metadata/ingestion/models/owner_config_custom.py`

```python
"""Custom OwnerConfig model for Pydantic 2.11.9 compatibility"""
from typing import Union, Dict, List, Optional
from pydantic import BaseModel, Field

# Type aliases for clarity
OwnerValue = Union[str, List[str]]
OwnerMapping = Dict[str, OwnerValue]
OwnerField = Union[str, OwnerMapping]

class OwnerConfig(BaseModel):
    """
    Owner Configuration for metadata ingestion.
    
    Supports:
    - Single owner for all entities (string)
    - Specific owner per entity (dict)
    - Multiple owners per entity (array)
    
    Business rules enforced at runtime:
    - Multiple users allowed
    - Only ONE team allowed
    - Users and teams are mutually exclusive
    """
    
    default: Optional[str] = Field(
        None,
        description="Default owner for all entities"
    )
    
    service: Optional[str] = Field(
        None,
        description="Owner for service level"
    )
    
    database: Optional[OwnerField] = Field(
        None,
        description="Owner for databases"
    )
    
    databaseSchema: Optional[OwnerField] = Field(
        None,
        alias="databaseSchema",
        description="Owner for schemas"
    )
    
    table: Optional[OwnerField] = Field(
        None,
        description="Owner for tables"
    )
    
    enableInheritance: Optional[bool] = Field(
        True,
        description="Enable inheritance from parent entities"
    )
    
    model_config = {"extra": "forbid"}
```

**使用自定义模型**：

修改 `owner_utils.py`（第264-268行）：

```python
# 添加导入
from metadata.ingestion.models.owner_config_custom import OwnerConfig as CustomOwnerConfig

# 修改 get_owner_from_config 函数
def get_owner_from_config(...):
    # 如果是自动生成的模型有问题，转换为自定义模型
    if hasattr(owner_config, "model_dump"):
        config_dict = owner_config.model_dump(exclude_none=True)
        # 尝试使用自定义模型重新验证
        try:
            custom_config = CustomOwnerConfig(**config_dict)
            config_dict = custom_config.model_dump(exclude_none=True)
        except:
            pass  # 如果失败，继续使用原始dict
        
        resolver = OwnerResolver(metadata, config_dict)
        return resolver.resolve_owner(entity_type, entity_name, parent_owner)
```

但这**只是后备方案**，应该首先尝试修复自动生成。

## ✅ 总结

**推荐路径**（按优先级）：

1. **首先尝试**: 使用新的 $ref schema → `mvn clean install` → 测试
2. **如果失败**: 检查 datamodel_generation.py 自动修复是否运行
3. **最后手段**: 使用完全自定义的 OwnerConfig 模型

**预期结果**：
- ✅ 完全支持多owner配置（数组形式）
- ✅ 兼容 Pydantic 2.11.9
- ✅ 无 RootModel 错误
- ✅ 所有8个测试通过

立即执行第1步试试？
