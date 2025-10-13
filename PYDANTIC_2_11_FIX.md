# Pydantic 2.11.9 兼容性修复方案

## 🎯 问题分析

**当前版本**: Pydantic 2.11.9

**问题根源**:
1. JSON Schema 使用嵌套的 `oneOf` 定义（string | array）
2. datamodel-code-generator 为此生成 RootModel
3. Pydantic 2.x 的 RootModel **不支持** `model_config['extra']`

**错误示例**:
```python
# datamodel-code-generator 生成的代码
class Database(RootModel[Union[str, Dict[str, Union[str, List[str]]]]]):
    model_config = ConfigDict(extra="forbid")  # ❌ RootModel 不支持这个
    root: Union[str, Dict[str, Union[str, List[str]]]]
```

## ✅ 解决方案

### 方案 1: 简化 Schema（推荐，立即可用）⭐

**核心思路**: 移除嵌套的 `oneOf`，只支持字符串形式的 owner，避免生成 RootModel

#### 修改内容

**替换文件**: `openmetadata-spec/src/main/resources/json/schema/type/ownerConfig.json`

**关键改动**:

```json
// 修改前（导致 RootModel）:
"database": {
  "oneOf": [
    { "type": "string" },
    {
      "type": "object",
      "additionalProperties": {
        "oneOf": [              // ← 嵌套的 oneOf 导致 RootModel
          { "type": "string" },
          { "type": "array", "items": { "type": "string" } }
        ]
      }
    }
  ]
}

// 修改后（避免 RootModel）:
"database": {
  "anyOf": [                    // ← 使用 anyOf
    { "type": "string" },
    {
      "type": "object",
      "additionalProperties": {
        "type": "string"        // ← 只支持字符串，移除数组
      }
    }
  ]
}
```

**优点**:
- ✅ 不生成 RootModel
- ✅ 完全兼容 Pydantic 2.11.9
- ✅ 生成简单的 Union 类型
- ✅ 立即可用，无需额外配置

**缺点**:
- ⚠️ 暂时不支持数组形式的多个 owner（如 `["alice", "bob"]`）
- ⚠️ 只能配置单个 owner（字符串形式）

**生成的 Pydantic 模型**:
```python
from typing import Union, Dict, Optional
from pydantic import BaseModel, Field

class OwnerConfig(BaseModel):
    default: Optional[str] = Field(None, description="...")
    database: Optional[Union[str, Dict[str, str]]] = Field(None)  # ✅ 简单的 Union
    databaseSchema: Optional[Union[str, Dict[str, str]]] = Field(None)
    table: Optional[Union[str, Dict[str, str]]] = Field(None)
    enableInheritance: Optional[bool] = Field(True)
```

#### 实施步骤

```bash
cd ~/workspaces/OpenMetadata

# 1. 备份原文件
cp openmetadata-spec/src/main/resources/json/schema/type/ownerConfig.json \
   openmetadata-spec/src/main/resources/json/schema/type/ownerConfig.json.bak

# 2. 使用优化的 schema（我已创建）
cp /workspace/ownerConfig_optimized.json \
   openmetadata-spec/src/main/resources/json/schema/type/ownerConfig.json

# 3. 重新生成 Pydantic 模型
cd openmetadata-spec
mvn clean install

# 4. 重新安装 ingestion
cd ../ingestion
pip install -e . --force-reinstall --no-deps

# 5. 验证
python3 -c "from metadata.generated.schema.type import ownerConfig; print('✅ Success')"

# 6. 测试
cd ..
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-01-basic-configuration.yaml
```

### 方案 2: 继续使用自动修复脚本（临时方案）

如果不想修改 schema，可以继续使用自动修复：

```bash
# 使用现有的修复逻辑
cd ~/workspaces/OpenMetadata
python3 scripts/datamodel_generation.py

# scripts/datamodel_generation.py 已包含 RootModel 自动修复
```

### 方案 3: 未来支持数组（长期方案）

如果未来需要支持多个 owner（数组形式），需要：

1. **更复杂的 Schema 定义**（使用 discriminator）
2. **或者使用自定义 validator** 在 Python 代码中处理
3. **或者等待 datamodel-code-generator 改进**

## 📋 配置对比

### 简化后支持的配置

```yaml
ownerConfig:
  default: "data-platform-team"
  
  # ✅ 支持：字符串形式
  database: "database-admin"
  
  # ✅ 支持：字典映射（单个字符串值）
  database:
    "sales_db": "sales-team"
    "finance_db": "finance-team"
  
  databaseSchema:
    "sales_db.public": "public-team"
    "finance_db.accounting": "accounting-team"
  
  table:
    "sales_db.public.orders": "order-team"
    "finance_db.accounting.revenue": "revenue-team"
  
  enableInheritance: true
```

### 不再支持的配置

```yaml
ownerConfig:
  # ❌ 不支持：数组形式（多个 owner）
  database:
    "sales_db": ["alice", "bob", "charlie"]  # ❌ 报错
  
  table:
    "orders": ["user1", "user2"]  # ❌ 报错
```

**解决方法**: 如果需要多个 owner，选择其中一个主要负责人：
```yaml
# 从:
database:
  "sales_db": ["alice", "bob"]

# 改为:
database:
  "sales_db": "alice"  # 选择主要负责人
```

## 🔧 测试配置更新

由于简化后只支持单个 owner，需要更新测试配置：

### Test 1-2, 5-6: 无需修改 ✅
这些测试已经使用单个字符串，兼容新 schema

### Test 3: Multiple Users → 改为单个 owner

```yaml
# 文件: test-03-multiple-users.yaml

# 修改前:
ownerConfig:
  database:
    "finance_db": ["alice", "bob"]
  table:
    "finance_db.accounting.revenue": ["charlie", "david", "emma"]
    "finance_db.accounting.expenses": ["frank"]

# 修改后:
ownerConfig:
  database:
    "finance_db": "alice"  # ✅ 单个 owner
  table:
    "finance_db.accounting.revenue": "charlie"  # ✅
    "finance_db.accounting.expenses": "frank"  # ✅
```

### Test 4: Validation → 简化验证场景

```yaml
# 文件: test-04-validation-errors.yaml

# 修改前:
ownerConfig:
  database:
    "finance_db": ["finance-team", "audit-team", "compliance-team"]
  table:
    "finance_db.accounting.revenue": ["alice", "bob", "finance-team"]

# 修改后（测试其他验证场景）:
ownerConfig:
  database:
    "finance_db": "finance-team"  # ✅ 单个 team
  table:
    "finance_db.accounting.revenue": "alice"  # ✅
    "finance_db.accounting.budgets": "nonexistent-team"  # 测试不存在的 owner
```

### Test 7: Partial Success → 修改测试策略

```yaml
# 文件: test-07-partial-success.yaml

# 修改前:
ownerConfig:
  table:
    "finance_db.accounting.revenue": ["alice", "nonexistent-user-1", "bob"]

# 修改后（测试不存在的单个 owner）:
ownerConfig:
  table:
    "finance_db.accounting.revenue": "alice"  # ✅ 存在的 owner
    "finance_db.accounting.budgets": "nonexistent-user-1"  # ✅ 测试不存在
```

### Test 8: Complex Mixed → 简化配置

```yaml
# 文件: test-08-complex-mixed.yaml

# 修改前:
ownerConfig:
  database:
    "marketing_db": ["marketing-user-1", "marketing-user-2"]
  databaseSchema:
    "finance_db.accounting": ["alice", "bob"]
  table:
    "finance_db.accounting.revenue": ["charlie", "david", "emma"]

# 修改后:
ownerConfig:
  database:
    "marketing_db": "marketing-user-1"  # ✅
  databaseSchema:
    "finance_db.accounting": "alice"  # ✅
  table:
    "finance_db.accounting.revenue": "charlie"  # ✅
```

## 📊 方案对比

| 方案 | 优点 | 缺点 | 推荐度 |
|------|------|------|--------|
| **方案1: 简化Schema** | 彻底解决，无需修复脚本 | 不支持数组 | ⭐⭐⭐⭐⭐ |
| **方案2: 自动修复** | 保持原schema，支持数组 | 每次生成都需要修复 | ⭐⭐⭐ |
| **方案3: 等待改进** | 完美支持 | 时间不确定 | ⭐ |

## ✅ 推荐实施

**立即执行**（方案1）:

```bash
# 1. 使用简化的 schema
cp /workspace/ownerConfig_optimized.json \
   ~/workspaces/OpenMetadata/openmetadata-spec/src/main/resources/json/schema/type/ownerConfig.json

# 2. 重新生成
cd ~/workspaces/OpenMetadata/openmetadata-spec
mvn clean install

# 3. 重新安装
cd ../ingestion
pip install -e . --force-reinstall --no-deps

# 4. 验证
python3 -c "from metadata.generated.schema.type import ownerConfig; print('✅ Success')"

# 5. 运行测试
cd ..
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-05-inheritance-enabled.yaml
```

## 🎯 总结

**对于 Pydantic 2.11.9**:
- ✅ 方案1（简化Schema）是最干净的解决方案
- ✅ 完全兼容，无需额外修复脚本
- ✅ 代码生成稳定可靠
- ⚠️ 暂时牺牲数组支持（大多数场景单个owner已足够）

**未来如需数组支持**:
- 可以在 Python 代码层面实现（使用 validator）
- 或者使用更复杂的 discriminated union schema
- 或者等待 datamodel-code-generator 改进
