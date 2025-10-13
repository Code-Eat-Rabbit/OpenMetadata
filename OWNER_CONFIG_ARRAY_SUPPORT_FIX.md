# Owner Config 数组支持修复指南

## 🔴 问题诊断

**错误信息**：
```
ValidationError: ownerConfig.database.dict[str,str].finance_db
  Input should be a valid string [type=string_type, input_value=['alice', 'bob'], input_type=list]
```

**根本原因**：
- JSON Schema (`ownerConfig.json`) **正确定义**了数组支持
- 但生成的 Pydantic 模型当前只支持 `Union[str, Dict[str, str]]`
- 需要支持 `Union[str, Dict[str, Union[str, List[str]]]]`

## ✅ 解决方案

### 选项 1: 重新生成 Pydantic 模型（推荐，永久解决）

#### 步骤 1: 重新生成模型

```bash
# 从 OpenMetadata 根目录
cd openmetadata-spec

# 清理并重新生成所有模型
mvn clean install

# 这会从 JSON Schema 重新生成 Python Pydantic 模型
```

#### 步骤 2: 重新安装 ingestion 包

```bash
cd ../ingestion

# 重新安装以使用新生成的模型
pip install -e . --force-reinstall --no-deps
```

#### 步骤 3: 验证修复

```bash
# 运行 test-03 验证数组支持
metadata ingest -c tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml
```

### 选项 2: 临时修改测试配置（快速测试）

如果无法立即重新生成模型，可以临时修改测试文件使用单个 owner：

#### 修改 test-03-multiple-users.yaml

```yaml
# 原始（报错）
ownerConfig:
  database:
    "finance_db": ["alice", "bob"]  # ❌ 数组

# 临时修改
ownerConfig:
  database:
    "finance_db": "alice"  # ✅ 单个字符串
```

#### 修改 test-04-validation-errors.yaml

```yaml
# 原始（报错）
ownerConfig:
  database:
    "finance_db": ["finance-team", "audit-team", "compliance-team"]  # ❌
  table:
    "finance_db.accounting.revenue": ["alice", "bob", "finance-team"]  # ❌

# 临时修改
ownerConfig:
  database:
    "finance_db": "finance-team"  # ✅
  table:
    "finance_db.accounting.revenue": "alice"  # ✅
```

#### 修改 test-07-partial-success.yaml

```yaml
# 原始（报错）
ownerConfig:
  table:
    "finance_db.accounting.revenue": ["alice", "nonexistent-user-1", "bob", "nonexistent-user-2"]  # ❌

# 临时修改
ownerConfig:
  table:
    "finance_db.accounting.revenue": "alice"  # ✅
    "finance_db.accounting.budgets": "nonexistent-user-1"  # ✅ 测试不存在的owner
```

#### 修改 test-08-complex-mixed.yaml

```yaml
# 原始（报错）
ownerConfig:
  database:
    "marketing_db": ["marketing-user-1", "marketing-user-2"]  # ❌
  databaseSchema:
    "finance_db.accounting": ["alice", "bob"]  # ❌
  table:
    "finance_db.accounting.revenue": ["charlie", "david", "emma"]  # ❌

# 临时修改
ownerConfig:
  database:
    "marketing_db": "marketing-user-1"  # ✅
  databaseSchema:
    "finance_db.accounting": "alice"  # ✅
  table:
    "finance_db.accounting.revenue": "charlie"  # ✅
```

### 选项 3: 检查现有 Pydantic 模型定义

检查当前模型是否已支持数组：

```bash
# 查找 OwnerConfig 相关的生成代码
find ingestion -name "*.py" -path "*/generated/*" | xargs grep -l "OwnerConfig" 2>/dev/null

# 或者检查编译后的包
python3 -c "from metadata.generated.schema.type.ownerConfig import OwnerConfig; import inspect; print(inspect.getsource(OwnerConfig))"
```

## 🔧 验证步骤

### 1. 检查 JSON Schema 定义

```bash
# JSON Schema 应该包含 oneOf 数组支持
cat openmetadata-spec/src/main/resources/json/schema/type/ownerConfig.json | jq '.properties.database.oneOf[1].additionalProperties'
```

期望输出：
```json
{
  "oneOf": [
    { "type": "string" },
    { 
      "type": "array",
      "items": { "type": "string" }
    }
  ]
}
```

### 2. 运行测试验证

```bash
# Test 1-2 应该正常（不使用数组）
metadata ingest -c tests/unit/metadata/ingestion/owner_config_tests/test-01-basic-configuration.yaml
metadata ingest -c tests/unit/metadata/ingestion/owner_config_tests/test-02-fqn-matching.yaml

# Test 3-4, 7-8 需要数组支持
metadata ingest -c tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml
```

## 📋 技术细节

### JSON Schema 到 Pydantic 转换

**JSON Schema 定义**：
```json
{
  "database": {
    "oneOf": [
      { "type": "string" },
      {
        "type": "object",
        "additionalProperties": {
          "oneOf": [
            { "type": "string" },
            { "type": "array", "items": { "type": "string" } }
          ]
        }
      }
    ]
  }
}
```

**期望的 Pydantic 模型**：
```python
from typing import Union, Dict, List
from pydantic import BaseModel, Field

class OwnerConfig(BaseModel):
    database: Union[
        str,  # Single owner for all databases
        Dict[str, Union[str, List[str]]]  # Map of db names to owner(s)
    ] = Field(None)
```

**当前可能的模型**（缺少 List 支持）：
```python
database: Union[str, Dict[str, str]] = Field(None)  # ❌ 不支持 List[str]
```

## 🎯 推荐行动

1. **立即**：使用选项 2 临时修改测试配置，验证 test 1-2, 5-6 可以正常运行
2. **短期**：重新生成 Pydantic 模型（选项 1）
3. **长期**：确保 CI/CD 流程包含模型生成验证

## ⚠️ 注意事项

1. 重新生成模型后，需要重新安装 ingestion 包
2. 如果修改了 JSON Schema，务必运行 `mvn clean install` 而不是 `mvn install`
3. 测试前确保所有用户和团队已创建（运行 `setup-test-entities.sh`）

## 🔗 相关文件

- JSON Schema: `openmetadata-spec/src/main/resources/json/schema/type/ownerConfig.json`
- Owner Utils: `ingestion/src/metadata/utils/owner_utils.py`
- Test 配置: `ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-0*.yaml`

## 📞 获取帮助

如果重新生成模型后问题仍然存在，请检查：
1. Maven 生成日志中是否有错误
2. Pydantic 版本是否兼容（需要 Pydantic 2.x）
3. JSON Schema 定义是否正确
