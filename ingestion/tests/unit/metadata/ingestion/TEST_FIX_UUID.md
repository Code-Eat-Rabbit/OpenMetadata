# UUID 验证错误修复

## 问题

运行测试时出现 Pydantic 验证错误：

```
pydantic_core._pydantic_core.ValidationError: 1 validation error for User
id
  Input should be a valid UUID, invalid character: expected an optional prefix of `urn:uuid:` 
  followed by [0-9a-fA-F-], found `u` at 1 [type=uuid_parsing, input_value='user-alice', input_type=str]
```

## 根本原因

User 和 Team 实体的 `id` 字段要求 UUID 格式，但代码使用了字符串：
```python
# ❌ 错误
id="user-" + name  # 'user-alice' 不是有效的 UUID
```

## 修复方案

### 1. 添加 uuid 模块导入

```python
import uuid
from typing import Any, Dict, List, Optional, Union
```

### 2. 修复 `_create_mock_user` 方法

```python
def _create_mock_user(self, name: str, email: str) -> User:
    """Create a mock User entity"""
    return User(
        id=uuid.uuid4(),  # ✅ 使用真实的 UUID
        name=EntityName(name),
        fullyQualifiedName=FullyQualifiedEntityName(name),
        email=Email(email),
        displayName=name.capitalize(),
    )
```

### 3. 修复 `_create_mock_team` 方法

```python
def _create_mock_team(self, name: str, display_name: str) -> Team:
    """Create a mock Team entity"""
    return Team(
        id=uuid.uuid4(),  # ✅ 使用真实的 UUID
        name=EntityName(name),
        fullyQualifiedName=FullyQualifiedEntityName(name),
        displayName=display_name,
        teamType="Group",
    )
```

## 验证

修复后，请重新运行测试：

```bash
cd ingestion
pytest tests/unit/metadata/ingestion/test_owner_config.py -v
```

预期结果：所有 10 个测试应该通过 ✅

## 修复状态

- ✅ 已添加 `import uuid`
- ✅ 已修复 `_create_mock_user()`
- ✅ 已修复 `_create_mock_team()`
- ✅ 通过 linter 检查（无错误）

## 参考

参考项目中其他测试如何创建 User/Team：
- `ingestion/tests/unit/topology/dashboard/test_grafana.py:545`
- `ingestion/tests/unit/topology/dashboard/test_powerbi.py:160`

使用 `uuid.uuid4()` 是标准做法。
