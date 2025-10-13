# RootModel 问题的根本解决方案

## 🎯 问题根源

通过分析 `scripts/datamodel_generation.py`，发现 OpenMetadata 使用 **datamodel-code-generator** 从 JSON Schema 生成 Pydantic 模型。

**代码生成命令**（第41行）：
```python
args = "--input openmetadata-spec/src/main/resources/json/schema \
        --output-model-type pydantic_v2.BaseModel \
        --use-annotated \
        --base-class metadata.ingestion.models.custom_pydantic.BaseModel \
        --input-file-type jsonschema \
        --output ingestion/src/metadata/generated/schema \
        --set-default-enum-member"
```

**问题**：
- `datamodel-code-generator` 为包含 `oneOf` 的复杂类型生成 `RootModel`
- 生成的 `RootModel` 类包含 `model_config = ConfigDict(extra="forbid")`
- Pydantic 2.x 的 `RootModel` **不支持** `model_config['extra']`

## ✅ 根本解决方案

### 方案 1: 修改代码生成脚本（推荐 ⭐）

在 `scripts/datamodel_generation.py` 中添加后处理步骤，自动移除 RootModel 的 model_config。

#### 实现步骤

**编辑文件**：`scripts/datamodel_generation.py`

在文件末尾添加（第101行之后）：

```python
# Fix RootModel model_config issue for Pydantic 2.x
# RootModel does not support model_config['extra']
# Issue: https://github.com/pydantic/pydantic/issues/xxxx
ROOTMODEL_FIX_FILE_PATHS = [
    f"{ingestion_path}src/metadata/generated/schema/type/ownerConfig.py",
    # 添加其他可能有 RootModel 问题的文件
]

def remove_rootmodel_config(file_path):
    """
    Remove model_config from RootModel classes as it's not supported in Pydantic 2.x
    
    Replaces:
        class SomeClass(RootModel[Type]):
            model_config = ConfigDict(...)
            root: Type = Field(...)
    
    With:
        class SomeClass(RootModel[Type]):
            root: Type = Field(...)
    """
    import re
    
    if not os.path.exists(file_path):
        print(f"Warning: File not found: {file_path}")
        return
    
    with open(file_path, "r", encoding=UTF_8) as file_:
        content = file_.read()
    
    # Pattern to match RootModel classes with model_config
    # Matches: class XXX(RootModel[...]):
    #              model_config = ConfigDict(...)
    pattern = r'(class\s+\w+\(RootModel\[[^\]]+\]\):)\s+(model_config\s*=\s*ConfigDict\([^)]*\)\s*)'
    
    # Remove model_config from RootModel classes
    fixed_content = re.sub(pattern, r'\1\n    ', content, flags=re.MULTILINE)
    
    if content != fixed_content:
        with open(file_path, "w", encoding=UTF_8) as file_:
            file_.write(fixed_content)
        print(f"Fixed RootModel in: {file_path}")
    else:
        print(f"No RootModel fixes needed in: {file_path}")

print("\n# Fixing RootModel model_config issues...")
for file_path in ROOTMODEL_FIX_FILE_PATHS:
    remove_rootmodel_config(file_path)
print("# RootModel fixes completed\n")
```

#### 自动发现需要修复的文件

更智能的实现（自动查找所有包含 RootModel 的文件）：

```python
# Automatically fix all RootModel issues
import glob

print("\n# Fixing RootModel model_config issues...")

# Find all generated Python files
generated_files = glob.glob(f"{ingestion_path}src/metadata/generated/**/*.py", recursive=True)

for file_path in generated_files:
    try:
        with open(file_path, "r", encoding=UTF_8) as file_:
            content = file_.read()
        
        # Check if file contains RootModel
        if "RootModel" in content and "model_config" in content:
            # Pattern to match RootModel classes with model_config
            pattern = r'(class\s+\w+\(RootModel\[[^\]]+\]\):)\s+(model_config\s*=\s*ConfigDict\([^)]*\)\s*)'
            fixed_content = re.sub(pattern, r'\1\n    ', content, flags=re.MULTILINE)
            
            if content != fixed_content:
                with open(file_path, "w", encoding=UTF_8) as file_:
                    file_.write(fixed_content)
                print(f"  ✓ Fixed: {file_path}")
    except Exception as e:
        print(f"  ✗ Error processing {file_path}: {e}")

print("# RootModel fixes completed\n")
```

### 方案 2: 修改 JSON Schema 定义（更彻底）

修改 `ownerConfig.json` 的 schema 定义，避免生成 RootModel。

**当前定义**（导致 RootModel）：
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

**改进定义**（避免 RootModel）：
```json
{
  "database": {
    "anyOf": [
      {
        "type": "string",
        "description": "Single owner for all databases"
      },
      {
        "type": "object",
        "description": "Map of database names to owner(s)",
        "patternProperties": {
          ".*": {
            "anyOf": [
              { "type": "string" },
              { 
                "type": "array",
                "items": { "type": "string" },
                "minItems": 1
              }
            ]
          }
        }
      }
    ]
  }
}
```

**区别**：
- 使用 `anyOf` 替代 `oneOf`（更宽松）
- 使用 `patternProperties` 替代 `additionalProperties`（更明确）

### 方案 3: datamodel-code-generator 配置参数

检查是否有参数可以控制 RootModel 的生成行为：

```python
# 在 datamodel_generation.py 第41行修改
args = f"--input {directory_root}openmetadata-spec/src/main/resources/json/schema \
        --output-model-type pydantic_v2.BaseModel \
        --use-annotated \
        --base-class metadata.ingestion.models.custom_pydantic.BaseModel \
        --input-file-type jsonschema \
        --output {ingestion_path}src/metadata/generated/schema \
        --set-default-enum-member \
        --collapse-root-models \  # ← 尝试这个参数（如果支持）
        --disable-extra \          # ← 或这个参数
        ".split(" ")
```

**注意**：需要查看 `datamodel-code-generator` 文档确认可用参数。

```bash
# 检查可用参数
datamodel-codegen --help | grep -i root
datamodel-codegen --help | grep -i extra
```

## 🚀 推荐实施步骤

### 步骤 1: 修改代码生成脚本（立即实施）

```bash
cd ~/workspaces/OpenMetadata

# 备份原文件
cp scripts/datamodel_generation.py scripts/datamodel_generation.py.bak

# 编辑文件
vi scripts/datamodel_generation.py
```

在文件末尾添加上面提供的 RootModel 修复代码。

### 步骤 2: 重新生成模型

```bash
# 运行生成脚本
python3 scripts/datamodel_generation.py

# 验证修复
python3 -c "from metadata.generated.schema.type import ownerConfig; print('✓ Import successful')"
```

### 步骤 3: 测试验证

```bash
# 运行测试
cd ~/workspaces/OpenMetadata
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-01-basic-configuration.yaml
```

### 步骤 4: 提交更改

```bash
git add scripts/datamodel_generation.py
git commit -m "fix: Auto-remove model_config from RootModel classes in code generation

RootModel in Pydantic 2.x does not support model_config['extra'].
Added post-processing step to automatically remove model_config from
all generated RootModel classes.

Fixes: #<issue_number>"
```

## 📋 完整修复代码

保存为 `fix_rootmodel_generation.py`，可以独立运行或集成到 `datamodel_generation.py`：

```python
#!/usr/bin/env python3
"""
Fix RootModel model_config issue in generated Pydantic models.
Can be run standalone or integrated into datamodel_generation.py
"""
import os
import re
import glob
import sys

UTF_8 = "UTF-8"

def remove_rootmodel_config(file_path, verbose=True):
    """
    Remove model_config from RootModel classes.
    
    Args:
        file_path: Path to Python file to fix
        verbose: Print progress messages
    
    Returns:
        bool: True if file was modified
    """
    if not os.path.exists(file_path):
        if verbose:
            print(f"Warning: File not found: {file_path}")
        return False
    
    with open(file_path, "r", encoding=UTF_8) as file_:
        content = file_.read()
    
    # Skip files without RootModel
    if "RootModel" not in content or "model_config" not in content:
        return False
    
    # Pattern: class XXX(RootModel[...]):
    #              model_config = ConfigDict(...)
    pattern = r'(class\s+\w+\(RootModel\[[^\]]+\]\):)\s+(model_config\s*=\s*ConfigDict\([^)]*\)\s*)'
    
    fixed_content = re.sub(pattern, r'\1\n    ', content, flags=re.MULTILINE)
    
    if content != fixed_content:
        with open(file_path, "w", encoding=UTF_8) as file_:
            file_.write(fixed_content)
        if verbose:
            print(f"  ✓ Fixed: {file_path}")
        return True
    
    return False

def fix_all_rootmodels(ingestion_path="./", verbose=True):
    """
    Find and fix all RootModel issues in generated files.
    
    Args:
        ingestion_path: Path to ingestion directory
        verbose: Print progress messages
    
    Returns:
        int: Number of files fixed
    """
    if verbose:
        print("\n# Fixing RootModel model_config issues...")
    
    generated_path = f"{ingestion_path}src/metadata/generated/**/*.py"
    generated_files = glob.glob(generated_path, recursive=True)
    
    fixed_count = 0
    for file_path in generated_files:
        try:
            if remove_rootmodel_config(file_path, verbose=verbose):
                fixed_count += 1
        except Exception as e:
            if verbose:
                print(f"  ✗ Error processing {file_path}: {e}")
    
    if verbose:
        print(f"# Fixed {fixed_count} file(s)\n")
    
    return fixed_count

if __name__ == "__main__":
    # Detect if running from ingestion directory
    current_dir = os.getcwd()
    ingestion_path = "./" if current_dir.endswith("/ingestion") else "ingestion/"
    
    print("="*60)
    print("RootModel model_config Fixer")
    print("="*60)
    print(f"Ingestion path: {ingestion_path}")
    
    fixed_count = fix_all_rootmodels(ingestion_path)
    
    print("="*60)
    if fixed_count > 0:
        print(f"✅ Successfully fixed {fixed_count} file(s)")
        print("\nNext: Run your tests to verify the fix")
        sys.exit(0)
    else:
        print("⚠️  No RootModel issues found (already fixed?)")
        sys.exit(0)
```

## 🎯 验证修复

### 自动化测试

创建测试脚本 `test_rootmodel_fix.py`：

```python
#!/usr/bin/env python3
"""Test that RootModel classes don't have model_config"""
import glob
import re
import sys

def test_no_rootmodel_config():
    """Verify no RootModel classes have model_config"""
    
    files_with_issues = []
    
    generated_files = glob.glob("ingestion/src/metadata/generated/**/*.py", recursive=True)
    
    for file_path in generated_files:
        with open(file_path, "r") as f:
            content = f.read()
        
        # Find RootModel classes with model_config
        pattern = r'class\s+(\w+)\(RootModel\[[^\]]+\]\):\s+model_config\s*='
        matches = re.findall(pattern, content, re.MULTILINE)
        
        if matches:
            files_with_issues.append((file_path, matches))
    
    if files_with_issues:
        print("❌ Found RootModel classes with model_config:")
        for file_path, classes in files_with_issues:
            print(f"  {file_path}: {', '.join(classes)}")
        sys.exit(1)
    else:
        print("✅ All RootModel classes are correctly configured")
        sys.exit(0)

if __name__ == "__main__":
    test_rootmodel_fix()
```

运行：
```bash
python3 test_rootmodel_fix.py
```

## 📚 集成到 CI/CD

在 `.github/workflows/` 或 CI 配置中添加验证步骤：

```yaml
- name: Verify RootModel fixes
  run: |
    python3 test_rootmodel_fix.py
```

## 🔗 相关 Issue

建议在 OpenMetadata GitHub 仓库创建 Issue：

**标题**: "Auto-fix RootModel model_config in code generation"

**内容**:
```markdown
## Problem
When using datamodel-code-generator with Pydantic 2.x, generated RootModel 
classes include `model_config = ConfigDict(extra="forbid")` which is not 
supported and causes runtime errors.

## Solution
Add post-processing step in `scripts/datamodel_generation.py` to automatically
remove model_config from all RootModel classes.

## Implementation
See attached code in comment below.

## Related
- Pydantic docs: https://docs.pydantic.dev/latest/concepts/models/#rootmodel-and-custom-root-types
- Error: https://errors.pydantic.dev/2.11/u/root-model-extra
```

## ⚡ 总结

**短期**：使用方案 1 在代码生成后自动修复

**中期**：考虑方案 2 优化 JSON Schema 定义

**长期**：向 `datamodel-code-generator` 项目提交 PR，增加处理 RootModel 的选项

这样每次运行 `mvn clean install` 重新生成代码时，都会自动修复 RootModel 问题！
