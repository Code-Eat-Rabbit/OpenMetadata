#!/usr/bin/env python3
"""
修复 ownerConfig.py 中的 RootModel model_config 问题

使用方法:
    python3 fix_ownerconfig_rootmodel.py [path_to_ownerConfig.py]

如果不提供路径，将使用默认路径:
    ingestion/src/metadata/generated/schema/type/ownerConfig.py
"""

import re
import sys
import os
from pathlib import Path

def fix_rootmodel_config(file_path):
    """
    移除 RootModel 类中的 model_config 定义
    
    Pydantic 2.x 的 RootModel 不支持 model_config['extra'] 设置
    """
    print(f"Processing: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"❌ Error: File not found: {file_path}")
        return False
    
    # 备份原文件
    backup_path = f"{file_path}.bak"
    with open(file_path, 'r', encoding='utf-8') as f:
        original_content = f.read()
    
    with open(backup_path, 'w', encoding='utf-8') as f:
        f.write(original_content)
    print(f"✓ Backup created: {backup_path}")
    
    # 修复策略：
    # 1. 找到 class XXX(RootModel[...]):
    # 2. 删除后面的 model_config = ConfigDict(...) 块
    
    # 正则表达式匹配 RootModel 类及其 model_config
    # 匹配模式：
    # class ClassName(RootModel[...]):
    #     model_config = ConfigDict(
    #         extra="forbid",
    #     )
    pattern = r'(class\s+\w+\(RootModel\[[^\]]+\]\):)\s+(model_config\s*=\s*ConfigDict\([^)]*\)\s*)'
    
    # 替换为只保留类定义
    fixed_content = re.sub(pattern, r'\1\n    ', original_content, flags=re.MULTILINE)
    
    # 检查是否有修改
    if original_content == fixed_content:
        print("⚠️  No RootModel model_config found to fix")
        print("    File might already be fixed or doesn't have the issue")
        return False
    
    # 保存修复后的文件
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    
    print("✓ Fixed RootModel classes")
    
    # 显示差异
    print("\n" + "="*60)
    print("Changes made:")
    print("="*60)
    
    # 简单的行比较
    original_lines = original_content.split('\n')
    fixed_lines = fixed_content.split('\n')
    
    changes_count = 0
    for i, (orig, fixed) in enumerate(zip(original_lines, fixed_lines), 1):
        if orig != fixed:
            if 'model_config' in orig:
                print(f"Line {i}: - {orig.strip()}")
                changes_count += 1
    
    print(f"\n✓ Removed {changes_count} model_config lines from RootModel classes")
    print(f"✓ File saved: {file_path}")
    
    return True

def main():
    # 默认路径
    default_path = "ingestion/src/metadata/generated/schema/type/ownerConfig.py"
    
    # 从命令行参数获取路径，或使用默认路径
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = default_path
        print(f"Using default path: {file_path}")
        print(f"(You can specify a custom path: python3 {sys.argv[0]} <path>)")
        print()
    
    # 转换为绝对路径
    file_path = os.path.abspath(file_path)
    
    success = fix_rootmodel_config(file_path)
    
    if success:
        print("\n" + "="*60)
        print("✅ Fix completed successfully!")
        print("="*60)
        print("\nNext steps:")
        print("1. Verify the fix:")
        print("   python3 -c \"from metadata.generated.schema.type import ownerConfig\"")
        print("\n2. Run your test:")
        print("   metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml")
        sys.exit(0)
    else:
        print("\n" + "="*60)
        print("⚠️  Fix may not be needed or file not found")
        print("="*60)
        sys.exit(1)

if __name__ == "__main__":
    main()
