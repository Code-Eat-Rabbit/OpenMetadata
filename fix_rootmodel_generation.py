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
    
    if not generated_files:
        if verbose:
            print(f"  Warning: No files found at {generated_path}")
        return 0
    
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
    
    # Determine ingestion path
    if current_dir.endswith("/ingestion"):
        ingestion_path = "./"
    elif os.path.exists("ingestion"):
        ingestion_path = "ingestion/"
    else:
        print("Error: Could not find ingestion directory")
        print(f"Current directory: {current_dir}")
        print("Please run from OpenMetadata root or ingestion directory")
        sys.exit(1)
    
    print("="*60)
    print("RootModel model_config Fixer")
    print("="*60)
    print(f"Current directory: {current_dir}")
    print(f"Ingestion path: {ingestion_path}")
    print()
    
    fixed_count = fix_all_rootmodels(ingestion_path)
    
    print("="*60)
    if fixed_count > 0:
        print(f"✅ Successfully fixed {fixed_count} file(s)")
        print("\nNext steps:")
        print("1. Verify the fix:")
        print("   python3 -c 'from metadata.generated.schema.type import ownerConfig'")
        print("\n2. Run your tests:")
        print("   metadata ingest -c ingestion/tests/unit/.../test-03-multiple-users.yaml")
        sys.exit(0)
    else:
        print("⚠️  No RootModel issues found")
        print("   Either already fixed or no generated files found")
        sys.exit(0)
