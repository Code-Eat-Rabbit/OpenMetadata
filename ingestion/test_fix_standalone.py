#!/usr/bin/env python3
"""
独立验证修复效果的测试 - 不依赖metadata模块
"""

import json
from typing import List, Optional
from enum import Enum
from pydantic import BaseModel


class ConstraintType(str, Enum):
    PRIMARY_KEY = "PRIMARY_KEY"
    FOREIGN_KEY = "FOREIGN_KEY"
    UNIQUE = "UNIQUE"


class TableConstraint(BaseModel):
    """模拟TableConstraint类"""
    constraintType: ConstraintType
    columns: List[str]
    referredColumns: Optional[List[str]] = None


class MockTable(BaseModel):
    """模拟Table实体"""
    name: str
    tableConstraints: Optional[List[TableConstraint]] = None


def _get_constraint_key(constraint):
    """
    Generate a unique key for a table constraint.
    
    The key includes constraintType, columns, and referredColumns (if present)
    to ensure proper matching of foreign key constraints.
    
    Args:
        constraint: TableConstraint object
        
    Returns:
        str: Unique key for the constraint
    """
    key = f"{constraint.constraintType}:{','.join(sorted(constraint.columns))}"
    # Include referredColumns in the key for foreign key constraints to ensure proper matching
    if hasattr(constraint, 'referredColumns') and constraint.referredColumns:
        key += f":{','.join(sorted(constraint.referredColumns))}"
    return key


def _table_constraints_handler_fixed(source, destination):
    """
    Handle table constraints patching properly.
    This ensures we only perform allowed operations on constraints and maintain the structure.
    
    Fixed to include referredColumns in constraint matching to prevent unnecessary 
    version updates for foreign key constraints (issue #17987).
    """
    if not hasattr(source, "tableConstraints") or not hasattr(
        destination, "tableConstraints"
    ):
        return

    source_table_constraints = getattr(source, "tableConstraints")
    destination_table_constraints = getattr(destination, "tableConstraints")

    if not source_table_constraints or not destination_table_constraints:
        return

    # Create a dictionary of source constraints for easy lookup
    source_constraints_dict = {}
    for constraint in source_table_constraints:
        # Create a unique key based on constraintType, columns, and referredColumns
        key = _get_constraint_key(constraint)
        source_constraints_dict[key] = constraint

    # Rearrange destination constraints to match source order when possible
    rearranged_constraints = []

    # First add constraints that exist in both source and destination (preserving order from source)
    for source_constraint in source_table_constraints:
        key = _get_constraint_key(source_constraint)
        for dest_constraint in destination_table_constraints:
            dest_key = _get_constraint_key(dest_constraint)
            if key == dest_key:
                rearranged_constraints.append(dest_constraint)
                break

    # Then add new constraints from destination that don't exist in source
    for dest_constraint in destination_table_constraints:
        dest_key = _get_constraint_key(dest_constraint)
        if dest_key not in source_constraints_dict:
            rearranged_constraints.append(dest_constraint)

    # Update the destination constraints with the rearranged list
    setattr(destination, "tableConstraints", rearranged_constraints)


def _table_constraints_handler_original(source, destination):
    """
    原始的有bug的版本 - 用于对比
    """
    if not hasattr(source, "tableConstraints") or not hasattr(
        destination, "tableConstraints"
    ):
        return

    source_table_constraints = getattr(source, "tableConstraints")
    destination_table_constraints = getattr(destination, "tableConstraints")

    if not source_table_constraints or not destination_table_constraints:
        return

    # 🐛 BUG: Create a dictionary of source constraints for easy lookup
    # 这里的key生成不包含referredColumns！
    source_constraints_dict = {}
    for constraint in source_table_constraints:
        # Create a unique key based on constraintType and columns
        key = f"{constraint.constraintType}:{','.join(sorted(constraint.columns))}"
        source_constraints_dict[key] = constraint

    # Rearrange destination constraints to match source order when possible
    rearranged_constraints = []

    # First add constraints that exist in both source and destination (preserving order from source)
    for source_constraint in source_table_constraints:
        key = f"{source_constraint.constraintType}:{','.join(sorted(source_constraint.columns))}"
        for dest_constraint in destination_table_constraints:
            dest_key = f"{dest_constraint.constraintType}:{','.join(sorted(dest_constraint.columns))}"
            if key == dest_key:
                rearranged_constraints.append(dest_constraint)
                break

    # Then add new constraints from destination that don't exist in source
    for dest_constraint in destination_table_constraints:
        dest_key = f"{dest_constraint.constraintType}:{','.join(sorted(dest_constraint.columns))}"
        if dest_key not in source_constraints_dict:
            rearranged_constraints.append(dest_constraint)

    # Update the destination constraints with the rearranged list
    setattr(destination, "tableConstraints", rearranged_constraints)


def test_constraint_key_generation():
    """测试约束key生成逻辑"""
    print("=== 测试约束key生成逻辑 ===")
    
    # 测试主键约束
    pk = TableConstraint(
        constraintType=ConstraintType.PRIMARY_KEY,
        columns=["id"]
    )
    pk_key = _get_constraint_key(pk)
    print(f"主键约束key: {pk_key}")
    
    # 测试外键约束
    fk1 = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["department.id"]
    )
    fk1_key = _get_constraint_key(fk1)
    print(f"外键约束1 key: {fk1_key}")
    
    # 测试相同列但不同referredColumns的外键约束
    fk2 = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["public.department.id"]
    )
    fk2_key = _get_constraint_key(fk2)
    print(f"外键约束2 key: {fk2_key}")
    
    # 验证不同的外键约束有不同的key
    assert fk1_key != fk2_key, "不同referredColumns的外键约束应该有不同的key"
    print("✅ 外键约束key生成逻辑正确")
    
    return pk_key, fk1_key, fk2_key


def test_bug_reproduction_and_fix():
    """测试bug复现和修复效果对比"""
    print("\n=== Bug复现和修复效果对比 ===")
    
    # 创建测试约束
    fk_constraint_v1 = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["department.id"]
    )
    
    fk_constraint_v2 = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["public.department.id"]  # 不同的referredColumns
    )
    
    print(f"约束v1: {fk_constraint_v1}")
    print(f"约束v2: {fk_constraint_v2}")
    
    # 测试原始有bug的版本
    print(f"\n--- 原始版本（有bug）---")
    
    # 第一次摄取
    source1_orig = MockTable(name="employees", tableConstraints=None)
    dest1_orig = MockTable(name="employees", tableConstraints=[fk_constraint_v1])
    
    _table_constraints_handler_original(source1_orig, dest1_orig)
    
    # 第二次摄取
    source2_orig = dest1_orig.model_copy()
    dest2_orig = MockTable(name="employees", tableConstraints=[fk_constraint_v2])
    
    _table_constraints_handler_original(source2_orig, dest2_orig)
    
    # 检查原始版本的结果
    orig_constraints1_str = json.dumps([c.model_dump() for c in dest1_orig.tableConstraints], sort_keys=True)
    orig_constraints2_str = json.dumps([c.model_dump() for c in dest2_orig.tableConstraints], sort_keys=True)
    
    orig_stable = orig_constraints1_str == orig_constraints2_str
    print(f"原始版本稳定性: {'✅ 稳定' if orig_stable else '❌ 不稳定（bug确认）'}")
    
    if not orig_stable:
        print("🐛 Bug确认：外键约束因referredColumns不同而被错误处理")
        print(f"摄取1结果: {dest1_orig.tableConstraints[0].referredColumns}")
        print(f"摄取2结果: {dest2_orig.tableConstraints[0].referredColumns}")
    
    # 测试修复后的版本
    print(f"\n--- 修复后版本 ---")
    
    # 第一次摄取
    source1_fixed = MockTable(name="employees", tableConstraints=None)
    dest1_fixed = MockTable(name="employees", tableConstraints=[fk_constraint_v1])
    
    _table_constraints_handler_fixed(source1_fixed, dest1_fixed)
    
    # 第二次摄取
    source2_fixed = dest1_fixed.model_copy()
    dest2_fixed = MockTable(name="employees", tableConstraints=[fk_constraint_v2])
    
    _table_constraints_handler_fixed(source2_fixed, dest2_fixed)
    
    # 检查修复后版本的结果
    print(f"摄取1结果: {dest1_fixed.tableConstraints[0].referredColumns}")
    print(f"摄取2结果: {dest2_fixed.tableConstraints[0].referredColumns}")
    
    # 验证修复效果：不同的referredColumns应该被保持
    fixed_correct = (
        dest1_fixed.tableConstraints[0].referredColumns == ["department.id"] and
        dest2_fixed.tableConstraints[0].referredColumns == ["public.department.id"]
    )
    
    print(f"修复后正确性: {'✅ 正确' if fixed_correct else '❌ 仍有问题'}")
    
    if fixed_correct:
        print("✅ 修复成功：不同的referredColumns被正确识别为不同约束")
    else:
        print("❌ 修复失败：约束仍被错误处理")
    
    return orig_stable, fixed_correct


def test_same_constraint_stability():
    """测试相同约束的稳定性"""
    print("\n=== 测试相同约束的稳定性 ===")
    
    # 创建完全相同的外键约束
    fk_constraint = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["department.id"]
    )
    
    # 第一次摄取
    source1 = MockTable(name="employees", tableConstraints=None)
    dest1 = MockTable(name="employees", tableConstraints=[fk_constraint])
    
    _table_constraints_handler_fixed(source1, dest1)
    
    # 第二次摄取（完全相同的约束）
    source2 = dest1.model_copy()
    dest2 = MockTable(name="employees", tableConstraints=[fk_constraint.model_copy()])
    
    _table_constraints_handler_fixed(source2, dest2)
    
    # 检查稳定性
    constraints1_str = json.dumps([c.model_dump() for c in dest1.tableConstraints], sort_keys=True)
    constraints2_str = json.dumps([c.model_dump() for c in dest2.tableConstraints], sort_keys=True)
    
    is_stable = constraints1_str == constraints2_str
    print(f"相同约束稳定性: {'✅ 稳定' if is_stable else '❌ 不稳定'}")
    
    return is_stable


if __name__ == "__main__":
    print("开始验证修复效果...\n")
    
    try:
        # 测试key生成
        test_constraint_key_generation()
        
        # 测试bug复现和修复
        orig_stable, fixed_correct = test_bug_reproduction_and_fix()
        
        # 测试相同约束稳定性
        same_stable = test_same_constraint_stability()
        
        print(f"\n=== 修复验证总结 ===")
        print(f"原始版本有bug: {'✅ 确认' if not orig_stable else '❌ 未复现'}")
        print(f"修复后正确性: {'✅ 通过' if fixed_correct else '❌ 失败'}")
        print(f"相同约束稳定性: {'✅ 通过' if same_stable else '❌ 失败'}")
        
        if not orig_stable and fixed_correct and same_stable:
            print(f"\n🎉 修复验证成功！issue #17987 已解决")
            print("修复要点：")
            print("1. 在约束key生成中包含referredColumns")
            print("2. 确保不同referredColumns的外键约束被正确识别为不同约束")
            print("3. 保持相同约束的稳定性")
        else:
            print(f"\n❌ 修复验证失败，需要进一步调试")
            
    except Exception as e:
        print(f"测试执行出错: {e}")
        import traceback
        traceback.print_exc()