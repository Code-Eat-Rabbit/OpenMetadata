#!/usr/bin/env python3
"""
简化版测试：复现外键版本控制bug
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


def _table_constraints_handler_original(source, destination):
    """
    原始的table constraints处理函数（从patch_request.py复制）
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


def test_constraint_stability():
    """测试约束稳定性"""
    print("=== 测试外键版本控制bug ===\n")
    
    # 创建约束
    foreign_key = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["department.id"]
    )
    
    primary_key = TableConstraint(
        constraintType=ConstraintType.PRIMARY_KEY,
        columns=["id"]
    )
    
    # 测试场景1：只有外键
    print("--- 场景1：只有外键约束 ---")
    
    # 第一次摄取
    source1 = MockTable(name="employees", tableConstraints=None)
    dest1 = MockTable(name="employees", tableConstraints=[foreign_key])
    
    print(f"摄取1 - Source: {source1.tableConstraints}")
    print(f"摄取1 - Dest before: {dest1.tableConstraints}")
    
    _table_constraints_handler_original(source1, dest1)
    
    print(f"摄取1 - Dest after: {dest1.tableConstraints}")
    
    # 第二次摄取（相同约束）
    source2 = dest1.model_copy()  # 上次的结果作为source
    dest2 = MockTable(name="employees", tableConstraints=[foreign_key])  # 相同的约束
    
    print(f"\n摄取2 - Source: {source2.tableConstraints}")
    print(f"摄取2 - Dest before: {dest2.tableConstraints}")
    
    _table_constraints_handler_original(source2, dest2)
    
    print(f"摄取2 - Dest after: {dest2.tableConstraints}")
    
    # 检查是否有变化
    constraints1_str = json.dumps([c.model_dump() for c in dest1.tableConstraints], sort_keys=True)
    constraints2_str = json.dumps([c.model_dump() for c in dest2.tableConstraints], sort_keys=True)
    
    fk_stable = constraints1_str == constraints2_str
    print(f"外键约束稳定性: {'✅ 稳定' if fk_stable else '❌ 不稳定'}")
    
    if not fk_stable:
        print(f"差异: {constraints1_str} != {constraints2_str}")
    
    # 测试场景2：只有主键
    print("\n--- 场景2：只有主键约束 ---")
    
    # 第一次摄取
    source1 = MockTable(name="employees", tableConstraints=None)
    dest1 = MockTable(name="employees", tableConstraints=[primary_key])
    
    print(f"摄取1 - Source: {source1.tableConstraints}")
    print(f"摄取1 - Dest before: {dest1.tableConstraints}")
    
    _table_constraints_handler_original(source1, dest1)
    
    print(f"摄取1 - Dest after: {dest1.tableConstraints}")
    
    # 第二次摄取（相同约束）
    source2 = dest1.model_copy()  # 上次的结果作为source
    dest2 = MockTable(name="employees", tableConstraints=[primary_key])  # 相同的约束
    
    print(f"\n摄取2 - Source: {source2.tableConstraints}")
    print(f"摄取2 - Dest before: {dest2.tableConstraints}")
    
    _table_constraints_handler_original(source2, dest2)
    
    print(f"摄取2 - Dest after: {dest2.tableConstraints}")
    
    # 检查是否有变化
    constraints1_str = json.dumps([c.model_dump() for c in dest1.tableConstraints], sort_keys=True)
    constraints2_str = json.dumps([c.model_dump() for c in dest2.tableConstraints], sort_keys=True)
    
    pk_stable = constraints1_str == constraints2_str
    print(f"主键约束稳定性: {'✅ 稳定' if pk_stable else '❌ 不稳定'}")
    
    if not pk_stable:
        print(f"差异: {constraints1_str} != {constraints2_str}")
    
    # 总结
    print(f"\n=== 总结 ===")
    print(f"外键约束稳定性: {'✅ 稳定' if fk_stable else '❌ 不稳定'}")
    print(f"主键约束稳定性: {'✅ 稳定' if pk_stable else '❌ 不稳定'}")
    
    if not fk_stable and pk_stable:
        print("\n🐛 Bug确认: 外键约束不稳定，但主键约束稳定")
        print("这证实了issue #17987中描述的问题")
    elif not fk_stable and not pk_stable:
        print("\n⚠️  所有约束类型都不稳定，可能是通用问题")
    elif fk_stable and pk_stable:
        print("\n✅ 所有约束类型都稳定，未发现问题")
    
    return fk_stable, pk_stable


def analyze_key_generation():
    """分析key生成逻辑是否有问题"""
    print("\n=== 分析key生成逻辑 ===")
    
    # 测试不同约束的key生成
    foreign_key = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["department.id"]
    )
    
    primary_key = TableConstraint(
        constraintType=ConstraintType.PRIMARY_KEY,
        columns=["id"]
    )
    
    # 当前的key生成逻辑（不包含referredColumns）
    fk_key = f"{foreign_key.constraintType}:{','.join(sorted(foreign_key.columns))}"
    pk_key = f"{primary_key.constraintType}:{','.join(sorted(primary_key.columns))}"
    
    print(f"外键key: {fk_key}")
    print(f"主键key: {pk_key}")
    
    # 问题：外键的referredColumns没有包含在key中！
    print(f"外键referredColumns: {foreign_key.referredColumns}")
    print(f"主键referredColumns: {primary_key.referredColumns}")
    
    print("\n🔍 发现问题：当前key生成逻辑不包含referredColumns！")
    print("这意味着具有相同constraintType和columns但不同referredColumns的约束")
    print("会被认为是相同的约束，导致不必要的重新排列。")


if __name__ == "__main__":
    fk_stable, pk_stable = test_constraint_stability()
    analyze_key_generation()