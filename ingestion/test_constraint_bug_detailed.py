#!/usr/bin/env python3
"""
详细测试：复现外键版本控制bug - 重现referredColumns不匹配的情况
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
    这里存在bug：key生成不包含referredColumns
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


def _table_constraints_handler_fixed(source, destination):
    """
    修复后的table constraints处理函数
    修复：key生成包含referredColumns
    """
    if not hasattr(source, "tableConstraints") or not hasattr(
        destination, "tableConstraints"
    ):
        return

    source_table_constraints = getattr(source, "tableConstraints")
    destination_table_constraints = getattr(destination, "tableConstraints")

    if not source_table_constraints or not destination_table_constraints:
        return

    # ✅ FIX: Create a dictionary of source constraints for easy lookup
    # 修复：key生成包含referredColumns
    source_constraints_dict = {}
    for constraint in source_table_constraints:
        # Create a unique key based on constraintType, columns, and referredColumns
        key = f"{constraint.constraintType}:{','.join(sorted(constraint.columns))}"
        if constraint.referredColumns:
            key += f":{','.join(sorted(constraint.referredColumns))}"
        source_constraints_dict[key] = constraint

    # Rearrange destination constraints to match source order when possible
    rearranged_constraints = []

    # First add constraints that exist in both source and destination (preserving order from source)
    for source_constraint in source_table_constraints:
        key = f"{source_constraint.constraintType}:{','.join(sorted(source_constraint.columns))}"
        if source_constraint.referredColumns:
            key += f":{','.join(sorted(source_constraint.referredColumns))}"
        
        for dest_constraint in destination_table_constraints:
            dest_key = f"{dest_constraint.constraintType}:{','.join(sorted(dest_constraint.columns))}"
            if dest_constraint.referredColumns:
                dest_key += f":{','.join(sorted(dest_constraint.referredColumns))}"
            
            if key == dest_key:
                rearranged_constraints.append(dest_constraint)
                break

    # Then add new constraints from destination that don't exist in source
    for dest_constraint in destination_table_constraints:
        dest_key = f"{dest_constraint.constraintType}:{','.join(sorted(dest_constraint.columns))}"
        if dest_constraint.referredColumns:
            dest_key += f":{','.join(sorted(dest_constraint.referredColumns))}"
        
        if dest_key not in source_constraints_dict:
            rearranged_constraints.append(dest_constraint)

    # Update the destination constraints with the rearranged list
    setattr(destination, "tableConstraints", rearranged_constraints)


def test_foreign_key_bug_scenario():
    """
    测试能够触发bug的具体场景：
    当外键约束的referredColumns在不同摄取周期中略有不同时
    """
    print("=== 测试外键版本控制bug - 具体场景 ===\n")
    
    # 场景：同一个外键约束，但referredColumns的表示方式略有不同
    # 这可能发生在不同的数据库连接器或摄取周期中
    
    fk_constraint_v1 = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["department.id"]  # 第一种表示方式
    )
    
    fk_constraint_v2 = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["public.department.id"]  # 第二种表示方式（包含schema）
    )
    
    print("--- 场景：外键约束referredColumns表示方式不同 ---")
    print(f"约束v1: {fk_constraint_v1}")
    print(f"约束v2: {fk_constraint_v2}")
    
    # 第一次摄取
    source1 = MockTable(name="employees", tableConstraints=None)
    dest1 = MockTable(name="employees", tableConstraints=[fk_constraint_v1])
    
    print(f"\n摄取1 - Source: {source1.tableConstraints}")
    print(f"摄取1 - Dest before: {dest1.tableConstraints}")
    
    _table_constraints_handler_original(source1, dest1)
    
    print(f"摄取1 - Dest after: {dest1.tableConstraints}")
    
    # 第二次摄取（相同约束但referredColumns不同）
    source2 = dest1.model_copy()  # 上次的结果作为source
    dest2 = MockTable(name="employees", tableConstraints=[fk_constraint_v2])  # 不同的referredColumns
    
    print(f"\n摄取2 - Source: {source2.tableConstraints}")
    print(f"摄取2 - Dest before: {dest2.tableConstraints}")
    
    _table_constraints_handler_original(source2, dest2)
    
    print(f"摄取2 - Dest after: {dest2.tableConstraints}")
    
    # 检查是否有变化
    constraints1_str = json.dumps([c.model_dump() for c in dest1.tableConstraints], sort_keys=True)
    constraints2_str = json.dumps([c.model_dump() for c in dest2.tableConstraints], sort_keys=True)
    
    original_stable = constraints1_str == constraints2_str
    print(f"\n原始逻辑稳定性: {'✅ 稳定' if original_stable else '❌ 不稳定'}")
    
    if not original_stable:
        print("🐛 Bug确认：外键约束因referredColumns不同而被重新排列！")
        print(f"摄取1结果: {constraints1_str}")
        print(f"摄取2结果: {constraints2_str}")
    
    # 测试修复后的逻辑
    print("\n--- 测试修复后的逻辑 ---")
    
    # 重置测试
    source1 = MockTable(name="employees", tableConstraints=None)
    dest1 = MockTable(name="employees", tableConstraints=[fk_constraint_v1])
    
    _table_constraints_handler_fixed(source1, dest1)
    
    source2 = dest1.model_copy()
    dest2 = MockTable(name="employees", tableConstraints=[fk_constraint_v2])
    
    _table_constraints_handler_fixed(source2, dest2)
    
    # 检查修复后的稳定性
    fixed_constraints1_str = json.dumps([c.model_dump() for c in dest1.tableConstraints], sort_keys=True)
    fixed_constraints2_str = json.dumps([c.model_dump() for c in dest2.tableConstraints], sort_keys=True)
    
    fixed_stable = fixed_constraints1_str == fixed_constraints2_str
    print(f"修复后逻辑稳定性: {'✅ 稳定' if fixed_stable else '❌ 仍不稳定'}")
    
    if not fixed_stable:
        print("修复后的逻辑正确处理了不同的referredColumns")
        print(f"摄取1结果: {fixed_constraints1_str}")
        print(f"摄取2结果: {fixed_constraints2_str}")
    
    return original_stable, fixed_stable


def test_multiple_foreign_keys():
    """测试多个外键约束的情况"""
    print("\n=== 测试多个外键约束的场景 ===\n")
    
    # 创建多个外键约束
    fk1 = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["department.id"]
    )
    
    fk2 = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["manager_id"],
        referredColumns=["employee.id"]
    )
    
    pk = TableConstraint(
        constraintType=ConstraintType.PRIMARY_KEY,
        columns=["id"]
    )
    
    # 第一次摄取：按某种顺序
    constraints_order1 = [pk, fk1, fk2]
    
    # 第二次摄取：不同的顺序
    constraints_order2 = [fk2, pk, fk1]
    
    print("--- 测试约束顺序变化的影响 ---")
    print(f"顺序1: {[f'{c.constraintType}({c.columns})' for c in constraints_order1]}")
    print(f"顺序2: {[f'{c.constraintType}({c.columns})' for c in constraints_order2]}")
    
    # 第一次摄取
    source1 = MockTable(name="employees", tableConstraints=None)
    dest1 = MockTable(name="employees", tableConstraints=constraints_order1)
    
    _table_constraints_handler_original(source1, dest1)
    
    # 第二次摄取
    source2 = dest1.model_copy()
    dest2 = MockTable(name="employees", tableConstraints=constraints_order2)
    
    _table_constraints_handler_original(source2, dest2)
    
    # 比较结果
    result1_order = [f'{c.constraintType}({c.columns})' for c in dest1.tableConstraints]
    result2_order = [f'{c.constraintType}({c.columns})' for c in dest2.tableConstraints]
    
    print(f"\n摄取1结果顺序: {result1_order}")
    print(f"摄取2结果顺序: {result2_order}")
    
    order_stable = result1_order == result2_order
    print(f"顺序稳定性: {'✅ 稳定' if order_stable else '❌ 不稳定'}")
    
    if not order_stable:
        print("⚠️  约束顺序发生了变化，这可能导致不必要的版本更新")
    
    return order_stable


if __name__ == "__main__":
    original_stable, fixed_stable = test_foreign_key_bug_scenario()
    order_stable = test_multiple_foreign_keys()
    
    print(f"\n=== 最终总结 ===")
    print(f"原始逻辑稳定性: {'✅ 稳定' if original_stable else '❌ 不稳定'}")
    print(f"修复后逻辑稳定性: {'✅ 稳定' if fixed_stable else '❌ 不稳定'}")
    print(f"约束顺序稳定性: {'✅ 稳定' if order_stable else '❌ 不稳定'}")
    
    if not original_stable or not order_stable:
        print(f"\n🐛 确认bug存在！问题出现在：")
        if not original_stable:
            print("- 外键约束的referredColumns未包含在key生成中")
        if not order_stable:
            print("- 约束重新排列逻辑导致顺序不稳定")
        print("\n建议的修复方案：")
        print("1. 在key生成中包含referredColumns")
        print("2. 改进约束匹配和排列逻辑")