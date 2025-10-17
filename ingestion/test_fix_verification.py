#!/usr/bin/env python3
"""
验证修复效果的测试
"""

import json
import sys
import os

# 添加src路径以便导入修复后的模块
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from typing import List, Optional
from enum import Enum
from pydantic import BaseModel

# 导入修复后的函数
from metadata.ingestion.models.patch_request import _table_constraints_handler, _get_constraint_key


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


def test_foreign_key_stability_after_fix():
    """测试修复后外键约束的稳定性"""
    print("\n=== 测试修复后外键约束稳定性 ===")
    
    # 创建两个具有相同约束类型和列但不同referredColumns的外键约束
    fk_constraint_v1 = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["department.id"]
    )
    
    fk_constraint_v2 = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["public.department.id"]
    )
    
    print(f"约束v1: {fk_constraint_v1}")
    print(f"约束v2: {fk_constraint_v2}")
    
    # 第一次摄取
    source1 = MockTable(name="employees", tableConstraints=None)
    dest1 = MockTable(name="employees", tableConstraints=[fk_constraint_v1])
    
    print(f"\n摄取1 - Source: {source1.tableConstraints}")
    print(f"摄取1 - Dest before: {dest1.tableConstraints}")
    
    _table_constraints_handler(source1, dest1)
    
    print(f"摄取1 - Dest after: {dest1.tableConstraints}")
    
    # 第二次摄取（不同的referredColumns）
    source2 = dest1.model_copy()
    dest2 = MockTable(name="employees", tableConstraints=[fk_constraint_v2])
    
    print(f"\n摄取2 - Source: {source2.tableConstraints}")
    print(f"摄取2 - Dest before: {dest2.tableConstraints}")
    
    _table_constraints_handler(source2, dest2)
    
    print(f"摄取2 - Dest after: {dest2.tableConstraints}")
    
    # 验证：由于referredColumns不同，这应该被视为不同的约束
    # 因此dest2应该保持原来的约束（fk_constraint_v2）
    expected_constraint = fk_constraint_v2
    actual_constraint = dest2.tableConstraints[0]
    
    print(f"\n期望约束: {expected_constraint}")
    print(f"实际约束: {actual_constraint}")
    
    # 检查referredColumns是否保持正确
    expected_referred = expected_constraint.referredColumns
    actual_referred = actual_constraint.referredColumns
    
    is_correct = expected_referred == actual_referred
    print(f"referredColumns正确性: {'✅ 正确' if is_correct else '❌ 错误'}")
    
    if is_correct:
        print("✅ 修复成功：不同的referredColumns被正确识别为不同的约束")
    else:
        print("❌ 修复失败：约束仍然被错误地合并")
    
    return is_correct


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
    
    _table_constraints_handler(source1, dest1)
    
    # 第二次摄取（完全相同的约束）
    source2 = dest1.model_copy()
    dest2 = MockTable(name="employees", tableConstraints=[fk_constraint.model_copy()])
    
    _table_constraints_handler(source2, dest2)
    
    # 检查稳定性
    constraints1_str = json.dumps([c.model_dump() for c in dest1.tableConstraints], sort_keys=True)
    constraints2_str = json.dumps([c.model_dump() for c in dest2.tableConstraints], sort_keys=True)
    
    is_stable = constraints1_str == constraints2_str
    print(f"相同约束稳定性: {'✅ 稳定' if is_stable else '❌ 不稳定'}")
    
    if not is_stable:
        print(f"差异: {constraints1_str} != {constraints2_str}")
    
    return is_stable


def test_mixed_constraints():
    """测试混合约束的处理"""
    print("\n=== 测试混合约束处理 ===")
    
    pk = TableConstraint(
        constraintType=ConstraintType.PRIMARY_KEY,
        columns=["id"]
    )
    
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
    
    # 第一次摄取：特定顺序
    source1 = MockTable(name="employees", tableConstraints=None)
    dest1 = MockTable(name="employees", tableConstraints=[pk, fk1, fk2])
    
    _table_constraints_handler(source1, dest1)
    
    # 第二次摄取：不同顺序
    source2 = dest1.model_copy()
    dest2 = MockTable(name="employees", tableConstraints=[fk2, fk1, pk])
    
    _table_constraints_handler(source2, dest2)
    
    # 检查顺序是否按source保持
    result1_types = [c.constraintType for c in dest1.tableConstraints]
    result2_types = [c.constraintType for c in dest2.tableConstraints]
    
    print(f"摄取1结果顺序: {result1_types}")
    print(f"摄取2结果顺序: {result2_types}")
    
    order_preserved = result1_types == result2_types
    print(f"顺序保持: {'✅ 保持' if order_preserved else '❌ 未保持'}")
    
    return order_preserved


if __name__ == "__main__":
    print("开始验证修复效果...\n")
    
    try:
        # 测试key生成
        test_constraint_key_generation()
        
        # 测试外键稳定性
        fk_correct = test_foreign_key_stability_after_fix()
        
        # 测试相同约束稳定性
        same_stable = test_same_constraint_stability()
        
        # 测试混合约束
        order_preserved = test_mixed_constraints()
        
        print(f"\n=== 修复验证总结 ===")
        print(f"外键约束正确性: {'✅ 通过' if fk_correct else '❌ 失败'}")
        print(f"相同约束稳定性: {'✅ 通过' if same_stable else '❌ 失败'}")
        print(f"约束顺序保持: {'✅ 通过' if order_preserved else '❌ 失败'}")
        
        if fk_correct and same_stable and order_preserved:
            print(f"\n🎉 修复验证成功！issue #17987 已解决")
        else:
            print(f"\n❌ 修复验证失败，需要进一步调试")
            
    except Exception as e:
        print(f"测试执行出错: {e}")
        import traceback
        traceback.print_exc()