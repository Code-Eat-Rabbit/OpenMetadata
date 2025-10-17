#!/usr/bin/env python3
"""
测试脚本：复现外键版本控制bug

这个脚本用于复现issue #17987中描述的bug：
当数据库中的字段被定义为外键时，每次执行摄取而不改变该字段，
都会创建一个新版本并引用该字段。这对主键不会发生。
"""

import json
from typing import List, Optional

from metadata.generated.schema.entity.data.table import ConstraintType, TableConstraint
from metadata.ingestion.models.patch_request import _table_constraints_handler
from pydantic import BaseModel


class MockTable(BaseModel):
    """模拟Table实体"""
    name: str
    tableConstraints: Optional[List[TableConstraint]] = None


def create_table_with_constraints(name: str, constraints: List[TableConstraint]) -> MockTable:
    """创建带约束的表"""
    return MockTable(name=name, tableConstraints=constraints)


def simulate_ingestion_cycle(table_name: str, constraints: List[TableConstraint]) -> List[MockTable]:
    """
    模拟多次摄取周期，每次都使用相同的约束
    返回每次摄取后的表状态
    """
    results = []
    
    # 第一次摄取 - 创建表
    source_table = MockTable(name=table_name, tableConstraints=None)
    destination_table = create_table_with_constraints(table_name, constraints)
    
    _table_constraints_handler(source_table, destination_table)
    results.append(destination_table.model_copy())
    
    # 后续摄取 - 模拟相同的约束被重新发现
    for i in range(3):  # 模拟3次额外的摄取周期
        # source是上一次的状态
        source_table = results[-1].model_copy()
        # destination是新发现的相同约束
        destination_table = create_table_with_constraints(table_name, constraints)
        
        print(f"\n=== 摄取周期 {i+2} ===")
        print(f"Source约束: {source_table.tableConstraints}")
        print(f"Destination约束: {destination_table.tableConstraints}")
        
        _table_constraints_handler(source_table, destination_table)
        
        print(f"处理后的约束: {destination_table.tableConstraints}")
        results.append(destination_table.model_copy())
    
    return results


def test_foreign_key_versioning_bug():
    """测试外键版本控制bug"""
    print("=== 测试外键版本控制bug ===")
    
    # 创建包含外键的约束
    foreign_key_constraint = TableConstraint(
        constraintType=ConstraintType.FOREIGN_KEY,
        columns=["department_id"],
        referredColumns=["department.id"]
    )
    
    primary_key_constraint = TableConstraint(
        constraintType=ConstraintType.PRIMARY_KEY,
        columns=["id"]
    )
    
    # 测试只有外键的情况
    print("\n--- 测试场景1: 只有外键约束 ---")
    fk_results = simulate_ingestion_cycle("employees", [foreign_key_constraint])
    
    # 检查是否每次都重新排列了约束（这可能导致版本变化）
    for i, result in enumerate(fk_results):
        print(f"摄取 {i+1} 后的约束: {result.tableConstraints}")
    
    # 测试只有主键的情况
    print("\n--- 测试场景2: 只有主键约束 ---")
    pk_results = simulate_ingestion_cycle("employees", [primary_key_constraint])
    
    for i, result in enumerate(pk_results):
        print(f"摄取 {i+1} 后的约束: {result.tableConstraints}")
    
    # 测试混合约束的情况
    print("\n--- 测试场景3: 混合约束（主键+外键） ---")
    mixed_constraints = [primary_key_constraint, foreign_key_constraint]
    mixed_results = simulate_ingestion_cycle("employees", mixed_constraints)
    
    for i, result in enumerate(mixed_results):
        print(f"摄取 {i+1} 后的约束: {result.tableConstraints}")
    
    return fk_results, pk_results, mixed_results


def analyze_constraint_stability(results: List[MockTable], constraint_type: str):
    """分析约束的稳定性"""
    print(f"\n=== 分析 {constraint_type} 约束稳定性 ===")
    
    if len(results) < 2:
        print("需要至少2次摄取结果进行比较")
        return True
    
    # 比较每次摄取的结果
    stable = True
    for i in range(1, len(results)):
        prev_constraints = results[i-1].tableConstraints or []
        curr_constraints = results[i].tableConstraints or []
        
        # 将约束转换为可比较的格式
        prev_set = set()
        curr_set = set()
        
        for c in prev_constraints:
            key = f"{c.constraintType}:{','.join(sorted(c.columns))}"
            if c.referredColumns:
                key += f":{','.join(sorted(c.referredColumns))}"
            prev_set.add(key)
        
        for c in curr_constraints:
            key = f"{c.constraintType}:{','.join(sorted(c.columns))}"
            if c.referredColumns:
                key += f":{','.join(sorted(c.referredColumns))}"
            curr_set.add(key)
        
        if prev_set != curr_set:
            print(f"❌ 摄取 {i} -> {i+1}: 约束发生变化")
            print(f"   之前: {prev_set}")
            print(f"   现在: {curr_set}")
            stable = False
        else:
            # 检查顺序是否改变
            prev_order = [str(c) for c in prev_constraints]
            curr_order = [str(c) for c in curr_constraints]
            if prev_order != curr_order:
                print(f"⚠️  摄取 {i} -> {i+1}: 约束顺序发生变化")
                print(f"   之前顺序: {prev_order}")
                print(f"   现在顺序: {curr_order}")
                stable = False
            else:
                print(f"✅ 摄取 {i} -> {i+1}: 约束保持稳定")
    
    if stable:
        print(f"✅ {constraint_type} 约束在所有摄取周期中保持稳定")
    else:
        print(f"❌ {constraint_type} 约束在摄取过程中发生了变化")
    
    return stable


if __name__ == "__main__":
    print("开始测试外键版本控制bug...")
    
    # 运行测试
    fk_results, pk_results, mixed_results = test_foreign_key_versioning_bug()
    
    # 分析结果
    print("\n" + "="*60)
    print("分析结果:")
    
    fk_stable = analyze_constraint_stability(fk_results, "外键")
    pk_stable = analyze_constraint_stability(pk_results, "主键")
    mixed_stable = analyze_constraint_stability(mixed_results, "混合")
    
    print("\n" + "="*60)
    print("总结:")
    print(f"外键约束稳定性: {'✅ 稳定' if fk_stable else '❌ 不稳定'}")
    print(f"主键约束稳定性: {'✅ 稳定' if pk_stable else '❌ 不稳定'}")
    print(f"混合约束稳定性: {'✅ 稳定' if mixed_stable else '❌ 不稳定'}")
    
    if not fk_stable and pk_stable:
        print("\n🐛 Bug确认: 外键约束不稳定，但主键约束稳定")
        print("这证实了issue #17987中描述的问题")
    elif not fk_stable and not pk_stable:
        print("\n⚠️  所有约束类型都不稳定，可能是通用问题")
    elif fk_stable and pk_stable:
        print("\n✅ 所有约束类型都稳定，未发现问题")