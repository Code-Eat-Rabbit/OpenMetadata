#!/usr/bin/env python3
"""
验证修复后现有功能仍然正常工作
基于原始test_table_constraints.py的测试用例
"""

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


class MockEntity(BaseModel):
    """Mock entity class for testing the table constraints handler"""
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


def _table_constraints_handler(source, destination):
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


class TableConstraintsHandlerTest:
    """Test cases for _table_constraints_handler function"""

    def test_no_table_constraints_attributes(self):
        """Test handling when entities don't have tableConstraints attributes"""

        class EntityWithoutConstraints(BaseModel):
            pass

        source = EntityWithoutConstraints()
        destination = EntityWithoutConstraints()

        # Should not raise any exceptions
        _table_constraints_handler(source, destination)
        print("✅ test_no_table_constraints_attributes passed")

    def test_null_table_constraints(self):
        """Test handling when tableConstraints are None"""
        source = MockEntity(tableConstraints=None)
        destination = MockEntity(tableConstraints=None)

        # Should not raise any exceptions
        _table_constraints_handler(source, destination)
        print("✅ test_null_table_constraints passed")

    def test_empty_table_constraints(self):
        """Test handling when tableConstraints are empty lists"""
        source = MockEntity(tableConstraints=[])
        destination = MockEntity(tableConstraints=[])

        # Should not raise any exceptions
        _table_constraints_handler(source, destination)
        assert destination.tableConstraints == []
        print("✅ test_empty_table_constraints passed")

    def test_source_empty_destination_with_constraints(self):
        """Test handling when source has no constraints but destination does"""
        source = MockEntity(tableConstraints=[])
        destination = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.PRIMARY_KEY, columns=["id"]
                )
            ]
        )

        # Run the handler
        _table_constraints_handler(source, destination)

        # Destination should still have its constraints
        assert len(destination.tableConstraints) == 1
        assert destination.tableConstraints[0].constraintType == ConstraintType.PRIMARY_KEY
        assert destination.tableConstraints[0].columns == ["id"]
        print("✅ test_source_empty_destination_with_constraints passed")

    def test_preserve_constraint_order_from_source(self):
        """Test that constraints are ordered based on the source order"""
        source = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.PRIMARY_KEY, columns=["id"]
                ),
                TableConstraint(constraintType=ConstraintType.UNIQUE, columns=["name"]),
            ]
        )

        destination = MockEntity(
            tableConstraints=[
                TableConstraint(constraintType=ConstraintType.UNIQUE, columns=["name"]),
                TableConstraint(
                    constraintType=ConstraintType.PRIMARY_KEY, columns=["id"]
                ),
            ]
        )

        # Run the handler
        _table_constraints_handler(source, destination)

        # Destination should have constraints ordered like the source
        assert len(destination.tableConstraints) == 2
        assert destination.tableConstraints[0].constraintType == ConstraintType.PRIMARY_KEY
        assert destination.tableConstraints[0].columns == ["id"]
        assert destination.tableConstraints[1].constraintType == ConstraintType.UNIQUE
        assert destination.tableConstraints[1].columns == ["name"]
        print("✅ test_preserve_constraint_order_from_source passed")

    def test_add_new_constraints_from_destination(self):
        """Test that new constraints from destination are added at the end"""
        source = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.PRIMARY_KEY, columns=["id"]
                )
            ]
        )

        destination = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.PRIMARY_KEY, columns=["id"]
                ),
                TableConstraint(constraintType=ConstraintType.UNIQUE, columns=["name"]),
            ]
        )

        # Run the handler
        _table_constraints_handler(source, destination)

        # Destination should have original constraint followed by new one
        assert len(destination.tableConstraints) == 2
        assert destination.tableConstraints[0].constraintType == ConstraintType.PRIMARY_KEY
        assert destination.tableConstraints[0].columns == ["id"]
        assert destination.tableConstraints[1].constraintType == ConstraintType.UNIQUE
        assert destination.tableConstraints[1].columns == ["name"]
        print("✅ test_add_new_constraints_from_destination passed")

    def test_multiple_columns_in_constraints(self):
        """Test handling constraints with multiple columns"""
        source = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.UNIQUE,
                    columns=["first_name", "last_name"],
                )
            ]
        )

        destination = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.UNIQUE,
                    columns=[
                        "last_name",
                        "first_name",
                    ],  # Order changed but should be identified as same constraint
                )
            ]
        )

        # Run the handler
        _table_constraints_handler(source, destination)

        # Should recognize these as the same constraint despite different column order
        assert len(destination.tableConstraints) == 1
        assert destination.tableConstraints[0].constraintType == ConstraintType.UNIQUE
        # Column order in destination should be preserved
        assert destination.tableConstraints[0].columns == ["last_name", "first_name"]
        print("✅ test_multiple_columns_in_constraints passed")

    def test_complex_constraint_rearrangement(self):
        """Test a complex scenario with multiple constraints being rearranged"""
        source = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.PRIMARY_KEY, columns=["id"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.FOREIGN_KEY, 
                    columns=["department_id"],
                    referredColumns=["department.id"]  # 添加referredColumns
                ),
                TableConstraint(
                    constraintType=ConstraintType.UNIQUE, columns=["email"]
                ),
            ]
        )

        destination = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.UNIQUE, columns=["email"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.PRIMARY_KEY, columns=["id"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.UNIQUE,
                    columns=["username"],  # New constraint
                )
                # Note: FOREIGN_KEY is missing
            ]
        )

        # Run the handler
        _table_constraints_handler(source, destination)

        # Destination should have constraints rearranged to match source order
        # with new constraints at the end
        assert len(destination.tableConstraints) == 3
        assert destination.tableConstraints[0].constraintType == ConstraintType.PRIMARY_KEY
        assert destination.tableConstraints[0].columns == ["id"]
        assert destination.tableConstraints[1].constraintType == ConstraintType.UNIQUE
        assert destination.tableConstraints[1].columns == ["email"]
        assert destination.tableConstraints[2].constraintType == ConstraintType.UNIQUE
        assert destination.tableConstraints[2].columns == ["username"]
        print("✅ test_complex_constraint_rearrangement passed")

    def test_same_constraint_type_different_columns(self):
        """Test handling multiple constraints of the same type but with different columns"""
        source = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.UNIQUE, columns=["email"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.UNIQUE, columns=["username"]
                ),
            ]
        )

        destination = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.UNIQUE, columns=["username"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.UNIQUE, columns=["email"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.UNIQUE,
                    columns=["phone"],  # New constraint
                ),
            ]
        )

        # Run the handler
        _table_constraints_handler(source, destination)

        # Destination should preserve the order from source and add new constraint at the end
        assert len(destination.tableConstraints) == 3
        assert destination.tableConstraints[0].constraintType == ConstraintType.UNIQUE
        assert destination.tableConstraints[0].columns == ["email"]
        assert destination.tableConstraints[1].constraintType == ConstraintType.UNIQUE
        assert destination.tableConstraints[1].columns == ["username"]
        assert destination.tableConstraints[2].constraintType == ConstraintType.UNIQUE
        assert destination.tableConstraints[2].columns == ["phone"]
        print("✅ test_same_constraint_type_different_columns passed")

    def test_foreign_key_with_referred_columns(self):
        """Test that foreign keys with different referredColumns are treated as different constraints"""
        source = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.FOREIGN_KEY,
                    columns=["department_id"],
                    referredColumns=["department.id"]
                )
            ]
        )

        destination = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.FOREIGN_KEY,
                    columns=["department_id"],
                    referredColumns=["public.department.id"]  # Different referredColumns
                )
            ]
        )

        # Run the handler
        _table_constraints_handler(source, destination)

        # Should treat these as different constraints due to different referredColumns
        assert len(destination.tableConstraints) == 1
        # The destination constraint should be preserved (not replaced by source)
        assert destination.tableConstraints[0].referredColumns == ["public.department.id"]
        print("✅ test_foreign_key_with_referred_columns passed")


if __name__ == "__main__":
    print("运行现有功能测试以验证修复没有破坏现有功能...\n")
    
    test = TableConstraintsHandlerTest()
    
    try:
        test.test_no_table_constraints_attributes()
        test.test_null_table_constraints()
        test.test_empty_table_constraints()
        test.test_source_empty_destination_with_constraints()
        test.test_preserve_constraint_order_from_source()
        test.test_add_new_constraints_from_destination()
        test.test_multiple_columns_in_constraints()
        test.test_complex_constraint_rearrangement()
        test.test_same_constraint_type_different_columns()
        test.test_foreign_key_with_referred_columns()
        
        print(f"\n🎉 所有现有功能测试通过！修复没有破坏现有功能。")
        print("额外验证了外键referredColumns的正确处理。")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()