#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Unit tests for the _table_constraints_handler function in patch_request.py
"""
from typing import List, Optional
from unittest import TestCase

from pydantic import BaseModel

from metadata.generated.schema.entity.data.table import ConstraintType, TableConstraint
from metadata.ingestion.models.patch_request import _table_constraints_handler


# pylint: disable=unsubscriptable-object
class MockEntity(BaseModel):
    """Mock entity class for testing the table constraints handler"""

    tableConstraints: Optional[List[TableConstraint]] = None


class TableConstraintsHandlerTest(TestCase):
    """Test cases for _table_constraints_handler function"""

    def test_no_table_constraints_attributes(self):
        """Test handling when entities don't have tableConstraints attributes"""

        class EntityWithoutConstraints(BaseModel):
            pass

        source = EntityWithoutConstraints()
        destination = EntityWithoutConstraints()

        # Should not raise any exceptions
        _table_constraints_handler(source, destination)

    def test_null_table_constraints(self):
        """Test handling when tableConstraints are None"""
        source = MockEntity(tableConstraints=None)
        destination = MockEntity(tableConstraints=None)

        # Should not raise any exceptions
        _table_constraints_handler(source, destination)

    def test_empty_table_constraints(self):
        """Test handling when tableConstraints are empty lists"""
        source = MockEntity(tableConstraints=[])
        destination = MockEntity(tableConstraints=[])

        # Should not raise any exceptions
        _table_constraints_handler(source, destination)
        self.assertEqual(destination.tableConstraints, [])

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
        self.assertEqual(len(destination.tableConstraints), 1)
        self.assertEqual(
            destination.tableConstraints[0].constraintType, ConstraintType.PRIMARY_KEY
        )
        self.assertEqual(destination.tableConstraints[0].columns, ["id"])

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
        self.assertEqual(len(destination.tableConstraints), 2)
        self.assertEqual(
            destination.tableConstraints[0].constraintType, ConstraintType.PRIMARY_KEY
        )
        self.assertEqual(destination.tableConstraints[0].columns, ["id"])
        self.assertEqual(
            destination.tableConstraints[1].constraintType, ConstraintType.UNIQUE
        )
        self.assertEqual(destination.tableConstraints[1].columns, ["name"])

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
        self.assertEqual(len(destination.tableConstraints), 2)
        self.assertEqual(
            destination.tableConstraints[0].constraintType, ConstraintType.PRIMARY_KEY
        )
        self.assertEqual(destination.tableConstraints[0].columns, ["id"])
        self.assertEqual(
            destination.tableConstraints[1].constraintType, ConstraintType.UNIQUE
        )
        self.assertEqual(destination.tableConstraints[1].columns, ["name"])

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
        self.assertEqual(len(destination.tableConstraints), 1)
        self.assertEqual(
            destination.tableConstraints[0].constraintType, ConstraintType.UNIQUE
        )
        # Column order in destination should be preserved
        self.assertEqual(
            destination.tableConstraints[0].columns, ["last_name", "first_name"]
        )

    def test_complex_constraint_rearrangement(self):
        """Test a complex scenario with multiple constraints being rearranged"""
        source = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.PRIMARY_KEY, columns=["id"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.FOREIGN_KEY, columns=["department_id"]
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
        self.assertEqual(len(destination.tableConstraints), 3)
        self.assertEqual(
            destination.tableConstraints[0].constraintType, ConstraintType.PRIMARY_KEY
        )
        self.assertEqual(destination.tableConstraints[0].columns, ["id"])
        self.assertEqual(
            destination.tableConstraints[1].constraintType, ConstraintType.UNIQUE
        )
        self.assertEqual(destination.tableConstraints[1].columns, ["email"])
        self.assertEqual(
            destination.tableConstraints[2].constraintType, ConstraintType.UNIQUE
        )
        self.assertEqual(destination.tableConstraints[2].columns, ["username"])

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
        self.assertEqual(len(destination.tableConstraints), 3)
        self.assertEqual(
            destination.tableConstraints[0].constraintType, ConstraintType.UNIQUE
        )
        self.assertEqual(destination.tableConstraints[0].columns, ["email"])
        self.assertEqual(
            destination.tableConstraints[1].constraintType, ConstraintType.UNIQUE
        )
        self.assertEqual(destination.tableConstraints[1].columns, ["username"])
        self.assertEqual(
            destination.tableConstraints[2].constraintType, ConstraintType.UNIQUE
        )
        self.assertEqual(destination.tableConstraints[2].columns, ["phone"])

    def test_foreign_key_different_referred_columns(self):
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
        # The destination constraint should be preserved (not replaced by source)
        self.assertEqual(len(destination.tableConstraints), 1)
        self.assertEqual(
            destination.tableConstraints[0].constraintType, ConstraintType.FOREIGN_KEY
        )
        self.assertEqual(destination.tableConstraints[0].columns, ["department_id"])
        self.assertEqual(
            destination.tableConstraints[0].referredColumns, ["public.department.id"]
        )

    def test_foreign_key_same_referred_columns(self):
        """Test that foreign keys with same referredColumns are treated as same constraints"""
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
                    referredColumns=["department.id"]  # Same referredColumns
                )
            ]
        )

        # Run the handler
        _table_constraints_handler(source, destination)

        # Should treat these as same constraints due to same referredColumns
        self.assertEqual(len(destination.tableConstraints), 1)
        self.assertEqual(
            destination.tableConstraints[0].constraintType, ConstraintType.FOREIGN_KEY
        )
        self.assertEqual(destination.tableConstraints[0].columns, ["department_id"])
        self.assertEqual(
            destination.tableConstraints[0].referredColumns, ["department.id"]
        )

    def test_mixed_constraints_with_foreign_keys(self):
        """Test complex scenario with mixed constraint types including foreign keys with referredColumns"""
        source = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.PRIMARY_KEY, columns=["id"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.FOREIGN_KEY,
                    columns=["department_id"],
                    referredColumns=["department.id"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.FOREIGN_KEY,
                    columns=["manager_id"],
                    referredColumns=["employee.id"]
                ),
            ]
        )

        destination = MockEntity(
            tableConstraints=[
                TableConstraint(
                    constraintType=ConstraintType.FOREIGN_KEY,
                    columns=["manager_id"],
                    referredColumns=["employee.id"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.PRIMARY_KEY, columns=["id"]
                ),
                TableConstraint(
                    constraintType=ConstraintType.FOREIGN_KEY,
                    columns=["department_id"],
                    referredColumns=["public.department.id"]  # Different referredColumns
                ),
            ]
        )

        # Run the handler
        _table_constraints_handler(source, destination)

        # Should rearrange to match source order and preserve different referredColumns
        self.assertEqual(len(destination.tableConstraints), 3)
        
        # First constraint should be PRIMARY_KEY (from source order)
        self.assertEqual(
            destination.tableConstraints[0].constraintType, ConstraintType.PRIMARY_KEY
        )
        self.assertEqual(destination.tableConstraints[0].columns, ["id"])
        
        # Second constraint should be FOREIGN_KEY with manager_id (from source order)
        self.assertEqual(
            destination.tableConstraints[1].constraintType, ConstraintType.FOREIGN_KEY
        )
        self.assertEqual(destination.tableConstraints[1].columns, ["manager_id"])
        self.assertEqual(
            destination.tableConstraints[1].referredColumns, ["employee.id"]
        )
        
        # Third constraint should be FOREIGN_KEY with department_id but different referredColumns
        # This should be treated as a new constraint and added at the end
        self.assertEqual(
            destination.tableConstraints[2].constraintType, ConstraintType.FOREIGN_KEY
        )
        self.assertEqual(destination.tableConstraints[2].columns, ["department_id"])
        self.assertEqual(
            destination.tableConstraints[2].referredColumns, ["public.department.id"]
        )
