# SPDX-License-Identifier: Apache-2.0
"""
Unit tests for owner_utils module
"""

import unittest
from unittest.mock import MagicMock

from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.entityReferenceList import EntityReferenceList
from metadata.utils.owner_utils import OwnerResolver, get_owner_from_config


class TestOwnerResolver(unittest.TestCase):
    """Test cases for OwnerResolver class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_metadata = MagicMock()

        # Mock successful owner lookup
        mock_owner = EntityReference(
            id="owner-id", type="user", name="test-user", fullyQualifiedName="test-user"
        )
        self.mock_owner_list = EntityReferenceList(root=[mock_owner])

    def test_default_not_handled_by_resolver(self):
        """Test that default owner is NOT handled by OwnerResolver (caller should handle it)"""
        config = {"default": "data-team"}

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)
        result = resolver.resolve_owner(entity_type="table", entity_name="test_table")

        # Should return None since no rule matches (default is not handled here)
        self.assertIsNone(result)

    def test_level_specific_owner(self):
        """Test level-specific owner configuration"""
        config = {
            "default": "default-team",
            "database": "db-team",
            "schema": "schema-team",
            "table": "table-team",
        }

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Test database level
        result = resolver.resolve_owner(entity_type="database", entity_name="test_db")
        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="db-team", is_owner=True
        )

        # Test table level
        result = resolver.resolve_owner(entity_type="table", entity_name="test_table")
        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="table-team", is_owner=True
        )

    def test_specific_entity_mapping(self):
        """Test specific entity name mapping"""
        config = {
            "table": {"orders": "sales-team", "customers": "customer-team"},
        }

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Test specific table mapping
        result = resolver.resolve_owner(entity_type="table", entity_name="orders")
        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="sales-team", is_owner=True
        )

        # Test unmapped table returns None (caller should handle default)
        result = resolver.resolve_owner(entity_type="table", entity_name="products")
        self.assertIsNone(result)

    def test_fqn_matching_priority(self):
        """Test FQN matching has priority over simple name"""
        config = {
            "table": {
                "sales_db.public.orders": "sales-team",
                "orders": "generic-team",  # Should not match when FQN matches
            },
        }

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Test FQN match has priority
        result = resolver.resolve_owner(
            entity_type="table", entity_name="sales_db.public.orders"
        )
        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="sales-team", is_owner=True
        )

    def test_simple_name_fallback(self):
        """Test automatic fallback to simple name when FQN doesn't match"""
        config = {"table": {"orders": "sales-team"}}

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Test FQN automatically falls back to simple name
        result = resolver.resolve_owner(
            entity_type="table", entity_name="sales_db.public.orders"
        )
        self.assertIsNotNone(result)
        # Should match on simple name "orders" (last part after '.')
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="sales-team", is_owner=True
        )

    def test_inheritance_enabled(self):
        """Test owner inheritance from parent (no default in config)"""
        config = {"enableInheritance": True, "table": {}}

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Table should inherit from schema owner
        result = resolver.resolve_owner(
            entity_type="table", entity_name="test_table", parent_owner="schema-team"
        )
        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="schema-team", is_owner=True
        )

    def test_inheritance_disabled(self):
        """Test that inheritance can be disabled"""
        config = {"enableInheritance": False, "table": {}}

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Table should NOT inherit, should return None (no rule match)
        result = resolver.resolve_owner(
            entity_type="table", entity_name="test_table", parent_owner="schema-team"
        )
        # Should return None since inheritance is disabled and no other rule matches
        self.assertIsNone(result)

    def test_priority_order(self):
        """Test priority order: specific > level > inheritance (default not handled here)"""
        config = {
            "enableInheritance": True,
            "table": {"orders": "specific-team"},
        }

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Specific configuration should have highest priority
        result = resolver.resolve_owner(
            entity_type="table", entity_name="orders", parent_owner="parent-team"
        )
        self.assertIsNotNone(result)
        # Should use specific, not parent
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="specific-team", is_owner=True
        )

    def test_owner_not_found(self):
        """Test handling when owner is not found"""
        config = {"table": "nonexistent-team"}

        self.mock_metadata.get_reference_by_name.return_value = None

        resolver = OwnerResolver(self.mock_metadata, config)
        result = resolver.resolve_owner(entity_type="table", entity_name="test_table")

        self.assertIsNone(result)

    def test_empty_config(self):
        """Test with empty configuration"""
        resolver = OwnerResolver(self.mock_metadata, {})
        result = resolver.resolve_owner(entity_type="table", entity_name="test_table")

        self.assertIsNone(result)

    def test_email_lookup(self):
        """Test owner lookup by email"""
        config = {"table": "admin@company.com"}

        # First call (by name) returns None, second call (by email) succeeds
        self.mock_metadata.get_reference_by_name.return_value = None
        self.mock_metadata.get_reference_by_email.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)
        result = resolver.resolve_owner(entity_type="table", entity_name="test_table")

        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_email.assert_called_with(
            "admin@company.com"
        )

    def test_multiple_owners_support(self):
        """Test support for multiple owners (list of owners)"""
        config = {"table": {"orders": ["sales-team", "finance-team"]}}

        # Mock two different owners
        mock_owner1 = EntityReference(
            id="owner-id-1",
            type="team",
            name="sales-team",
            fullyQualifiedName="sales-team",
        )
        mock_owner2 = EntityReference(
            id="owner-id-2",
            type="team",
            name="finance-team",
            fullyQualifiedName="finance-team",
        )

        # Configure mock to return different owners for different calls
        def mock_get_reference(name, is_owner=False):
            if name == "sales-team":
                return EntityReferenceList(root=[mock_owner1])
            elif name == "finance-team":
                return EntityReferenceList(root=[mock_owner2])
            return None

        self.mock_metadata.get_reference_by_name.side_effect = mock_get_reference

        resolver = OwnerResolver(self.mock_metadata, config)
        result = resolver.resolve_owner(entity_type="table", entity_name="orders")

        # Should return EntityReferenceList with both owners
        self.assertIsNotNone(result)
        self.assertEqual(len(result.root), 2)
        self.assertEqual(result.root[0].name, "sales-team")
        self.assertEqual(result.root[1].name, "finance-team")

    def test_level_config_with_list(self):
        """Test level-wide configuration with multiple owners"""
        config = {"table": ["team1", "team2"]}

        mock_owner1 = EntityReference(
            id="id1", type="team", name="team1", fullyQualifiedName="team1"
        )
        mock_owner2 = EntityReference(
            id="id2", type="team", name="team2", fullyQualifiedName="team2"
        )

        def mock_get_reference(name, is_owner=False):
            if name == "team1":
                return EntityReferenceList(root=[mock_owner1])
            elif name == "team2":
                return EntityReferenceList(root=[mock_owner2])
            return None

        self.mock_metadata.get_reference_by_name.side_effect = mock_get_reference

        resolver = OwnerResolver(self.mock_metadata, config)
        result = resolver.resolve_owner(entity_type="table", entity_name="any_table")

        self.assertIsNotNone(result)
        self.assertEqual(len(result.root), 2)


class TestGetOwnerFromConfig(unittest.TestCase):
    """Test cases for get_owner_from_config function"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_metadata = MagicMock()
        mock_owner = EntityReference(
            id="owner-id", type="user", name="test-user", fullyQualifiedName="test-user"
        )
        self.mock_owner_list = EntityReferenceList(root=[mock_owner])

    def test_string_config(self):
        """Test with string configuration (simple mode - treated as default)"""
        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        # String config is converted to {"default": "data-team"}
        # But since OwnerResolver doesn't handle default, this returns None
        result = get_owner_from_config(
            metadata=self.mock_metadata,
            owner_config="data-team",
            entity_type="table",
            entity_name="test_table",
        )

        # Note: This will return None because string config is treated as default
        # and OwnerResolver no longer handles default internally
        # Caller (database_service.py) should handle this scenario
        self.assertIsNone(result)

    def test_dict_config_with_rule(self):
        """Test with dict configuration containing rules"""
        config = {"table": "data-team"}
        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        result = get_owner_from_config(
            metadata=self.mock_metadata,
            owner_config=config,
            entity_type="table",
            entity_name="test_table",
        )

        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="data-team", is_owner=True
        )

    def test_none_config(self):
        """Test with None configuration"""
        result = get_owner_from_config(
            metadata=self.mock_metadata,
            owner_config=None,
            entity_type="table",
            entity_name="test_table",
        )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
