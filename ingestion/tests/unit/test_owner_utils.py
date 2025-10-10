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

    def test_simple_default_owner(self):
        """Test simple default owner configuration"""
        config = {"default": "data-team"}

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)
        result = resolver.resolve_owner(entity_type="table", entity_name="test_table")

        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="data-team", is_owner=True
        )

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
            "default": "default-team",
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

        # Test unmapped table falls back to default
        result = resolver.resolve_owner(entity_type="table", entity_name="products")
        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="default-team", is_owner=True
        )

    def test_fqn_matching(self):
        """Test FQN matching for entities (FQN should be prioritized over simple name)"""
        config = {
            "default": "default-team",
            "table": {
                "sales_db.public.orders": "sales-team",
                "analytics_db.public.reports": "analytics-team",
                "orders": "wrong-team",  # This should NOT match when FQN is provided
            },
        }

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Test FQN match - should use FQN match, not simple name match
        result = resolver.resolve_owner(
            entity_type="table", entity_name="sales_db.public.orders"
        )
        self.assertIsNotNone(result)
        # Should match "sales-team" from FQN, not "wrong-team" from simple name
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="sales-team", is_owner=True
        )

    def test_simple_name_fallback(self):
        """Test fallback to simple name when FQN doesn't match"""
        config = {"default": "default-team", "table": {"orders": "sales-team"}}

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Test FQN that falls back to simple name
        result = resolver.resolve_owner(
            entity_type="table", entity_name="sales_db.public.orders"
        )
        self.assertIsNotNone(result)
        # Should match on simple name "orders"
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="sales-team", is_owner=True
        )

    def test_inheritance_enabled(self):
        """Test owner inheritance from parent"""
        config = {"default": "default-team", "enableInheritance": True, "table": {}}

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Table should inherit from schema owner when no other owner is found
        result = resolver.resolve_owner(
            entity_type="table", 
            entity_name="test_table", 
            source_owner=None,
            parent_owner="schema-team"
        )
        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="schema-team", is_owner=True
        )

    def test_inheritance_disabled(self):
        """Test that inheritance can be disabled"""
        config = {"default": "default-team", "enableInheritance": False, "table": {}}

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Table should NOT inherit, should use default
        result = resolver.resolve_owner(
            entity_type="table", 
            entity_name="test_table", 
            source_owner=None,
            parent_owner="schema-team"
        )
        self.assertIsNotNone(result)
        # Should use default, not parent
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="default-team", is_owner=True
        )

    def test_priority_order(self):
        """Test priority order: rule > source > default (configurable)"""
        config = {
            "default": "default-team",
            "enableInheritance": True,
            "table": {"orders": "specific-team"},
            "ownerPriority": ["rule", "source", "default"],
        }

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Rule configuration should have highest priority
        result = resolver.resolve_owner(
            entity_type="table", 
            entity_name="orders", 
            source_owner="source-team",  # This should be ignored because rule has priority
            parent_owner="parent-team"
        )
        self.assertIsNotNone(result)
        # Should use rule (specific), not source or default
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="specific-team", is_owner=True
        )

    def test_owner_not_found(self):
        """Test handling when owner is not found"""
        config = {"default": "nonexistent-team"}

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
        config = {"default": "admin@company.com"}

        # First call (by name) returns None, second call (by email) succeeds
        self.mock_metadata.get_reference_by_name.return_value = None
        self.mock_metadata.get_reference_by_email.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)
        result = resolver.resolve_owner(entity_type="table", entity_name="test_table")

        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_email.assert_called_with(
            "admin@company.com"
        )


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
        """Test with string configuration (simple mode)"""
        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        result = get_owner_from_config(
            metadata=self.mock_metadata,
            owner_config="data-team",
            entity_type="table",
            entity_name="test_table",
            source_owner=None,
            parent_owner=None,
        )

        self.assertIsNotNone(result)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="data-team", is_owner=True
        )

    def test_dict_config(self):
        """Test with dict configuration"""
        config = {"default": "data-team"}
        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        result = get_owner_from_config(
            metadata=self.mock_metadata,
            owner_config=config,
            entity_type="table",
            entity_name="test_table",
            source_owner=None,
            parent_owner=None,
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
            source_owner=None,
            parent_owner=None,
        )

        self.assertIsNone(result)
    
    def test_with_source_owner(self):
        """Test with source owner from database system"""
        config = {
            "default": "default-team",
            "ownerPriority": ["source", "default"],  # Source first
        }
        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        result = get_owner_from_config(
            metadata=self.mock_metadata,
            owner_config=config,
            entity_type="table",
            entity_name="test_table",
            source_owner="db-owner",  # From includeOwners
            parent_owner=None,
        )

        self.assertIsNotNone(result)
        # Should use source owner (from database)
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="db-owner", is_owner=True
        )


    def test_multiple_owners(self):
        """Test multiple owners support"""
        config = {
            "default": ["team1", "team2"],
            "table": {"orders": ["sales-team", "finance-team"]},
        }

        mock_owner1 = EntityReference(
            id="owner1", type="team", name="sales-team", fullyQualifiedName="sales-team"
        )
        mock_owner2 = EntityReference(
            id="owner2", type="team", name="finance-team", fullyQualifiedName="finance-team"
        )
        self.mock_metadata.get_reference_by_name.side_effect = [
            EntityReferenceList(root=[mock_owner1]),
            EntityReferenceList(root=[mock_owner2]),
        ]

        resolver = OwnerResolver(self.mock_metadata, config)

        result = resolver.resolve_owner(entity_type="table", entity_name="orders")
        self.assertIsNotNone(result)
        self.assertEqual(len(result.root), 2)
        self.assertEqual(result.root[0].name, "sales-team")
        self.assertEqual(result.root[1].name, "finance-team")

    def test_source_priority_over_rule(self):
        """Test that source owner (from database) can have priority over rule"""
        # Priority: source > rule > default (source first!)
        config = {
            "default": "default-team",
            "enableInheritance": True,
            "table": {"orders": "rule-team"},
            "ownerPriority": ["source", "rule", "default"],
        }

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # With custom priority, source (from database) should win over rule
        result = resolver.resolve_owner(
            entity_type="table", 
            entity_name="orders", 
            source_owner="db-source-team",  # From includeOwners
            parent_owner="parent-team"
        )
        self.assertIsNotNone(result)
        # Should use source (from database), not rule
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="db-source-team", is_owner=True
        )

    def test_fqn_priority_over_simple_name(self):
        """Test that FQN match has priority over simple name match"""
        config = {
            "default": "default-team",
            "database": {
                "service.production_db": "prod-team",  # FQN match
                "production_db": "dev-team",  # Simple name match
            },
        }

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # Test with FQN - should match FQN config
        result = resolver.resolve_owner(
            entity_type="database", entity_name="service.production_db"
        )
        self.assertIsNotNone(result)
        # Should match "prod-team" from FQN, not "dev-team" from simple name
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="prod-team", is_owner=True
        )

    def test_source_and_inheritance(self):
        """Test that source (database owner) and inheritance (parent owner) are independent"""
        config = {
            "default": "default-team",
            "enableInheritance": True,
            "table": {},  # No rule
            "ownerPriority": ["rule", "source", "default"],
        }

        self.mock_metadata.get_reference_by_name.return_value = self.mock_owner_list

        resolver = OwnerResolver(self.mock_metadata, config)

        # No rule, source owner should be used
        result = resolver.resolve_owner(
            entity_type="table", 
            entity_name="orders", 
            source_owner="db-owner",  # From includeOwners
            parent_owner="parent-team"  # For inheritance
        )
        self.assertIsNotNone(result)
        # Should use source (from database), not parent or default
        self.mock_metadata.get_reference_by_name.assert_called_with(
            name="db-owner", is_owner=True
        )
    
    def test_inheritance_as_fallback(self):
        """Test that inheritance works as fallback when no owner is found from priority"""
        config = {
            "default": "default-team",
            "enableInheritance": True,
            "table": {},  # No rule
            "ownerPriority": ["rule", "source", "default"],
        }

        # First call (parent) succeeds, others return None
        self.mock_metadata.get_reference_by_name.side_effect = [
            None,  # default-team lookup fails
            self.mock_owner_list,  # parent-team succeeds
        ]

        resolver = OwnerResolver(self.mock_metadata, config)

        # No rule, no source, default fails -> should fall back to inheritance
        result = resolver.resolve_owner(
            entity_type="table", 
            entity_name="orders", 
            source_owner=None,  # No source owner
            parent_owner="parent-team"  # Should be used as fallback
        )
        self.assertIsNotNone(result)
        # Should eventually use parent as fallback


if __name__ == "__main__":
    unittest.main()
