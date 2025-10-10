# SPDX-License-Identifier: Apache-2.0
"""
Owner resolution utilities for metadata ingestion.

This module provides utilities to resolve owners for entities based on hierarchical
configuration following the topology structure (service -> database -> schema -> table).
"""

import traceback
from typing import Dict, List, Optional, Union

from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.entityReferenceList import EntityReferenceList
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class OwnerResolver:
    """
    Resolves owners for entities based on hierarchical ownerConfig.

    Configuration structure:
    {
        "default": "fallback-owner",  # Default owner (handled by caller)
        "service": "service-owner",   # Optional
        "database": "db-owner" | {"db1": "owner1", "db2": ["owner1", "owner2"]},
        "schema": "schema-owner" | {"schema1": "owner1"},
        "table": "table-owner" | {"table1": ["owner1", "owner2"]},
        "enableInheritance": true  # Default true
    }

    Resolution order (highest to lowest priority) - Rules only:
    1. Current level custom configuration (FQN match first, then simple name)
    2. Current level general configuration (string or list format)
    3. Inherited parent owner (if enableInheritance=true)
    Note: Default owner is NOT handled here - caller should handle it separately
    """

    def __init__(self, metadata: OpenMetadata, owner_config: Optional[Dict] = None):
        """
        Initialize the owner resolver

        Args:
            metadata: OpenMetadata client for owner lookups
            owner_config: Owner configuration dict
        """
        self.metadata = metadata
        self.config = owner_config or {}
        self.enable_inheritance = self.config.get("enableInheritance", True)

    def resolve_owner(
        self,
        entity_type: str,
        entity_name: str,
        parent_owner: Optional[Union[str, List[str]]] = None,
    ) -> Optional[EntityReferenceList]:
        """
        Resolve owner for an entity based on rule configuration only.
        Does NOT handle default owner - caller should handle that separately.

        Args:
            entity_type: Type of entity ("database", "databaseSchema", "table")
            entity_name: Name or FQN of the entity
            parent_owner: Owner(s) inherited from parent entity

        Returns:
            EntityReferenceList with resolved owner(s), or None if no rule matches
        """
        if not self.config:
            return None

        try:
            logger.debug(
                f"Resolving owner for {entity_type} '{entity_name}', parent_owner: {parent_owner}"
            )
            logger.debug(f"Full config: {self.config}")

            # 1. Try to get owner from current level configuration
            level_config = self.config.get(entity_type)
            logger.debug(f"Level config for '{entity_type}': {level_config}")

            if level_config:
                # If it's a dict, try exact matching (FQN first, then simple name)
                if isinstance(level_config, dict):
                    # Priority 1a: Try FQN exact match first
                    if entity_name in level_config:
                        owner_config = level_config[entity_name]
                        owner_ref = self._get_owner_ref(owner_config)
                        if owner_ref:
                            logger.debug(
                                f"Using specific {entity_type} owner (FQN match) for '{entity_name}': {owner_config}"
                            )
                            return owner_ref

                    # Priority 1b: Try simple name match (extract last part after '.')
                    if "." in entity_name:
                        simple_name = entity_name.split(".")[-1]
                        if simple_name in level_config:
                            owner_config = level_config[simple_name]
                            owner_ref = self._get_owner_ref(owner_config)
                            if owner_ref:
                                logger.debug(
                                    f"Using specific {entity_type} owner (simple name match) for '{simple_name}': {owner_config}"
                                )
                                return owner_ref

                # If it's a string or list, use it directly as level-wide configuration
                elif isinstance(level_config, (str, list)):
                    owner_ref = self._get_owner_ref(level_config)
                    if owner_ref:
                        logger.debug(
                            f"Using {entity_type} level owner for '{entity_name}': {level_config}"
                        )
                        return owner_ref

            # 2. If inheritance is enabled, use parent owner
            if self.enable_inheritance and parent_owner:
                owner_ref = self._get_owner_ref(parent_owner)
                if owner_ref:
                    logger.debug(
                        f"Using inherited owner for '{entity_name}': {parent_owner}"
                    )
                    return owner_ref

            # Note: Default owner is NOT handled here
            # Caller (database_service.py) should handle default separately
            # to maintain proper priority: rules > source > default

        except Exception as exc:
            logger.warning(
                f"Error resolving owner for {entity_type} '{entity_name}': {exc}"
            )
            logger.debug(traceback.format_exc())

        return None

    def _get_owner_ref(
        self, owner_config: Union[str, List[str]]
    ) -> Optional[EntityReferenceList]:
        """
        Get owner reference(s) from OpenMetadata.
        Supports single owner (string) or multiple owners (list of strings).

        Args:
            owner_config: Single owner name/email or list of owner names/emails

        Returns:
            EntityReferenceList with owner(s) or None if none found
        """
        try:
            if not owner_config:
                return None

            # Normalize to list for unified processing
            owner_names = (
                [owner_config] if isinstance(owner_config, str) else owner_config
            )

            owner_refs: List[EntityReference] = []

            for owner_name in owner_names:
                if not owner_name:
                    continue

                # Try to get owner by name (handles both users and teams)
                owner_ref = self.metadata.get_reference_by_name(
                    name=owner_name, is_owner=True
                )

                if owner_ref and owner_ref.root:
                    # Collect all EntityReference objects
                    owner_refs.extend(owner_ref.root)
                    continue

                # Try by email if name lookup failed and it looks like an email
                if "@" in owner_name:
                    owner_ref = self.metadata.get_reference_by_email(owner_name)
                    if owner_ref and owner_ref.root:
                        owner_refs.extend(owner_ref.root)
                        continue

                logger.warning(f"Could not find owner: {owner_name}")

            # Return EntityReferenceList if we found any owners
            if owner_refs:
                return EntityReferenceList(root=owner_refs)

        except Exception as exc:
            logger.debug(f"Error getting owner reference for '{owner_config}': {exc}")
            logger.debug(traceback.format_exc())

        return None


def get_owner_from_config(
    metadata: OpenMetadata,
    owner_config: Optional[Union[str, Dict]],
    entity_type: str,
    entity_name: str,
    parent_owner: Optional[str] = None,
) -> Optional[EntityReferenceList]:
    """
    Convenience function to resolve owner from configuration

    Args:
        metadata: OpenMetadata client
        owner_config: Owner configuration (string for simple mode, dict for hierarchical mode)
        entity_type: Type of entity ("database", "databaseSchema", "table")
        entity_name: Name or FQN of the entity
        parent_owner: Owner inherited from parent entity

    Returns:
        EntityReferenceList with resolved owner, or None
    """
    logger.debug(
        f"get_owner_from_config called: entity_type={entity_type}, entity_name={entity_name}, owner_config type={type(owner_config)}"
    )

    # Handle simple string mode (single owner for all entities)
    if isinstance(owner_config, str):
        resolver = OwnerResolver(metadata, {"default": owner_config})
        return resolver.resolve_owner(entity_type, entity_name, parent_owner)

    # Handle new ownerConfig dict mode or Pydantic model
    if isinstance(owner_config, dict):
        resolver = OwnerResolver(metadata, owner_config)
        return resolver.resolve_owner(entity_type, entity_name, parent_owner)

    # Handle Pydantic model (convert to dict)
    if hasattr(owner_config, "model_dump"):
        logger.debug("Converting Pydantic model to dict")
        config_dict = owner_config.model_dump(exclude_none=True)
        resolver = OwnerResolver(metadata, config_dict)
        return resolver.resolve_owner(entity_type, entity_name, parent_owner)

    logger.debug(f"Unsupported owner_config type: {type(owner_config)}")
    return None
