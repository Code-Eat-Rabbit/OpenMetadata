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
        "default": "fallback-owner" | ["owner1", "owner2"],  # Default owner(s) for all entities
        "service": "service-owner" | ["owner1", "owner2"],   # Optional
        "database": "db-owner" | {"db1": "owner1" | ["owner1", "owner2"]},
        "databaseSchema": "schema-owner" | {"schema1": "owner1" | ["owner1", "owner2"]},
        "table": "table-owner" | {"table1": "owner1" | ["owner1", "owner2"]},
        "enableInheritance": true,  # Default true - inherit from parent entity
        "ownerPriority": ["rule", "source", "default"]  # Configurable priority order
    }

    Resolution order (configurable via ownerPriority):
    - "rule": Current level configuration from ownerConfig (FQN > name match)
    - "source": Owner from original data source/database system (requires includeOwners=true)
    - "default": Default owner configuration
    - Inheritance: Can inherit from parent entity when no owner found (controlled by enableInheritance)

    Default priority order: ["rule", "source", "default"]
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
        # Default priority order: rule -> source -> default
        self.owner_priority = self.config.get("ownerPriority", ["rule", "source", "default"])

    def resolve_owner(
        self,
        entity_type: str,
        entity_name: str,
        source_owner: Optional[str] = None,
        parent_owner: Optional[str] = None,
    ) -> Optional[EntityReferenceList]:
        """
        Resolve owner for an entity based on configuration

        Args:
            entity_type: Type of entity ("database", "databaseSchema", "table")
            entity_name: Name or FQN of the entity
            source_owner: Owner from original data source/database system (from includeOwners)
            parent_owner: Owner inherited from parent entity (for inheritance)

        Returns:
            EntityReferenceList with resolved owner, or None
        """
        if not self.config:
            return None

        try:
            logger.debug(
                f"Resolving owner for {entity_type} '{entity_name}', source_owner: {source_owner}, parent_owner: {parent_owner}"
            )
            logger.debug(f"Full config: {self.config}")
            logger.debug(f"Owner priority: {self.owner_priority}")

            # Try each priority level in configured order
            for priority in self.owner_priority:
                owner_ref = None
                
                if priority == "rule":
                    # Try to get owner from current level configuration
                    owner_ref = self._resolve_from_rule(entity_type, entity_name)
                    
                elif priority == "source" and source_owner:
                    # Try owner from original data source/database system
                    owner_ref = self._get_owner_refs(source_owner)
                    if owner_ref:
                        logger.debug(
                            f"Using source owner for '{entity_name}': {source_owner}"
                        )
                        
                elif priority == "default":
                    # Try default owner
                    default_owner = self.config.get("default")
                    if default_owner:
                        owner_ref = self._get_owner_refs(default_owner)
                        if owner_ref:
                            logger.debug(
                                f"Using default owner for '{entity_name}': {default_owner}"
                            )
                
                # Return first successful resolution
                if owner_ref:
                    return owner_ref
            
            # If no owner found and inheritance is enabled, try parent owner
            if self.enable_inheritance and parent_owner:
                owner_ref = self._get_owner_refs(parent_owner)
                if owner_ref:
                    logger.debug(
                        f"Using inherited parent owner for '{entity_name}': {parent_owner}"
                    )
                    return owner_ref

        except Exception as exc:
            logger.warning(
                f"Error resolving owner for {entity_type} '{entity_name}': {exc}"
            )
            logger.debug(traceback.format_exc())

        return None

    def _resolve_from_rule(
        self, entity_type: str, entity_name: str
    ) -> Optional[EntityReferenceList]:
        """
        Resolve owner from rule configuration.
        Priority: FQN match > simple name match > general level config
        
        Args:
            entity_type: Type of entity
            entity_name: Name or FQN of the entity
            
        Returns:
            EntityReferenceList or None
        """
        level_config = self.config.get(entity_type)
        logger.debug(f"Level config for '{entity_type}': {level_config}")
        
        if not level_config:
            return None
            
        # If it's a dict, try exact matching with FQN first, then simple name
        if isinstance(level_config, dict):
            # Priority 1: Try exact FQN match
            if entity_name in level_config:
                owner_value = level_config[entity_name]
                owner_ref = self._get_owner_refs(owner_value)
                if owner_ref:
                    logger.debug(
                        f"Using FQN-matched {entity_type} owner for '{entity_name}': {owner_value}"
                    )
                    return owner_ref
            
            # Priority 2: Try simple name match (last part of FQN)
            simple_name = entity_name.split(".")[-1]
            if simple_name != entity_name and simple_name in level_config:
                owner_value = level_config[simple_name]
                owner_ref = self._get_owner_refs(owner_value)
                if owner_ref:
                    logger.debug(
                        f"Using name-matched {entity_type} owner for '{simple_name}': {owner_value}"
                    )
                    return owner_ref
                    
        # If it's a string or list, use it directly as general level config
        elif isinstance(level_config, (str, list)):
            owner_ref = self._get_owner_refs(level_config)
            if owner_ref:
                logger.debug(
                    f"Using {entity_type} level owner for '{entity_name}': {level_config}"
                )
                return owner_ref
                
        return None
    
    def _get_owner_refs(
        self, owner_value: Union[str, List[str]]
    ) -> Optional[EntityReferenceList]:
        """
        Get owner references from owner value (supports single or multiple owners)
        
        Args:
            owner_value: Owner name/email or list of owner names/emails
            
        Returns:
            EntityReferenceList or None
        """
        if isinstance(owner_value, list):
            # Handle multiple owners
            owner_refs = []
            for owner_name in owner_value:
                owner_ref = self._get_single_owner_ref(owner_name)
                if owner_ref:
                    owner_refs.append(owner_ref)
            
            if owner_refs:
                return EntityReferenceList(root=owner_refs)
            return None
        else:
            # Handle single owner
            owner_ref = self._get_single_owner_ref(owner_value)
            if owner_ref:
                return EntityReferenceList(root=[owner_ref])
            return None
    
    def _get_single_owner_ref(self, owner_name: str) -> Optional[EntityReference]:
        """
        Get single owner reference from OpenMetadata

        Args:
            owner_name: User or team name/email

        Returns:
            EntityReference or None if not found
        """
        try:
            if not owner_name:
                return None

            # Try to get owner by name (handles both users and teams)
            owner_ref_list = self.metadata.get_reference_by_name(
                name=owner_name, is_owner=True
            )

            if owner_ref_list and owner_ref_list.root:
                return owner_ref_list.root[0]

            # Try by email if name lookup failed and it looks like an email
            if "@" in owner_name:
                owner_ref_list = self.metadata.get_reference_by_email(owner_name)
                if owner_ref_list and owner_ref_list.root:
                    return owner_ref_list.root[0]

            logger.warning(f"Could not find owner: {owner_name}")

        except Exception as exc:
            logger.debug(f"Error getting owner reference for '{owner_name}': {exc}")
            logger.debug(traceback.format_exc())

        return None


def get_owner_from_config(
    metadata: OpenMetadata,
    owner_config: Optional[Union[str, Dict]],
    entity_type: str,
    entity_name: str,
    source_owner: Optional[str] = None,
    parent_owner: Optional[str] = None,
) -> Optional[EntityReferenceList]:
    """
    Convenience function to resolve owner from configuration

    Args:
        metadata: OpenMetadata client
        owner_config: Owner configuration (string for simple mode, dict for hierarchical mode)
        entity_type: Type of entity ("database", "databaseSchema", "table")
        entity_name: Name or FQN of the entity
        source_owner: Owner from original data source/database system (from includeOwners)
        parent_owner: Owner inherited from parent entity (for inheritance)

    Returns:
        EntityReferenceList with resolved owner, or None
    """
    logger.debug(
        f"get_owner_from_config called: entity_type={entity_type}, entity_name={entity_name}, source_owner={source_owner}, owner_config type={type(owner_config)}"
    )

    # Handle simple string mode (single owner for all entities)
    if isinstance(owner_config, str):
        resolver = OwnerResolver(metadata, {"default": owner_config})
        return resolver.resolve_owner(entity_type, entity_name, source_owner, parent_owner)

    # Handle new ownerConfig dict mode or Pydantic model
    if isinstance(owner_config, dict):
        resolver = OwnerResolver(metadata, owner_config)
        return resolver.resolve_owner(entity_type, entity_name, source_owner, parent_owner)

    # Handle Pydantic model (convert to dict)
    if hasattr(owner_config, "model_dump"):
        logger.debug("Converting Pydantic model to dict")
        config_dict = owner_config.model_dump(exclude_none=True)
        resolver = OwnerResolver(metadata, config_dict)
        return resolver.resolve_owner(entity_type, entity_name, source_owner, parent_owner)

    logger.debug(f"Unsupported owner_config type: {type(owner_config)}")
    return None
