"""
Concept Expander: Utility for expanding OMOP concept IDs using concept hierarchies
"""

import pandas as pd
from typing import List, Dict, Set, Optional
import logging

logger = logging.getLogger(__name__)


class ConceptExpander:
    """
    Expands OMOP concept IDs using concept_ancestor relationships
    """
    
    def __init__(self, concept_ancestor_df: pd.DataFrame,
                 concept_df: Optional[pd.DataFrame] = None):
        """
        Initialize ConceptExpander
        
        Args:
            concept_ancestor_df: DataFrame with ancestor/descendant relationships
            concept_df: Optional DataFrame with concept details
        """
        self.concept_ancestor = concept_ancestor_df
        self.concept = concept_df
        
        # Build lookup dictionary for fast queries
        self.ancestor_map = self._build_ancestor_map()
        
        logger.info(f"ConceptExpander initialized with {len(concept_ancestor_df)} relationships")
    
    def _build_ancestor_map(self) -> Dict[int, Set[int]]:
        """Build a dictionary mapping ancestor_concept_id to descendant_concept_ids"""
        ancestor_map = {}
        
        for _, row in self.concept_ancestor.iterrows():
            ancestor_id = row['ancestor_concept_id']
            descendant_id = row['descendant_concept_id']
            
            if ancestor_id not in ancestor_map:
                ancestor_map[ancestor_id] = set()
            ancestor_map[ancestor_id].add(descendant_id)
        
        return ancestor_map
    
    def expand(self, concept_ids: List[int],
               include_self: bool = True) -> List[int]:
        """
        Expand concept IDs to include all descendants
        
        Args:
            concept_ids: List of ancestor concept IDs
            include_self: Whether to include the input concepts in output
            
        Returns:
            List of expanded concept IDs
        """
        expanded = set()
        
        for concept_id in concept_ids:
            if include_self:
                expanded.add(concept_id)
            
            # Add all descendants
            if concept_id in self.ancestor_map:
                expanded.update(self.ancestor_map[concept_id])
        
        expanded_list = sorted(list(expanded))
        
        logger.info(f"Expanded {len(concept_ids)} concepts to {len(expanded_list)} concepts")
        return expanded_list
    
    def get_concept_name(self, concept_id: int) -> Optional[str]:
        """
        Get concept name from concept_id
        
        Args:
            concept_id: Concept ID
            
        Returns:
            Concept name or None if not found
        """
        if self.concept is None:
            logger.warning("Concept DataFrame not provided")
            return None
        
        match = self.concept[self.concept['concept_id'] == concept_id]
        if len(match) > 0:
            return match.iloc[0]['concept_name']
        return None
    
    def filter_by_vocabulary(self, concept_ids: List[int],
                            vocabulary_id: str) -> List[int]:
        """
        Filter concept IDs by vocabulary
        
        Args:
            concept_ids: List of concept IDs
            vocabulary_id: Vocabulary ID (e.g., 'RxNorm', 'SNOMED', 'ICD10CM')
            
        Returns:
            Filtered list of concept IDs
        """
        if self.concept is None:
            logger.warning("Concept DataFrame not provided, cannot filter by vocabulary")
            return concept_ids
        
        filtered = self.concept[
            (self.concept['concept_id'].isin(concept_ids)) &
            (self.concept['vocabulary_id'] == vocabulary_id)
        ]['concept_id'].tolist()
        
        logger.info(f"Filtered {len(concept_ids)} concepts to {len(filtered)} in vocabulary {vocabulary_id}")
        return filtered
    
    def filter_standard_concepts(self, concept_ids: List[int]) -> List[int]:
        """
        Filter to standard concepts only
        
        Args:
            concept_ids: List of concept IDs
            
        Returns:
            Filtered list of standard concept IDs
        """
        if self.concept is None:
            logger.warning("Concept DataFrame not provided, cannot filter standard concepts")
            return concept_ids
        
        filtered = self.concept[
            (self.concept['concept_id'].isin(concept_ids)) &
            (self.concept['standard_concept'] == 'S')
        ]['concept_id'].tolist()
        
        logger.info(f"Filtered {len(concept_ids)} concepts to {len(filtered)} standard concepts")
        return filtered

