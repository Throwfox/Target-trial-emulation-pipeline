"""
Step 2: GLP-1 User/Non-User Identification

This module identifies GLP-1 users and potential non-users from the obesity cohort.

"""

import os
import sys
import json
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from types import SimpleNamespace
from typing import List, Dict
import logging

sys.path.append(str(Path(__file__).parent.parent))
from utils.omop_connector import OMOPConnector
from utils.helpers import summarize_cohort

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UserIdentifier:
    """Identify GLP-1 users and non-users"""
    
    def __init__(self, config: SimpleNamespace):
        self.config = config
        self.omop = OMOPConnector(config.omop_data_dir)
        
        # Load GLP-1 concept sets
        self.glp1_concepts = self._load_glp1_concepts()
        
        logger.info("UserIdentifier initialized")
    
    def _load_glp1_concepts(self) -> Dict[str, List[int]]:
        """Load GLP-1 drug concept sets"""
        glp1_drugs = {}
        
        # Search locations
        search_dirs = [
            Path(self.config.concept_sets_dir),  # 1. General concept sets dir (config/concept_sets)
            Path(self.config.glp1_concepts_dir), # 2. Specific GLP1 dir (usually config/concept_sets/glp1)
        ]
        
        for drug_name in self.config.glp1_drugs:
            concept_ids = []
            found = False
            
            for search_dir in search_dirs:
                if found: break
                
                # Try CSV
                csv_path = search_dir / f"{drug_name}_concepts.csv"
                if csv_path.exists():
                    try:
                        df = pd.read_csv(csv_path)
                        # Handle different column naming conventions
                        if 'CONCEPT_ID' in df.columns:
                            concept_ids = df['CONCEPT_ID'].tolist()
                        elif 'concept_id' in df.columns:
                            concept_ids = df['concept_id'].tolist()
                        
                        if concept_ids:
                            logger.info(f"Loaded {len(concept_ids)} concepts for {drug_name} from {csv_path}")
                            found = True
                            continue
                    except Exception as e:
                        logger.warning(f"Error reading {csv_path}: {e}")

                # Try JSON
                json_path = search_dir / f"{drug_name}_concepts.json"
                if json_path.exists():
                    try:
                        with open(json_path, 'r') as f:
                            data = json.load(f)
                            c_ids = data.get('concept_ids', [])
                            if data.get('include_descendants', True):
                                c_ids = self.omop.expand_concepts(c_ids)
                            concept_ids = c_ids
                            logger.info(f"Loaded {len(concept_ids)} concepts for {drug_name} from {json_path}")
                            found = True
                    except Exception as e:
                        logger.warning(f"Error reading {json_path}: {e}")
            
            glp1_drugs[drug_name] = concept_ids
            if not found:
                logger.warning(f"No concept file found for {drug_name}")
        
        return glp1_drugs
    
    def identify_users(self, cohort_df: pd.DataFrame) -> tuple:
        """
        Identify GLP-1 users and non-users
        
        Args:
            cohort_df: Obesity cohort DataFrame
            
        Returns:
            Tuple of (users_df, non_users_df)
        """
        logger.info("=" * 80)
        logger.info("STEP 2: IDENTIFYING GLP-1 USERS AND NON-USERS")
        logger.info("=" * 80)
        
        person_ids = cohort_df['person_id'].tolist()
        
        # Get all drug exposures for cohort
        logger.info("Retrieving drug exposures...")
        
        # Combine all GLP-1 concepts
        all_glp1_concepts = []
        for concepts in self.glp1_concepts.values():
            all_glp1_concepts.extend(concepts)
        all_glp1_concepts = list(set(all_glp1_concepts))
        
        logger.info(f"Searching for {len(all_glp1_concepts)} unique GLP-1 concepts")
        
        # Get GLP-1 exposures
        glp1_exposures = self.omop.get_drug_exposures(
            person_ids=person_ids,
            concept_ids=all_glp1_concepts,
            start_date=self.config.study_start_date,
            end_date=self.config.study_end_date
        )
        
        if len(glp1_exposures) == 0:
            logger.warning("No GLP-1 exposures found!")
            return pd.DataFrame(), cohort_df
        
        logger.info(f"Found {len(glp1_exposures)} GLP-1 exposure records")
        
        # Identify which specific GLP-1 drug
        glp1_exposures['glp1_drug_type'] = 'unknown'
        for drug_name, concept_ids in self.glp1_concepts.items():
            glp1_exposures.loc[
                glp1_exposures['drug_concept_id'].isin(concept_ids),
                'glp1_drug_type'
            ] = drug_name
        
        # Merge with cohort to get obesity entry dates
        glp1_with_cohort = glp1_exposures.merge(
            cohort_df[['person_id', 'obesity_entry_date']],
            on='person_id',
            how='inner'
        )
        
        glp1_with_cohort['drug_exposure_start_date'] = pd.to_datetime(
            glp1_with_cohort['drug_exposure_start_date']
        )
        glp1_with_cohort['obesity_entry_date'] = pd.to_datetime(
            glp1_with_cohort['obesity_entry_date']
        )
        
        # Filter: GLP-1 start must be on or after obesity entry
        glp1_valid = glp1_with_cohort[
            glp1_with_cohort['drug_exposure_start_date'] >= glp1_with_cohort['obesity_entry_date']
        ]
        
        logger.info(f"GLP-1 exposures after obesity entry: {len(glp1_valid)} records")
        
        # Get first GLP-1 exposure for each patient (index date)
        glp1_first = glp1_valid.groupby('person_id').agg({
            'drug_exposure_start_date': 'min',
            'glp1_drug_type': 'first'
        }).reset_index()
        
        glp1_first = glp1_first.rename(columns={
            'drug_exposure_start_date': 'glp1_index_date'
        })
        
        logger.info(f"Identified {len(glp1_first)} GLP-1 users")
        
        # Drug type distribution
        drug_counts = glp1_first['glp1_drug_type'].value_counts()
        logger.info("\nGLP-1 drug distribution:")
        for drug, count in drug_counts.items():
            logger.info(f"  - {drug}: {count}")
        
        # Merge users with full cohort data
        users = cohort_df.merge(glp1_first, on='person_id', how='inner')
        users['user_flag'] = 1
        
        # Identify non-users (no GLP-1 exposure)
        glp1_user_ids = set(glp1_first['person_id'].tolist())
        non_users = cohort_df[~cohort_df['person_id'].isin(glp1_user_ids)].copy()
        non_users['user_flag'] = 0
        
        logger.info(f"Identified {len(non_users)} potential non-users")
        
        # Save results
        users_path = os.path.join(
            self.config.output_dir, 'cohorts',
            f"{self.config.cohort_name}_glp1_users.csv"
        )
        non_users_path = os.path.join(
            self.config.output_dir, 'cohorts',
            f"{self.config.cohort_name}_glp1_nonusers.csv"
        )
        
        users.to_csv(users_path, index=False)
        non_users.to_csv(non_users_path, index=False)
        
        logger.info(f"\n{'=' * 80}")
        logger.info("USER IDENTIFICATION COMPLETE")
        logger.info(f"{'=' * 80}")
        logger.info(f"GLP-1 Users: {len(users)}")
        logger.info(f"Non-Users: {len(non_users)}")
        logger.info(f"Users saved to: {users_path}")
        logger.info(f"Non-users saved to: {non_users_path}")
        logger.info(f"{'=' * 80}\n")
        
        return users, non_users


def execute(config_path: str):
    """Execute user identification"""
    with open(config_path, 'r') as f:
        config = SimpleNamespace(**json.load(f))
    
    # Load obesity cohort
    cohort_path = os.path.join(
        config.output_dir, 'cohorts',
        f"{config.cohort_name}_obesity_cohort.csv"
    )
    cohort = pd.read_csv(cohort_path)
    
    # Identify users
    identifier = UserIdentifier(config)
    users, non_users = identifier.identify_users(cohort)
    
    return users, non_users


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Identify GLP-1 users and non-users")
    parser.add_argument("-c", "--config", type=str, required=True,
                       help="Path to configuration JSON file")
    
    args = parser.parse_args()
    execute(args.config)

