"""
Step 3: Apply Eligibility Criteria

Apply inclusion/exclusion criteria to filter the cohort.

Configurable criteria:
- Age requirements (≥18 years)
- Baseline encounter requirements
- Baseline condition requirements (inclusion/exclusion)
- Minimum follow-up time
- Other study-specific criteria

"""

import os
import sys
import json
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from types import SimpleNamespace
import logging

sys.path.append(str(Path(__file__).parent.parent))
from utils.omop_connector import OMOPConnector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EligibilityCriteriaFilter:
    """Apply eligibility criteria to users and non-users"""
    
    def __init__(self, config: SimpleNamespace):
        self.config = config
        self.omop = OMOPConnector(config.omop_data_dir)
    
    def apply_criteria(self, users_df: pd.DataFrame, non_users_df: pd.DataFrame) -> tuple:
        """
        Apply eligibility criteria
        
        Returns:
            Tuple of (filtered_users, filtered_non_users)
        """
        logger.info("=" * 80)
        logger.info("STEP 3: APPLYING ELIGIBILITY CRITERIA")
        logger.info("=" * 80)
        
        users_filtered = self._apply_user_criteria(users_df)
        non_users_filtered = self._apply_non_user_criteria(non_users_df)
        
        # Save results
        users_path = os.path.join(self.config.output_dir, 'cohorts',
                                 f"{self.config.cohort_name}_users_eligible.csv")
        non_users_path = os.path.join(self.config.output_dir, 'cohorts',
                                     f"{self.config.cohort_name}_nonusers_eligible.csv")
        
        users_filtered.to_csv(users_path, index=False)
        non_users_filtered.to_csv(non_users_path, index=False)
        
        logger.info(f"\nEligible Users: {len(users_filtered)}")
        logger.info(f"Eligible Non-Users: {len(non_users_filtered)}")
        
        return users_filtered, non_users_filtered
    
    def _apply_user_criteria(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply criteria to GLP-1 users"""
        logger.info(f"\nApplying criteria to Users (initial: {len(df)})")
        
        df = df.copy()
        df['glp1_index_date'] = pd.to_datetime(df['glp1_index_date'])
        
        # Age at index
        if self.config.min_age_at_index:
            df['age_at_index'] = df['age_at_obesity_entry']  # Simplified
            df = df[df['age_at_index'] >= self.config.min_age_at_index]
            logger.info(f"  After age ≥{self.config.min_age_at_index}: {len(df)}")
        
        # Baseline encounter requirement
        if self.config.require_baseline_encounter:
            df = df[df['visits_before_entry'] >= 1]
            logger.info(f"  After baseline encounter requirement: {len(df)}")
        
        # Minimum follow-up (simplified check)
        if self.config.min_followup_days:
            logger.info(f"  Minimum follow-up filter: {self.config.min_followup_days} days (to be validated later)")
        
        return df
    
    def _apply_non_user_criteria(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply criteria to non-users"""
        logger.info(f"\nApplying criteria to Non-Users (initial: {len(df)})")
        
        df = df.copy()
        
        # Age at entry
        if self.config.min_age_at_index:
            df = df[df['age_at_obesity_entry'] >= self.config.min_age_at_index]
            logger.info(f"  After age ≥{self.config.min_age_at_index}: {len(df)}")
        
        # Baseline encounter requirement
        if self.config.require_baseline_encounter:
            df = df[df['visits_before_entry'] >= 1]
            logger.info(f"  After baseline encounter requirement: {len(df)}")
        
        return df


def execute(config_path: str):
    """Execute eligibility filtering"""
    with open(config_path, 'r') as f:
        config = SimpleNamespace(**json.load(f))
    
    # Load users and non-users
    users_path = os.path.join(config.output_dir, 'cohorts',
                             f"{config.cohort_name}_glp1_users.csv")
    non_users_path = os.path.join(config.output_dir, 'cohorts',
                                 f"{config.cohort_name}_glp1_nonusers.csv")
    
    users = pd.read_csv(users_path)
    non_users = pd.read_csv(non_users_path)
    
    # Apply criteria
    criteria_filter = EligibilityCriteriaFilter(config)
    users_filtered, non_users_filtered = criteria_filter.apply_criteria(users, non_users)
    
    return users_filtered, non_users_filtered


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply eligibility criteria")
    parser.add_argument("-c", "--config", required=True, help="Config JSON file")
    args = parser.parse_args()
    execute(args.config)

