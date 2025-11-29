"""
Step 1: Cohort Extraction

This module extracts the AOM-eligible (Anti-Obesity Medication) cohort from OMOP data.

Inclusion Criteria:
1. Adults ≥ 18 years on index date
2. Patients with obesity before or on the index date
   - Obesity defined as: recorded diagnosis of obesity, BMI ≥ 30 kg/m², 
     or BMI 27-29.9 kg/m² with at least one weight-related comorbidity
3. Patients had at least one encounter before the index date
4. Within study period (configurable)

Author: Yao An Lee
Organization: University of Florida Department of Pharmaceutical Outcomes & Policy
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
from typing import List, Dict, Optional
import logging

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))
from utils.omop_connector import OMOPConnector
from utils.helpers import filter_date_range, calculate_age, summarize_cohort

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CohortExtractor:
    """Extract obesity cohort from OMOP data"""
    
    def __init__(self, config: SimpleNamespace):
        """
        Initialize CohortExtractor
        
        Args:
            config: Configuration object with study parameters
        """
        self.config = config
        self.omop = OMOPConnector(config.omop_data_dir)
        
        # Load concept sets
        self.obesity_dx_concepts = self._load_concepts(config.obesity_diagnosis_concepts)
        self.obesity_measurement_concepts = self._load_concepts(config.obesity_measurement_concepts)
        
        # Optional: specific condition concepts (e.g., T1D, IBD)
        if hasattr(config, 'specific_condition_concepts'):
            self.specific_condition_concepts = self._load_concepts(config.specific_condition_concepts)
        else:
            self.specific_condition_concepts = []
        
        logger.info("CohortExtractor initialized")
    
    def _load_concepts(self, concept_config: Dict) -> List[int]:
        """Load and expand concept IDs"""
        # Handle case where config is just a list of IDs (or empty list)
        if isinstance(concept_config, list):
            return concept_config

        if 'concept_ids' in concept_config:
            concept_ids = concept_config['concept_ids']
        else:
            concept_ids = []
        
        # Expand to include descendants if requested
        if concept_config.get('include_descendants', True):
            concept_ids = self.omop.expand_concepts(concept_ids)
        
        return concept_ids
    
    def extract_obesity_cohort(self) -> pd.DataFrame:
        """
        Extract patients meeting obesity criteria
        
        Returns:
            DataFrame with obesity cohort
        """
        logger.info("=" * 80)
        logger.info("STEP 1: EXTRACTING OBESITY COHORT")
        logger.info("=" * 80)
        
        # 1. Get patients with obesity diagnosis
        logger.info("Extracting patients with obesity diagnosis...")
        obesity_dx = self.omop.get_conditions(
            concept_ids=self.obesity_dx_concepts,
            start_date=self.config.study_start_date,
            end_date=self.config.study_end_date
        )
        
        if len(obesity_dx) > 0:
            obesity_dx = obesity_dx.rename(columns={
                'condition_start_date': 'obesity_date',
                'condition_occurrence_id': 'record_id'
            })
            obesity_dx['obesity_criterion'] = 'diagnosis'
            logger.info(f"Found {len(obesity_dx)} obesity diagnosis records for {obesity_dx['person_id'].nunique()} patients")
        else:
            logger.warning("No obesity diagnosis records found")
            obesity_dx = pd.DataFrame(columns=['person_id', 'obesity_date', 'record_id', 'obesity_criterion'])
        
        # 2. Get patients with BMI ≥ 30
        logger.info("Extracting patients with BMI measurements...")
        bmi_measurements = self.omop.get_measurements(
            concept_ids=self.obesity_measurement_concepts,
            start_date=self.config.study_start_date,
            end_date=self.config.study_end_date
        )
        
        obesity_bmi = pd.DataFrame()
        if len(bmi_measurements) > 0:
            bmi_measurements['value_as_number'] = pd.to_numeric(
                bmi_measurements['value_as_number'], errors='coerce'
            )
            
            # Filter for BMI >= 30 or BMI 27-29.9 (will check comorbidity later)
            obesity_bmi = bmi_measurements[
                bmi_measurements['value_as_number'] >= self.config.bmi_threshold_low
            ].copy()
            
            obesity_bmi = obesity_bmi.rename(columns={
                'measurement_date': 'obesity_date',
                'measurement_id': 'record_id',
                'value_as_number': 'bmi_value'
            })
            
            # Classify BMI criterion
            obesity_bmi['obesity_criterion'] = obesity_bmi['bmi_value'].apply(
                lambda x: 'bmi_30plus' if x >= 30 else 'bmi_27to29'
            )
            
            logger.info(f"Found {len(obesity_bmi)} BMI records ≥27 for {obesity_bmi['person_id'].nunique()} patients")
            logger.info(f"  - BMI ≥30: {len(obesity_bmi[obesity_bmi['bmi_value'] >= 30])} records")
            logger.info(f"  - BMI 27-29.9: {len(obesity_bmi[(obesity_bmi['bmi_value'] >= 27) & (obesity_bmi['bmi_value'] < 30)])} records")
        else:
            logger.warning("No BMI measurement records found")
        
        # 3. Combine obesity cohorts
        logger.info("Combining obesity cohorts...")
        
        # Select relevant columns for combination
        obesity_dx_subset = obesity_dx[['person_id', 'obesity_date', 'record_id', 'obesity_criterion']]
        obesity_bmi_subset = obesity_bmi[['person_id', 'obesity_date', 'record_id', 'obesity_criterion', 'bmi_value']]
        
        # Combine
        obesity_cohort = pd.concat([obesity_dx_subset, obesity_bmi_subset], ignore_index=True)
        obesity_cohort['obesity_date'] = pd.to_datetime(obesity_cohort['obesity_date'])
        
        # Get earliest obesity date for each patient
        earliest_obesity = obesity_cohort.groupby('person_id').agg({
            'obesity_date': 'min',
            'obesity_criterion': 'first'
        }).reset_index()
        earliest_obesity = earliest_obesity.rename(columns={'obesity_date': 'obesity_entry_date'})
        
        logger.info(f"Total obesity cohort: {len(earliest_obesity)} unique patients")
        
        # 4. Get demographics
        logger.info("Retrieving patient demographics...")
        person_ids = earliest_obesity['person_id'].tolist()
        demographics = self.omop.get_persons(person_ids=person_ids)
        
        # 5. Merge demographics with obesity dates
        cohort = earliest_obesity.merge(demographics, on='person_id', how='left')
        
        # Calculate age at obesity entry
        cohort['birth_date'] = pd.to_datetime(
            cohort['year_of_birth'].astype(str) + '-' +
            cohort['month_of_birth'].fillna(1).astype(int).astype(str) + '-' +
            cohort['day_of_birth'].fillna(1).astype(int).astype(str)
        )
        cohort['age_at_obesity_entry'] = (
            (cohort['obesity_entry_date'] - cohort['birth_date']).dt.days / 365.25
        ).astype(int)
        
        logger.info(f"Age range: {cohort['age_at_obesity_entry'].min()} to {cohort['age_at_obesity_entry'].max()} years")
        
        # 6. Apply age filter (≥18 years)
        if self.config.min_age_at_entry:
            cohort = cohort[cohort['age_at_obesity_entry'] >= self.config.min_age_at_entry]
            logger.info(f"After age ≥{self.config.min_age_at_entry} filter: {len(cohort)} patients")
        
        # 7. Get visit/encounter information
        logger.info("Retrieving visit information...")
        visits = self.omop.get_visits(
            person_ids=cohort['person_id'].tolist(),
            start_date=self.config.study_start_date,
            end_date=self.config.study_end_date
        )
        
        # Count visits before obesity entry date
        cohort_visits = cohort[['person_id', 'obesity_entry_date']].merge(
            visits[['person_id', 'visit_occurrence_id', 'visit_start_date']],
            on='person_id',
            how='left'
        )
        cohort_visits['visit_start_date'] = pd.to_datetime(cohort_visits['visit_start_date'])
        
        # Visits before obesity entry
        visits_before = cohort_visits[
            cohort_visits['visit_start_date'] < cohort_visits['obesity_entry_date']
        ]
        
        visit_counts = visits_before.groupby('person_id').size().reset_index(name='visits_before_entry')
        cohort = cohort.merge(visit_counts, on='person_id', how='left')
        cohort['visits_before_entry'] = cohort['visits_before_entry'].fillna(0).astype(int)
        
        logger.info(f"Patients with ≥1 visit before entry: {len(cohort[cohort['visits_before_entry'] >= 1])}")
        
        # 8. Optional: filter for specific conditions
        if len(self.specific_condition_concepts) > 0:
            logger.info("Filtering for specific condition cohort...")
            specific_conditions = self.omop.get_conditions(
                person_ids=cohort['person_id'].tolist(),
                concept_ids=self.specific_condition_concepts
            )
            
            patients_with_condition = specific_conditions['person_id'].unique()
            cohort['has_specific_condition'] = cohort['person_id'].isin(patients_with_condition)
            
            if self.config.require_specific_condition:
                cohort = cohort[cohort['has_specific_condition']]
                logger.info(f"After specific condition filter: {len(cohort)} patients")
        
        # 9. Save cohort
        output_path = os.path.join(self.config.output_dir, 'cohorts', self.config.cohort_name + '_obesity_cohort.csv')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        cohort.to_csv(output_path, index=False)
        
        logger.info(f"\n{'=' * 80}")
        logger.info("COHORT EXTRACTION COMPLETE")
        logger.info(f"{'=' * 80}")
        logger.info(f"Final cohort size: {len(cohort)} patients")
        logger.info(f"Cohort saved to: {output_path}")
        logger.info(f"{'=' * 80}\n")
        
        return cohort


def execute(config_path: str):
    """
    Execute cohort extraction
    
    Args:
        config_path: Path to configuration JSON file
    """
    # Load configuration
    with open(config_path, 'r') as f:
        config_dict = json.load(f)
    config = SimpleNamespace(**config_dict)
    
    # Run extraction
    extractor = CohortExtractor(config)
    cohort = extractor.extract_obesity_cohort()
    
    # Summary statistics
    summarize_cohort(cohort, person_id_col='person_id')
    
    return cohort


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract obesity cohort from OMOP data")
    parser.add_argument("-c", "--config", type=str, required=True,
                       help="Path to configuration JSON file")
    
    args = parser.parse_args()
    
    execute(args.config)

