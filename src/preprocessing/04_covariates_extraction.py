"""
Step 4: Covariates Extraction

Extract baseline covariates for propensity score matching:
- Demographics (age, sex, race, ethnicity, smoking, insurance)
- Comorbidities (diagnoses and procedures)
- Medications
- Laboratory values

"""

import os
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from types import SimpleNamespace
import logging
import argparse

sys.path.append(str(Path(__file__).parent.parent))
from utils.omop_connector import OMOPConnector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CovariatesExtractor:
    """Extract baseline covariates for PS matching"""
    
    def __init__(self, config: SimpleNamespace):
        self.config = config
        self.omop = OMOPConnector(config.omop_data_dir)
        self._load_covariate_concepts()
    
    def _load_covariate_concepts(self):
        """Load concept sets for comorbidities, medications, and labs"""
        # Load comorbidities from CSV
        comorbidity_file = os.path.join(self.config.concept_sets_dir, 'comorbidities.csv')
        self.comorbidities = pd.read_csv(comorbidity_file)
        
        # Load medications from CSV
        medication_file = os.path.join(self.config.concept_sets_dir, 'medications.csv')
        self.medications = pd.read_csv(medication_file)
        
        # Load lab concepts from JSON
        lab_file = os.path.join(self.config.concept_sets_dir, 'labs.json')
        with open(lab_file, 'r') as f:
            self.lab_concepts = json.load(f)
        
        logger.info(f"Loaded {len(self.comorbidities)} comorbidities")
        logger.info(f"Loaded {len(self.medications)} medications")
        logger.info(f"Loaded {len(self.lab_concepts)} lab tests")
    
    def extract_covariates(self, users_df: pd.DataFrame, non_users_df: pd.DataFrame):
        """
        Extract baseline covariates for both users and non-users
        
        Returns:
            Tuple of (users_with_covariates, non_users_with_covariates)
        """
        logger.info("=" * 80)
        logger.info("STEP 4: EXTRACTING BASELINE COVARIATES")
        logger.info("=" * 80)
        
        # Extract for users
        logger.info("\nExtracting covariates for GLP-1 users...")
        users_covariates = self._extract_for_cohort(users_df, is_user=True)
        
        # Extract for non-users
        logger.info("\nExtracting covariates for non-users...")
        non_users_covariates = self._extract_for_cohort(non_users_df, is_user=False)
        
        # Save results
        users_path = os.path.join(self.config.output_dir, 'cohorts',
                                 f"{self.config.cohort_name}_users_with_covariates.csv")
        non_users_path = os.path.join(self.config.output_dir, 'cohorts',
                                     f"{self.config.cohort_name}_nonusers_with_covariates.csv")
        
        users_covariates.to_csv(users_path, index=False)
        non_users_covariates.to_csv(non_users_path, index=False)
        
        logger.info(f"\nCovariates extraction complete")
        logger.info(f"Users with covariates: {len(users_covariates)}")
        logger.info(f"Non-users with covariates: {len(non_users_covariates)}")
        
        return users_covariates, non_users_covariates
    
    def _extract_for_cohort(self, df: pd.DataFrame, is_user: bool) -> pd.DataFrame:
        """Extract covariates for a cohort"""
        result = df.copy()
        person_ids = df['person_id'].tolist()
        
        # Determine index date
        if is_user:
            result['index_date'] = pd.to_datetime(result['glp1_index_date'])
        else:
            # For non-users, use obesity entry date as proxy (will be refined in matching)
            result['index_date'] = pd.to_datetime(result['obesity_entry_date'])
        
        # Extract demographics (already in df, just format)
        result = self._format_demographics(result)
        
        # Extract comorbidities
        comorbidity_features = self._extract_comorbidities(person_ids, result)
        result = result.merge(comorbidity_features, on='person_id', how='left')
        
        # Extract medications
        medication_features = self._extract_medications(person_ids, result)
        result = result.merge(medication_features, on='person_id', how='left')
        
        # Extract labs
        lab_features = self._extract_labs(person_ids, result)
        result = result.merge(lab_features, on='person_id', how='left')
        
        return result
    
    def _format_demographics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format demographic variables"""
        df = df.copy()
        
        # Sex
        if 'gender_concept_id' in df.columns:
            df['sex'] = df['gender_concept_id'].map({8507: 'M', 8532: 'F'})
        
        # Race/ethnicity (simplified)
        if 'race_concept_id' in df.columns:
            df['race'] = 'Other'  # Simplified
        
        return df
    
    def _extract_comorbidities(self, person_ids: list, cohort_df: pd.DataFrame) -> pd.DataFrame:
        """Extract comorbidity features"""
        logger.info("  Extracting comorbidities...")
        
        features = pd.DataFrame({'person_id': person_ids})
        
        # For each comorbidity, check if present in baseline
        for _, row in self.comorbidities.iterrows():
            comorbidity_name = row['Name']
            concept_ids = [int(x) for x in str(row['concept_ids']).split(',') if x.strip()]
            
            if len(concept_ids) == 0:
                continue
            
            # Get conditions
            conditions = self.omop.get_conditions(
                person_ids=person_ids,
                concept_ids=concept_ids
            )
            
            if len(conditions) > 0:
                # Check if condition occurred before index date
                conditions_with_index = conditions.merge(
                    cohort_df[['person_id', 'index_date']],
                    on='person_id'
                )
                conditions_with_index['condition_start_date'] = pd.to_datetime(
                    conditions_with_index['condition_start_date']
                )
                conditions_with_index['index_date'] = pd.to_datetime(
                    conditions_with_index['index_date']
                )
                
                baseline_conditions = conditions_with_index[
                    conditions_with_index['condition_start_date'] <= conditions_with_index['index_date']
                ]
                
                persons_with_condition = baseline_conditions['person_id'].unique()
                features[comorbidity_name] = features['person_id'].isin(persons_with_condition).astype(int)
            else:
                features[comorbidity_name] = 0
        
        logger.info(f"  Extracted {len(self.comorbidities)} comorbidity features")
        return features
    
    def _extract_medications(self, person_ids: list, cohort_df: pd.DataFrame) -> pd.DataFrame:
        """Extract medication features"""
        logger.info("  Extracting medications...")
        
        features = pd.DataFrame({'person_id': person_ids})
        
        # Similar logic as comorbidities
        for _, row in self.medications.iterrows():
            med_name = row['Name']
            concept_ids = [int(x) for x in str(row['concept_ids']).split(',') if x.strip()]
            
            if len(concept_ids) > 0:
                exposures = self.omop.get_drug_exposures(
                    person_ids=person_ids,
                    concept_ids=concept_ids
                )
                
                if len(exposures) > 0:
                    exposures_with_index = exposures.merge(
                        cohort_df[['person_id', 'index_date']],
                        on='person_id'
                    )
                    baseline_exposures = exposures_with_index[
                        pd.to_datetime(exposures_with_index['drug_exposure_start_date']) <=
                        pd.to_datetime(exposures_with_index['index_date'])
                    ]
                    persons_with_med = baseline_exposures['person_id'].unique()
                    features[med_name] = features['person_id'].isin(persons_with_med).astype(int)
                else:
                    features[med_name] = 0
            else:
                features[med_name] = 0
        
        logger.info(f"  Extracted {len(self.medications)} medication features")
        return features
    
    def _extract_labs(self, person_ids: list, cohort_df: pd.DataFrame) -> pd.DataFrame:
        """Extract lab features"""
        logger.info("  Extracting labs...")
        
        features = pd.DataFrame({'person_id': person_ids})
        
        for lab_name, concept_ids in self.lab_concepts.items():
            measurements = self.omop.get_measurements(
                person_ids=person_ids,
                concept_ids=concept_ids
            )
            
            if len(measurements) > 0:
                measurements_with_index = measurements.merge(
                    cohort_df[['person_id', 'index_date']],
                    on='person_id'
                )
                baseline_measurements = measurements_with_index[
                    pd.to_datetime(measurements_with_index['measurement_date']) <=
                    pd.to_datetime(measurements_with_index['index_date'])
                ]
                
                # Get most recent value before index
                baseline_measurements = baseline_measurements.sort_values('measurement_date')
                latest_measurements = baseline_measurements.groupby('person_id').last()
                features = features.merge(
                    latest_measurements[['value_as_number']].rename(columns={'value_as_number': lab_name}),
                    on='person_id',
                    how='left'
                )
            else:
                features[lab_name] = np.nan
        
        logger.info(f"  Extracted {len(self.lab_concepts)} lab features")
        return features


def execute(config_path: str):
    """Execute covariates extraction"""
    with open(config_path, 'r') as f:
        config = SimpleNamespace(**json.load(f))
    
    # Load eligible cohorts
    users_path = os.path.join(config.output_dir, 'cohorts',
                             f"{config.cohort_name}_users_eligible.csv")
    non_users_path = os.path.join(config.output_dir, 'cohorts',
                                 f"{config.cohort_name}_nonusers_eligible.csv")
    
    users = pd.read_csv(users_path)
    non_users = pd.read_csv(non_users_path)
    
    # Extract covariates
    extractor = CovariatesExtractor(config)
    users_cov, non_users_cov = extractor.extract_covariates(users, non_users)
    
    return users_cov, non_users_cov


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract baseline covariates")
    parser.add_argument("-c", "--config", required=True, help="Config JSON file")
    args = parser.parse_args()
    execute(args.config)

