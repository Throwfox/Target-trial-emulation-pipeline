"""
Step 5: Propensity Score Matching

Perform 1:1 propensity score matching between GLP-1 users and non-users.

Method:
- Logistic regression to estimate propensity scores
- Nearest neighbor matching with caliper
- Standardized mean difference (SMD) calculation for balance assessment

Author: Yao An Lee
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from types import SimpleNamespace
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from scipy.special import logit
from sklearn.neighbors import NearestNeighbors
import logging
import argparse

sys.path.append(str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PropensityMatcher:
    """Propensity score matching"""
    
    def __init__(self, config: SimpleNamespace):
        self.config = config
    
    def match(self, users_df: pd.DataFrame, non_users_df: pd.DataFrame):
        """
        Perform propensity score matching
        
        Returns:
            Tuple of (matched_users, matched_non_users, match_pairs)
        """
        logger.info("=" * 80)
        logger.info("STEP 5: PROPENSITY SCORE MATCHING")
        logger.info("=" * 80)
        
        # Prepare data
        logger.info("\nPreparing data for matching...")
        users_prepared, non_users_prepared, feature_cols = self._prepare_data(users_df, non_users_df)
        
        # Calculate propensity scores
        logger.info("\nCalculating propensity scores...")
        users_with_ps, non_users_with_ps = self._calculate_propensity_scores(
            users_prepared, non_users_prepared, feature_cols
        )
        
        # Perform matching
        logger.info("\nPerforming 1:1 nearest neighbor matching...")
        matched_pairs = self._perform_matching(users_with_ps, non_users_with_ps)
        
        # Get matched cohorts
        matched_users = users_with_ps[users_with_ps['person_id'].isin(matched_pairs['user_person_id'])]
        matched_non_users = non_users_with_ps[non_users_with_ps['person_id'].isin(matched_pairs['nonuser_person_id'])]
        
        # Calculate balance
        logger.info("\nCalculating covariate balance...")
        balance_stats = self._calculate_balance(matched_users, matched_non_users, feature_cols)
        
        # Save results
        self._save_results(matched_users, matched_non_users, matched_pairs, balance_stats)
        
        logger.info(f"\nMatching complete:")
        logger.info(f"  Matched users: {len(matched_users)}")
        logger.info(f"  Matched non-users: {len(matched_non_users)}")
        logger.info(f"  Match rate: {len(matched_users)/len(users_df)*100:.1f}%")
        
        return matched_users, matched_non_users, matched_pairs
    
    def _prepare_data(self, users_df, non_users_df):
        """Prepare data for PS calculation"""
        # Get matching features from config
        feature_cols = self.config.matching_features
        
        # Add treatment indicator
        users_df = users_df.copy()
        non_users_df = non_users_df.copy()
        users_df['treatment'] = 1
        non_users_df['treatment'] = 0
        
        # Combine
        combined = pd.concat([users_df, non_users_df], ignore_index=True)
        
        # Select features
        available_features = [f for f in feature_cols if f in combined.columns]
        logger.info(f"Using {len(available_features)} features for matching")
        
        # Handle missing values
        imputer = SimpleImputer(strategy='median')
        combined[available_features] = imputer.fit_transform(combined[available_features])
        
        # Split back
        users_prepared = combined[combined['treatment'] == 1].copy()
        non_users_prepared = combined[combined['treatment'] == 0].copy()
        
        return users_prepared, non_users_prepared, available_features
    
    def _calculate_propensity_scores(self, users_df, non_users_df, feature_cols):
        """Calculate propensity scores using logistic regression"""
        # Combine data
        combined = pd.concat([users_df, non_users_df], ignore_index=True)
        
        X = combined[feature_cols].values
        y = combined['treatment'].values
        
        # Standardize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Fit logistic regression
        lr = LogisticRegression(max_iter=1000, random_state=42)
        lr.fit(X_scaled, y)
        
        # Calculate propensity scores
        ps = lr.predict_proba(X_scaled)[:, 1]
        combined['propensity_score'] = ps
        combined['logit_ps'] = logit(np.clip(ps, 0.001, 0.999))
        
        # Split back
        users_with_ps = combined[combined['treatment'] == 1].copy()
        non_users_with_ps = combined[combined['treatment'] == 0].copy()
        
        logger.info(f"Propensity score range (users): {users_with_ps['propensity_score'].min():.3f} to {users_with_ps['propensity_score'].max():.3f}")
        logger.info(f"Propensity score range (non-users): {non_users_with_ps['propensity_score'].min():.3f} to {non_users_with_ps['propensity_score'].max():.3f}")
        
        return users_with_ps, non_users_with_ps
    
    def _perform_matching(self, users_df, non_users_df):
        """Perform 1:1 nearest neighbor matching with caliper"""
        # Use logit propensity score for matching
        user_ps = users_df[['person_id', 'logit_ps']].values
        non_user_ps = non_users_df[['person_id', 'logit_ps']].values
        
        # Calculate caliper (0.2 * SD of logit PS)
        all_logit_ps = np.concatenate([user_ps[:, 1], non_user_ps[:, 1]])
        caliper = 0.2 * np.std(all_logit_ps)
        logger.info(f"Using caliper: {caliper:.4f}")
        
        # Nearest neighbor matching
        nbrs = NearestNeighbors(n_neighbors=1, metric='euclidean')
        nbrs.fit(non_user_ps[:, 1].reshape(-1, 1))
        
        matched_pairs = []
        used_non_users = set()
        
        for user_id, user_logit_ps in user_ps:
            distances, indices = nbrs.kneighbors([[user_logit_ps]])
            distance = distances[0][0]
            
            # Check caliper
            if distance <= caliper:
                non_user_idx = indices[0][0]
                non_user_id = non_user_ps[non_user_idx, 0]
                
                # Check if non-user already matched
                if non_user_id not in used_non_users:
                    matched_pairs.append({
                        'user_person_id': user_id,
                        'nonuser_person_id': non_user_id,
                        'ps_distance': distance
                    })
                    used_non_users.add(non_user_id)
        
        matched_pairs_df = pd.DataFrame(matched_pairs)
        logger.info(f"Successfully matched {len(matched_pairs)} pairs")
        
        return matched_pairs_df
    
    def _calculate_balance(self, matched_users, matched_non_users, feature_cols):
        """Calculate standardized mean difference for covariate balance"""
        balance_stats = []
        
        for feature in feature_cols:
            if feature in matched_users.columns and feature in matched_non_users.columns:
                user_mean = matched_users[feature].mean()
                non_user_mean = matched_non_users[feature].mean()
                
                pooled_std = np.sqrt(
                    (matched_users[feature].var() + matched_non_users[feature].var()) / 2
                )
                
                if pooled_std > 0:
                    smd = (user_mean - non_user_mean) / pooled_std
                else:
                    smd = 0
                
                balance_stats.append({
                    'feature': feature,
                    'user_mean': user_mean,
                    'non_user_mean': non_user_mean,
                    'smd': abs(smd)
                })
        
        balance_df = pd.DataFrame(balance_stats)
        balance_df = balance_df.sort_values('smd', ascending=False)
        
        # Log features with SMD > 0.1
        imbalanced = balance_df[balance_df['smd'] > 0.1]
        if len(imbalanced) > 0:
            logger.warning(f"  {len(imbalanced)} features with SMD > 0.1:")
            for _, row in imbalanced.head(5).iterrows():
                logger.warning(f"    {row['feature']}: SMD = {row['smd']:.3f}")
        else:
            logger.info("  All features have SMD ≤ 0.1 (good balance)")
        
        return balance_df
    
    def _save_results(self, matched_users, matched_non_users, matched_pairs, balance_stats):
        """Save matching results"""
        output_dir = os.path.join(self.config.output_dir, 'matched_pairs')
        os.makedirs(output_dir, exist_ok=True)
        
        matched_users.to_csv(
            os.path.join(output_dir, f"{self.config.cohort_name}_matched_users.csv"),
            index=False
        )
        matched_non_users.to_csv(
            os.path.join(output_dir, f"{self.config.cohort_name}_matched_nonusers.csv"),
            index=False
        )
        matched_pairs.to_csv(
            os.path.join(output_dir, f"{self.config.cohort_name}_match_pairs.csv"),
            index=False
        )
        balance_stats.to_csv(
            os.path.join(output_dir, f"{self.config.cohort_name}_balance_stats.csv"),
            index=False
        )
        
        logger.info(f"\nResults saved to {output_dir}")


def execute(config_path: str):
    """Execute propensity score matching"""
    with open(config_path, 'r') as f:
        config = SimpleNamespace(**json.load(f))
    
    # Load cohorts with covariates
    users_path = os.path.join(config.output_dir, 'cohorts',
                             f"{config.cohort_name}_users_with_covariates.csv")
    non_users_path = os.path.join(config.output_dir, 'cohorts',
                                 f"{config.cohort_name}_nonusers_with_covariates.csv")
    
    users = pd.read_csv(users_path)
    non_users = pd.read_csv(non_users_path)
    
    # Perform matching
    matcher = PropensityMatcher(config)
    matched_users, matched_non_users, pairs = matcher.match(users, non_users)
    
    return matched_users, matched_non_users, pairs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Propensity score matching")
    parser.add_argument("-c", "--config", required=True, help="Config JSON file")
    args = parser.parse_args()
    execute(args.config)

