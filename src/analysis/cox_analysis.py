"""
Cox Proportional Hazards Regression Analysis

Evaluate time-to-event outcomes for matched cohorts.

"""

import pandas as pd
import numpy as np
from lifelines import CoxPHFitter, KaplanMeierFitter
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')


class CoxAnalyzer:
    """Cox regression analysis for time-to-event outcomes"""
    
    def __init__(self, config):
        self.config = config
    
    def analyze(self, matched_users, matched_non_users, outcomes):
        """
        Perform Cox regression analysis
        
        Args:
            matched_users: Matched GLP-1 users DataFrame
            matched_non_users: Matched non-users DataFrame
            outcomes: List of outcome definitions
        
        Returns:
            DataFrame with analysis results
        """
        # Combine cohorts
        matched_users['treatment'] = 1
        matched_non_users['treatment'] = 0
        combined = pd.concat([matched_users, matched_non_users])
        
        results = []
        
        for outcome in outcomes:
            outcome_name = outcome['name']
            print(f"\nAnalyzing outcome: {outcome_name}")
            
            # Prepare survival data
            survival_data = self._prepare_survival_data(
                combined, outcome
            )
            
            # Cox regression
            cph = CoxPHFitter()
            cph.fit(survival_data, duration_col='T', event_col='E', formula='treatment')
            
            hr = cph.hazard_ratios_['treatment']
            ci = cph.confidence_intervals_.loc['treatment']
            p_value = cph.summary.loc['treatment', 'p']
            
            results.append({
                'outcome': outcome_name,
                'HR': hr,
                'HR_lower_95CI': ci.iloc[0],
                'HR_upper_95CI': ci.iloc[1],
                'p_value': p_value
            })
            
            print(f"  HR: {hr:.3f} (95% CI: {ci.iloc[0]:.3f}-{ci.iloc[1]:.3f}), p={p_value:.4f}")
        
        return pd.DataFrame(results)
    
    def _prepare_survival_data(self, df, outcome):
        """Prepare data for survival analysis"""
        # Simplified: calculate follow-up time and event occurrence
        df = df.copy()
        df['T'] = np.random.uniform(30, 1000, len(df))  # Placeholder
        df['E'] = np.random.binomial(1, 0.1, len(df))  # Placeholder
        return df[['treatment', 'T', 'E']]

