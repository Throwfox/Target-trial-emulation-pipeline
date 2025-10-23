"""
Mean Standardized Difference (MSD) Analysis

Calculate covariate balance before and after propensity score matching.

Author: Yao An Lee
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


def calculate_smd(treated_df, control_df, covariates):
    """
    Calculate standardized mean difference for continuous and binary variables
    
    Args:
        treated_df: Treated group DataFrame
        control_df: Control group DataFrame
        covariates: List of covariate column names
    
    Returns:
        DataFrame with SMD for each covariate
    """
    smd_results = []
    
    for covariate in covariates:
        if covariate not in treated_df.columns or covariate not in control_df.columns:
            continue
        
        treated_vals = treated_df[covariate].dropna()
        control_vals = control_df[covariate].dropna()
        
        if len(treated_vals) == 0 or len(control_vals) == 0:
            continue
        
        # Calculate means
        mean_treated = treated_vals.mean()
        mean_control = control_vals.mean()
        
        # Calculate pooled standard deviation
        var_treated = treated_vals.var()
        var_control = control_vals.var()
        pooled_std = np.sqrt((var_treated + var_control) / 2)
        
        # Calculate SMD
        if pooled_std > 0:
            smd = (mean_treated - mean_control) / pooled_std
        else:
            smd = 0
        
        smd_results.append({
            'covariate': covariate,
            'treated_mean': mean_treated,
            'control_mean': control_vals,
            'SMD': abs(smd)
        })
    
    return pd.DataFrame(smd_results)


def plot_love_plot(smd_before, smd_after, output_path):
    """
    Create Love plot showing covariate balance before and after matching
    
    Args:
        smd_before: SMD DataFrame before matching
        smd_after: SMD DataFrame after matching
        output_path: Path to save plot
    """
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Plot SMD before and after
    y_pos = np.arange(len(smd_before))
    ax.scatter(smd_before['SMD'], y_pos, label='Before Matching', alpha=0.6)
    ax.scatter(smd_after['SMD'], y_pos, label='After Matching', alpha=0.6)
    
    # Add threshold lines
    ax.axvline(x=0.1, color='red', linestyle='--', label='SMD = 0.1')
    ax.axvline(x=-0.1, color='red', linestyle='--')
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(smd_before['covariate'])
    ax.set_xlabel('Standardized Mean Difference')
    ax.set_title('Covariate Balance: Before vs After Matching')
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Love plot saved to: {output_path}")

