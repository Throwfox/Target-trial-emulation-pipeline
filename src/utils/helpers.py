"""
Helper functions for data processing and analysis
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Tuple
import logging

logger = logging.getLogger(__name__)


def date_transfer(date_string: str, date_format: str = "%Y-%m-%d") -> datetime:
    """
    Convert date string to datetime object
    
    Args:
        date_string: Date string
        date_format: Date format string
        
    Returns:
        datetime object
    """
    return datetime.strptime(date_string, date_format)


def last_not_nan(series: pd.Series):
    """
    Get last non-NaN value from a Series
    
    Args:
        series: pandas Series
        
    Returns:
        Last non-NaN value or NaN if all values are NaN
    """
    return series.dropna().iloc[-1] if not series.dropna().empty else np.nan


def calculate_age(birth_date: Union[datetime, str],
                 reference_date: Union[datetime, str]) -> int:
    """
    Calculate age in years
    
    Args:
        birth_date: Birth date
        reference_date: Reference date for age calculation
        
    Returns:
        Age in years
    """
    if isinstance(birth_date, str):
        birth_date = pd.to_datetime(birth_date)
    if isinstance(reference_date, str):
        reference_date = pd.to_datetime(reference_date)
    
    age = (reference_date - birth_date).days / 365.25
    return int(age)


def calculate_followup_time(start_date: Union[datetime, str],
                           end_date: Union[datetime, str],
                           unit: str = 'days') -> float:
    """
    Calculate follow-up time between two dates
    
    Args:
        start_date: Start date
        end_date: End date
        unit: Time unit ('days', 'months', 'years')
        
    Returns:
        Follow-up time in specified unit
    """
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    
    days = (end_date - start_date).days
    
    if unit == 'days':
        return days
    elif unit == 'months':
        return days / 30.44
    elif unit == 'years':
        return days / 365.25
    else:
        raise ValueError(f"Unknown unit: {unit}")


def identify_baseline_period(index_date: pd.Series,
                             lookback_months: int = 12) -> Tuple[pd.Series, pd.Series]:
    """
    Calculate baseline period start and end dates
    
    Args:
        index_date: Series of index dates
        lookback_months: Number of months for baseline period
        
    Returns:
        Tuple of (baseline_start, baseline_end) Series
    """
    index_date = pd.to_datetime(index_date)
    baseline_end = index_date
    baseline_start = index_date - pd.DateOffset(months=lookback_months)
    
    return baseline_start, baseline_end


def identify_followup_period(index_date: pd.Series,
                            last_visit_date: pd.Series,
                            max_followup_years: Optional[int] = None) -> Tuple[pd.Series, pd.Series]:
    """
    Calculate follow-up period start and end dates
    
    Args:
        index_date: Series of index dates
        last_visit_date: Series of last visit dates
        max_followup_years: Maximum follow-up years (optional censoring)
        
    Returns:
        Tuple of (followup_start, followup_end) Series
    """
    index_date = pd.to_datetime(index_date)
    last_visit_date = pd.to_datetime(last_visit_date)
    
    followup_start = index_date
    followup_end = last_visit_date
    
    if max_followup_years:
        max_end = index_date + pd.DateOffset(years=max_followup_years)
        followup_end = pd.Series([min(a, b) for a, b in zip(followup_end, max_end)])
    
    return followup_start, followup_end


def encode_smoking_status(smoking_series: pd.Series) -> pd.Series:
    """
    Encode smoking status from various formats to standardized categories
    
    Args:
        smoking_series: Series with smoking status values
        
    Returns:
        Encoded smoking status Series
    """
    # Convert to string
    smoking_series = smoking_series.astype(str)
    
    # Map to standard categories
    current_smoker = ['01', '1', '1.0', '02', '2', '2.0', '07', '7', '7.0', '08', '8', '8.0']
    former_smoker = ['03', '3', '3.0']
    never_smoker = ['04', '4', '4.0']
    
    smoking_series = smoking_series.replace(current_smoker, 'Current Smoker')
    smoking_series = smoking_series.replace(former_smoker, 'Former Smoker')
    smoking_series = smoking_series.replace(never_smoker, 'Never Smoker')
    
    # Unknown for all others
    valid_categories = ['Current Smoker', 'Former Smoker', 'Never Smoker']
    smoking_series = smoking_series.where(smoking_series.isin(valid_categories), 'Unknown')
    
    return smoking_series


def encode_sex(sex_series: pd.Series) -> pd.Series:
    """
    Encode sex to standard categories (F/M/Unknown)
    
    Args:
        sex_series: Series with sex values
        
    Returns:
        Encoded sex Series
    """
    sex_series = sex_series.astype(str)
    sex_series = sex_series.where(sex_series.isin(['F', 'M']), np.nan)
    return sex_series


def calculate_bmi(weight_kg: float, height_cm: float) -> float:
    """
    Calculate BMI from weight and height
    
    Args:
        weight_kg: Weight in kilograms
        height_cm: Height in centimeters
        
    Returns:
        BMI value
    """
    if pd.isna(weight_kg) or pd.isna(height_cm) or height_cm == 0:
        return np.nan
    
    height_m = height_cm / 100
    bmi = weight_kg / (height_m ** 2)
    return bmi


def filter_date_range(df: pd.DataFrame,
                     date_column: str,
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Filter DataFrame by date range
    
    Args:
        df: Input DataFrame
        date_column: Name of date column
        start_date: Start date (YYYY-MM-DD) or None
        end_date: End date (YYYY-MM-DD) or None
        
    Returns:
        Filtered DataFrame
    """
    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column])
    
    if start_date:
        df = df[df[date_column] >= start_date]
    if end_date:
        df = df[df[date_column] <= end_date]
    
    return df


def aggregate_by_person(df: pd.DataFrame,
                       person_id_col: str = 'person_id',
                       agg_functions: Optional[Dict] = None) -> pd.DataFrame:
    """
    Aggregate DataFrame by person_id
    
    Args:
        df: Input DataFrame
        person_id_col: Name of person ID column
        agg_functions: Dictionary of aggregation functions
        
    Returns:
        Aggregated DataFrame
    """
    if agg_functions is None:
        # Default: take first value for each column
        agg_functions = {col: 'first' for col in df.columns if col != person_id_col}
    
    return df.groupby(person_id_col).agg(agg_functions).reset_index()


def create_binary_flags(df: pd.DataFrame,
                       condition_columns: List[str],
                       flag_name: str = 'any_condition') -> pd.DataFrame:
    """
    Create binary flag indicating presence of any condition
    
    Args:
        df: Input DataFrame
        condition_columns: List of condition column names
        flag_name: Name for the new flag column
        
    Returns:
        DataFrame with new flag column
    """
    df = df.copy()
    df[flag_name] = df[condition_columns].max(axis=1)
    return df


def save_with_date_suffix(df: pd.DataFrame,
                         base_path: str,
                         suffix: Optional[str] = None):
    """
    Save DataFrame with date suffix
    
    Args:
        df: DataFrame to save
        base_path: Base file path (without extension)
        suffix: Optional custom suffix
    """
    if suffix:
        output_path = f"{base_path}_{suffix}.csv"
    else:
        date_str = datetime.now().strftime("%Y%m%d")
        output_path = f"{base_path}_{date_str}.csv"
    
    df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df)} records to {output_path}")


def load_concept_set(concept_file: str) -> List[int]:
    """
    Load concept set from JSON file
    
    Args:
        concept_file: Path to JSON file with concept IDs
        
    Returns:
        List of concept IDs
    """
    import json
    
    with open(concept_file, 'r') as f:
        data = json.load(f)
    
    if isinstance(data, list):
        return data
    elif isinstance(data, dict) and 'concepts' in data:
        return data['concepts']
    else:
        raise ValueError(f"Unknown concept file format: {concept_file}")


def summarize_cohort(df: pd.DataFrame,
                    person_id_col: str = 'person_id',
                    print_summary: bool = True) -> Dict:
    """
    Generate summary statistics for a cohort
    
    Args:
        df: Cohort DataFrame
        person_id_col: Name of person ID column
        print_summary: Whether to print summary
        
    Returns:
        Dictionary with summary statistics
    """
    n_persons = df[person_id_col].nunique()
    n_records = len(df)
    
    summary = {
        'n_persons': n_persons,
        'n_records': n_records,
        'records_per_person': n_records / n_persons if n_persons > 0 else 0
    }
    
    if print_summary:
        logger.info(f"Cohort Summary:")
        logger.info(f"  - Unique persons: {n_persons:,}")
        logger.info(f"  - Total records: {n_records:,}")
        logger.info(f"  - Records per person: {summary['records_per_person']:.2f}")
    
    return summary

