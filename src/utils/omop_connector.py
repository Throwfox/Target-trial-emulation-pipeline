"""
OMOP Connector: DuckDB-based interface for OMOP CDM data

This module provides efficient querying of large OMOP CSV exports without
loading entire tables into memory.
"""

import os
import duckdb
import pandas as pd
from typing import List, Dict, Optional, Union
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OMOPConnector:
    """
    Connector for OMOP CDM data stored in CSV format using DuckDB.
    
    This class provides methods to efficiently query OMOP tables without
    loading full datasets into memory.
    """
    
    def __init__(self, data_dir: str, temp_db: Optional[str] = None):
        """
        Initialize OMOP Connector
        
        Args:
            data_dir: Path to directory containing OMOP CSV files
            temp_db: Path to temporary DuckDB database file (optional)
        """
        self.data_dir = Path(data_dir)
        self.temp_db = temp_db if temp_db else ':memory:'
        self.conn = duckdb.connect(self.temp_db)
        
        # Standard OMOP table names
        self.omop_tables = [
            'person', 'observation_period', 'visit_occurrence',
            'condition_occurrence', 'drug_exposure', 'procedure_occurrence',
            'measurement', 'observation', 'death',
            'concept', 'concept_ancestor', 'concept_relationship',
            'vocabulary', 'drug_strength'
        ]
        
        # Map tables to file paths
        self.table_paths = {}
        self._discover_tables()
        
        logger.info(f"OMOPConnector initialized with data directory: {data_dir}")
        logger.info(f"Found {len(self.table_paths)} OMOP tables")
    
    def _discover_tables(self):
        """Discover available OMOP tables in the data directory"""
        for table in self.omop_tables:
            csv_path = self.data_dir / f"{table}.csv"
            if csv_path.exists():
                self.table_paths[table] = str(csv_path)
                logger.debug(f"Found table: {table}")
    
    def create_view(self, table_name: str, view_name: Optional[str] = None):
        """
        Create a DuckDB view from an OMOP CSV file
        
        Args:
            table_name: Name of OMOP table
            view_name: Optional custom view name (defaults to table_name)
        """
        if table_name not in self.table_paths:
            raise ValueError(f"Table {table_name} not found in data directory")
        
        view_name = view_name or table_name
        csv_path = self.table_paths[table_name]
        
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_csv_auto('{csv_path}', header=true)
        """)
        
        logger.debug(f"Created view: {view_name}")
    
    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame
        
        Args:
            sql: SQL query string
            
        Returns:
            Query results as pandas DataFrame
        """
        return self.conn.execute(sql).df()
    
    def get_persons(self, person_ids: Optional[List[int]] = None,
                   min_birth_year: Optional[int] = None,
                   max_birth_year: Optional[int] = None) -> pd.DataFrame:
        """
        Get person records with optional filtering
        
        Args:
            person_ids: List of specific person_ids to retrieve
            min_birth_year: Minimum birth year
            max_birth_year: Maximum birth year
            
        Returns:
            DataFrame of person records
        """
        self.create_view('person')
        
        conditions = []
        if person_ids:
            ids_str = ','.join(map(str, person_ids))
            conditions.append(f"person_id IN ({ids_str})")
        if min_birth_year:
            conditions.append(f"year_of_birth >= {min_birth_year}")
        if max_birth_year:
            conditions.append(f"year_of_birth <= {max_birth_year}")
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        sql = f"SELECT * FROM person {where_clause}"
        return self.query(sql)
    
    def get_observations(self, person_ids: Optional[List[int]] = None,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get observation periods for persons
        
        Args:
            person_ids: List of person_ids
            start_date: Minimum observation start date (YYYY-MM-DD)
            end_date: Maximum observation end date (YYYY-MM-DD)
            
        Returns:
            DataFrame of observation periods
        """
        self.create_view('observation_period')
        
        conditions = []
        if person_ids:
            ids_str = ','.join(map(str, person_ids))
            conditions.append(f"person_id IN ({ids_str})")
        if start_date:
            conditions.append(f"observation_period_start_date >= '{start_date}'")
        if end_date:
            conditions.append(f"observation_period_end_date <= '{end_date}'")
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        sql = f"SELECT * FROM observation_period {where_clause}"
        return self.query(sql)
    
    def get_conditions(self, person_ids: Optional[List[int]] = None,
                      concept_ids: Optional[List[int]] = None,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get condition occurrences
        
        Args:
            person_ids: List of person_ids
            concept_ids: List of condition concept_ids
            start_date: Minimum condition start date
            end_date: Maximum condition end date
            
        Returns:
            DataFrame of condition occurrences
        """
        self.create_view('condition_occurrence')
        
        conditions = []
        if person_ids:
            ids_str = ','.join(map(str, person_ids))
            conditions.append(f"person_id IN ({ids_str})")
        if concept_ids:
            concepts_str = ','.join(map(str, concept_ids))
            conditions.append(f"condition_concept_id IN ({concepts_str})")
        if start_date:
            conditions.append(f"condition_start_date >= '{start_date}'")
        if end_date:
            conditions.append(f"condition_start_date <= '{end_date}'")
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        sql = f"SELECT * FROM condition_occurrence {where_clause}"
        return self.query(sql)
    
    def get_drug_exposures(self, person_ids: Optional[List[int]] = None,
                          concept_ids: Optional[List[int]] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get drug exposures
        
        Args:
            person_ids: List of person_ids
            concept_ids: List of drug concept_ids
            start_date: Minimum drug exposure start date
            end_date: Maximum drug exposure end date
            
        Returns:
            DataFrame of drug exposures
        """
        self.create_view('drug_exposure')
        
        conditions = []
        if person_ids:
            ids_str = ','.join(map(str, person_ids))
            conditions.append(f"person_id IN ({ids_str})")
        if concept_ids:
            concepts_str = ','.join(map(str, concept_ids))
            conditions.append(f"drug_concept_id IN ({concepts_str})")
        if start_date:
            conditions.append(f"drug_exposure_start_date >= '{start_date}'")
        if end_date:
            conditions.append(f"drug_exposure_start_date <= '{end_date}'")
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        sql = f"SELECT * FROM drug_exposure {where_clause}"
        return self.query(sql)
    
    def get_measurements(self, person_ids: Optional[List[int]] = None,
                        concept_ids: Optional[List[int]] = None,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get measurements (labs)
        
        Args:
            person_ids: List of person_ids
            concept_ids: List of measurement concept_ids
            start_date: Minimum measurement date
            end_date: Maximum measurement date
            
        Returns:
            DataFrame of measurements
        """
        self.create_view('measurement')
        
        conditions = []
        if person_ids:
            ids_str = ','.join(map(str, person_ids))
            conditions.append(f"person_id IN ({ids_str})")
        if concept_ids:
            concepts_str = ','.join(map(str, concept_ids))
            conditions.append(f"measurement_concept_id IN ({concepts_str})")
        if start_date:
            conditions.append(f"measurement_date >= '{start_date}'")
        if end_date:
            conditions.append(f"measurement_date <= '{end_date}'")
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        sql = f"SELECT * FROM measurement {where_clause}"
        return self.query(sql)
    
    def get_procedures(self, person_ids: Optional[List[int]] = None,
                      concept_ids: Optional[List[int]] = None,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get procedure occurrences
        
        Args:
            person_ids: List of person_ids
            concept_ids: List of procedure concept_ids
            start_date: Minimum procedure date
            end_date: Maximum procedure date
            
        Returns:
            DataFrame of procedure occurrences
        """
        self.create_view('procedure_occurrence')
        
        conditions = []
        if person_ids:
            ids_str = ','.join(map(str, person_ids))
            conditions.append(f"person_id IN ({ids_str})")
        if concept_ids:
            concepts_str = ','.join(map(str, concept_ids))
            conditions.append(f"procedure_concept_id IN ({concepts_str})")
        if start_date:
            conditions.append(f"procedure_date >= '{start_date}'")
        if end_date:
            conditions.append(f"procedure_date <= '{end_date}'")
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        sql = f"SELECT * FROM procedure_occurrence {where_clause}"
        return self.query(sql)
    
    def get_visits(self, person_ids: Optional[List[int]] = None,
                   visit_concept_ids: Optional[List[int]] = None,
                   start_date: Optional[str] = None,
                   end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get visit occurrences
        
        Args:
            person_ids: List of person_ids
            visit_concept_ids: List of visit type concept_ids
            start_date: Minimum visit start date
            end_date: Maximum visit end date
            
        Returns:
            DataFrame of visit occurrences
        """
        self.create_view('visit_occurrence')
        
        conditions = []
        if person_ids:
            ids_str = ','.join(map(str, person_ids))
            conditions.append(f"person_id IN ({ids_str})")
        if visit_concept_ids:
            concepts_str = ','.join(map(str, visit_concept_ids))
            conditions.append(f"visit_concept_id IN ({concepts_str})")
        if start_date:
            conditions.append(f"visit_start_date >= '{start_date}'")
        if end_date:
            conditions.append(f"visit_end_date <= '{end_date}'")
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        sql = f"SELECT * FROM visit_occurrence {where_clause}"
        return self.query(sql)
    
    def expand_concepts(self, concept_ids: List[int], 
                       include_descendants: bool = True) -> List[int]:
        """
        Expand concept IDs to include descendants using concept_ancestor
        
        Args:
            concept_ids: List of ancestor concept_ids
            include_descendants: Whether to include descendant concepts
            
        Returns:
            List of expanded concept_ids
        """
        if not include_descendants:
            return concept_ids
        
        self.create_view('concept_ancestor')
        
        ids_str = ','.join(map(str, concept_ids))
        sql = f"""
            SELECT DISTINCT descendant_concept_id
            FROM concept_ancestor
            WHERE ancestor_concept_id IN ({ids_str})
        """
        
        result = self.query(sql)
        expanded_ids = result['descendant_concept_id'].tolist()
        
        logger.info(f"Expanded {len(concept_ids)} concepts to {len(expanded_ids)} concepts")
        return expanded_ids
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

