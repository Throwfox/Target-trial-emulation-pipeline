"""
OMOP GLP-1 Propensity Score Matching and Analysis Pipeline

This package provides a comprehensive pipeline for conducting propensity score matched
cohort studies using GLP-1 receptor agonists in OMOP CDM formatted data.

Developed by: University of Florida Department of Pharmaceutical Outcomes & Policy
Contact: yaoanlee@ufl.edu
"""

__version__ = "1.0.0"
__author__ = "Yao An Lee"
__license__ = "MIT"

from . import utils
from . import preprocessing
from . import analysis

__all__ = ['utils', 'preprocessing', 'analysis']

