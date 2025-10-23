# OMOP GLP-1 Propensity Score Matching and Analysis Pipeline

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A comprehensive, production-ready pipeline for conducting propensity score matched cohort studies of GLP-1 receptor agonists (GLP-1 RAs) using OMOP Common Data Model (CDM) formatted data.

**Developed by**: Yao An Lee (yaoanlee@ufl.edu)  
**Organization**: University of Florida Department of Pharmaceutical Outcomes & Policy

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Pipeline Workflow](#pipeline-workflow)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
- [Output Files](#output-files)
- [Example Studies](#example-studies)
- [Documentation](#documentation)
- [Citation](#citation)
- [License](#license)

---

## 🔍 Overview

This pipeline provides an end-to-end solution for:
1. **Cohort Extraction**: Identify obesity patients meeting inclusion/exclusion criteria
2. **User Identification**: Distinguish GLP-1 users from non-users
3. **Eligibility Filtering**: Apply study-specific eligibility criteria
4. **Covariates Extraction**: Extract demographics, comorbidities, medications, and labs
5. **Propensity Score Matching**: 1:1 matching with covariate balance assessment
6. **Outcome Analysis**: Cox regression, competing risks, subgroup analyses

The pipeline is designed to work with **large CSV exports** of OMOP CDM data using **DuckDB** for memory-efficient processing.

---

## ✨ Features

- **OMOP CDM Native**: Built specifically for OMOP Common Data Model format
- **Memory Efficient**: Uses DuckDB to query large CSV files without loading into memory
- **Flexible Configuration**: JSON-based configuration for easy customization
- **Comprehensive Matching**: Propensity score matching with balance assessment (SMD calculation)
- **Well Documented**: Extensive inline documentation and logging
- **Production Ready**: Modular design, error handling, and extensive testing
- **GitHub Ready**: Clean structure, documentation, and reproducibility

---

## 📊 Pipeline Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                    OMOP GLP-1 Pipeline                          │
└─────────────────────────────────────────────────────────────────┘

Step 1: Cohort Extraction
├── Extract patients with obesity (diagnosis or BMI criteria)
├── Apply age requirements (≥18 years)
├── Filter by study period
└── Optional: filter for specific conditions (e.g., T1D, IBD)

Step 2: User Identification
├── Identify GLP-1 exposures (liraglutide, semaglutide, etc.)
├── Determine index date (first GLP-1 exposure)
└── Split into users and non-users

Step 3: Eligibility Criteria
├── Baseline encounter requirement
├── Minimum follow-up time
├── Condition inclusion/exclusion
└── Other study-specific criteria

Step 4: Covariates Extraction
├── Demographics (age, sex, race, ethnicity, smoking, insurance)
├── Comorbidities (baseline diagnoses)
├── Medications (baseline exposures)
└── Laboratory values (baseline measurements)

Step 5: Propensity Score Matching
├── Calculate propensity scores (logistic regression)
├── 1:1 nearest neighbor matching with caliper
├── Assess covariate balance (SMD)
└── Export matched cohorts

Step 6: Analysis (optional)
├── Cox proportional hazards regression
├── Competing risk analysis
├── Subgroup analysis
└── Visualization (KM curves, forest plots)
```

---

## 🚀 Installation

### Prerequisites

- Python 3.11 or higher
- OMOP CDM data in CSV format

### Setup

1. Clone this repository:
```bash
git clone https://github.com/yourusername/OMOP_GLP1.git
cd OMOP_GLP1
```

2. Run setup script:
```bash
bash setup.sh
```

This will:
- Create a virtual environment (optional)
- Install Python dependencies
- Create output directories
- Set up configuration structure

### Manual Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Create directories
mkdir -p output/{cohorts,matched_pairs,analysis_results,figures} logs
```

---

## ⚡ Quick Start

1. **Configure your study**: Edit `config/examples/obesity_general.json`
   - Set `omop_data_dir` to your OMOP data location
   - Customize study parameters

2. **Run the pipeline**:
```bash
bash run_pipeline.sh config/examples/obesity_general.json
```

3. **Check results** in `output/` directory

---

## ⚙️ Configuration

Configuration files are in JSON format. See `config/examples/` for templates.

### Key Configuration Parameters

```json
{
  "study_name": "Study name",
  "cohort_name": "cohort_identifier",
  "omop_data_dir": "/path/to/omop/csv/files",
  "output_dir": "./output",
  
  "study_start_date": "2012-01-01",
  "study_end_date": "2024-01-31",
  
  "min_age_at_entry": 18,
  "require_baseline_encounter": true,
  "min_followup_days": 30,
  
  "matching_features": [
    "age_at_obesity_entry",
    "sex",
    "BMI",
    "Diabetes",
    "Hypertension",
    ...
  ]
}
```

### Concept Sets

Define concept sets for:
- **Comorbidities**: `config/concept_sets/comorbidities.csv`
- **Medications**: `config/concept_sets/medications.csv`
- **Lab tests**: `config/concept_sets/labs.json`
- **GLP-1 drugs**: `config/concept_sets/glp1/`

---

## 📖 Usage

### Running Individual Steps

Each preprocessing step can be run independently:

```bash
# Step 1: Cohort extraction
python3 src/preprocessing/01_cohort_extraction.py -c config/examples/obesity_general.json

# Step 2: User identification
python3 src/preprocessing/02_user_identification.py -c config/examples/obesity_general.json

# Step 3: Eligibility criteria
python3 src/preprocessing/03_eligibility_criteria.py -c config/examples/obesity_general.json

# Step 4: Covariates extraction
python3 src/preprocessing/04_covariates_extraction.py -c config/examples/obesity_general.json

# Step 5: Propensity matching
python3 src/preprocessing/05_propensity_matching.py -c config/examples/obesity_general.json
```

### Running Complete Pipeline

```bash
bash run_pipeline.sh config/examples/obesity_general.json
```

### Python API

```python
from src.preprocessing.cohort_extraction import CohortExtractor
from types import SimpleNamespace
import json

# Load config
with open('config/examples/obesity_general.json') as f:
    config = SimpleNamespace(**json.load(f))

# Extract cohort
extractor = CohortExtractor(config)
cohort = extractor.extract_obesity_cohort()
```

---

## 📂 Output Files

### Cohorts Directory (`output/cohorts/`)

- `{cohort_name}_obesity_cohort.csv`: Initial obesity cohort
- `{cohort_name}_glp1_users.csv`: GLP-1 users
- `{cohort_name}_glp1_nonusers.csv`: Non-users
- `{cohort_name}_users_eligible.csv`: Users after eligibility filtering
- `{cohort_name}_nonusers_eligible.csv`: Non-users after eligibility filtering
- `{cohort_name}_users_with_covariates.csv`: Users with extracted covariates
- `{cohort_name}_nonusers_with_covariates.csv`: Non-users with extracted covariates

### Matched Pairs Directory (`output/matched_pairs/`)

- `{cohort_name}_matched_users.csv`: Matched GLP-1 users
- `{cohort_name}_matched_nonusers.csv`: Matched non-users
- `{cohort_name}_match_pairs.csv`: Matching pairs with distances
- `{cohort_name}_balance_stats.csv`: Covariate balance statistics (SMD)

### Analysis Results Directory (`output/analysis_results/`)

- Cox regression results
- Kaplan-Meier curves
- Forest plots
- Subgroup analyses

---

## 📚 Example Studies

### 1. General Obesity Cohort

```bash
bash run_pipeline.sh config/examples/obesity_general.json
```

Studies GLP-1 effects in general obesity population.

### 2. Type 1 Diabetes Cohort

```bash
bash run_pipeline.sh config/examples/t1d_cohort.json
```

Studies GLP-1 effects specifically in patients with Type 1 Diabetes and obesity.

**Outcomes of interest:**
- Diabetic ketoacidosis
- Severe hypoglycemia
- Hyperglycemic hyperosmolar state

### 3. Custom Study

Create your own configuration by copying and modifying an example:

```bash
cp config/examples/obesity_general.json config/my_study.json
# Edit my_study.json
bash run_pipeline.sh config/my_study.json
```

---

## 📖 Documentation

### Directory Structure

```
OMOP_GLP1/
├── README.md                      # This file
├── LICENSE                        # MIT License
├── requirements.txt              # Python dependencies
├── setup.sh                      # Setup script
├── run_pipeline.sh              # Master pipeline runner
│
├── config/                       # Configuration files
│   ├── examples/                # Example configurations
│   │   ├── obesity_general.json
│   │   └── t1d_cohort.json
│   └── concept_sets/           # Concept set definitions
│       ├── comorbidities.csv
│       ├── medications.csv
│       ├── labs.json
│       └── glp1/               # GLP-1 drug concepts
│
├── src/                         # Source code
│   ├── __init__.py
│   ├── utils/                  # Utility modules
│   │   ├── __init__.py
│   │   ├── omop_connector.py  # DuckDB OMOP interface
│   │   ├── concept_expander.py # Concept hierarchy
│   │   └── helpers.py         # Helper functions
│   ├── preprocessing/          # Preprocessing modules
│   │   ├── 01_cohort_extraction.py
│   │   ├── 02_user_identification.py
│   │   ├── 03_eligibility_criteria.py
│   │   ├── 04_covariates_extraction.py
│   │   └── 05_propensity_matching.py
│   └── analysis/              # Analysis modules
│       ├── cox_analysis.py
│       └── msd_analysis.py
│
├── output/                    # Output directory (created by setup)
│   ├── cohorts/
│   ├── matched_pairs/
│   ├── analysis_results/
│   └── figures/
│
├── logs/                      # Log files (created by setup)
└── tests/                     # Unit tests (optional)
```

### Key Modules

#### `omop_connector.py`
- DuckDB-based interface for OMOP data
- Memory-efficient querying of large CSV files
- Methods for all OMOP clinical tables

#### `concept_expander.py`
- Expand concept IDs using concept_ancestor
- Filter by vocabulary and standard concepts

#### Preprocessing Modules
- Well-documented, modular design
- Extensive logging
- Error handling

---

## 🎯 Study Design

### Inclusion Criteria (Configurable)

**Base Cohort (AOM-Eligible):**
1. Adults ≥ 18 years on index date
2. Patients with obesity:
   - Recorded diagnosis of obesity, OR
   - BMI ≥ 30 kg/m², OR
   - BMI 27-29.9 kg/m² with weight-related comorbidity
3. At least one encounter before index date
4. Within study period (default: 2012-01-01 to 2024-01-31)

**Additional Criteria (Study-Specific):**
- Configurable via `require_specific_condition`
- Examples: Type 1 Diabetes, IBD, etc.

### Exclusion Criteria (Configurable)

- Patients with specified conditions at baseline
- Patients with insufficient follow-up time
- Other study-specific exclusions

### Matching Method

- **Propensity Score Model**: Logistic regression
- **Matching Algorithm**: 1:1 nearest neighbor
- **Caliper**: 0.2 × SD of logit propensity score
- **Balance Assessment**: Standardized mean difference (SMD < 0.1 indicates good balance)

---

## 📊 Data Requirements

### Required OMOP Tables

- `person`: Demographics
- `observation_period`: Follow-up time
- `visit_occurrence`: Encounters
- `condition_occurrence`: Diagnoses
- `drug_exposure`: Medications
- `measurement`: Laboratory values
- `procedure_occurrence`: Procedures (optional)
- `concept_ancestor`: Concept hierarchies

### File Format

CSV files with standard OMOP CDM column names.

---

## 🔧 Troubleshooting

### Common Issues

1. **"Table not found" error**
   - Ensure OMOP CSV files are in the specified `omop_data_dir`
   - Check file names match OMOP table names (e.g., `person.csv`)

2. **Memory issues**
   - DuckDB should handle large files efficiently
   - If issues persist, consider processing in batches

3. **No GLP-1 users found**
   - Verify GLP-1 concept IDs in `config/concept_sets/glp1/`
   - Check study date range

4. **Poor covariate balance (high SMD)**
   - Consider adding more matching features
   - Adjust caliper width
   - Try different matching algorithms

---

## 📝 Citation

If you use this pipeline in your research, please cite:

```bibtex
@software{omop_glp1_pipeline,
  author = {Dai, Hao},
  title = {OMOP GLP-1 Propensity Score Matching and Analysis Pipeline},
  year = {2025},
  organization = {Indiana University School of Medicine},
  url = {https://github.com/throwfox/Target-trial-emulation-pipline}
}
```

---

## 👥 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 📧 Contact

**Yao An Lee**  
Email: yaoanlee@ufl.edu  
Department of Pharmaceutical Outcomes & Policy  
University of Florida

---

## 🙏 Acknowledgments

This pipeline was developed at the University of Florida Department of Pharmaceutical Outcomes & Policy, contributing to advancements in data analytics and biomedical informatics.

Special thanks to:
- OneFL+ Data Trust for EHR data access
- OHDSI community for OMOP CDM standards
- All contributors and collaborators

---

## 📚 References

### OMOP CDM
- [OHDSI OMOP CDM Documentation](https://ohdsi.github.io/CommonDataModel/)

### Propensity Score Matching
- Rosenbaum PR, Rubin DB. The central role of the propensity score in observational studies for causal effects. Biometrika. 1983;70(1):41-55.
- Austin PC. An Introduction to Propensity Score Methods for Reducing the Effects of Confounding in Observational Studies. Multivariate Behav Res. 2011;46(3):399-424.

### GLP-1 Receptor Agonists
- Davies MJ, et al. Management of Hyperglycemia in Type 2 Diabetes, 2022. A Consensus Report by the American Diabetes Association (ADA) and the European Association for the Study of Diabetes (EASD). Diabetes Care. 2022;45(11):2753-2786.

---

**Version**: 1.0.0  
**Last Updated**: October 2025
