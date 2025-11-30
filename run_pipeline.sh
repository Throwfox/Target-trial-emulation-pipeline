#!/bin/bash

# OMOP GLP-1 Complete Pipeline Runner
# Usage: bash run_pipeline.sh <config_file.json>

if [ $# -eq 0 ]; then
    echo "Error: No configuration file provided"
    echo "Usage: bash run_pipeline.sh <config_file.json>"
    echo "Example: bash run_pipeline.sh config/examples/obesity_general.json"
    exit 1
fi

CONFIG_FILE=$1

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file not found: $CONFIG_FILE"
    exit 1
fi

echo "========================================"
echo "OMOP GLP-1 Analysis Pipeline"
echo "========================================"
echo "Configuration: $CONFIG_FILE"
echo "Start time: $(date)"
echo ""

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Step 1: Cohort Extraction
echo ""
echo "----------------------------------------"
echo "STEP 1: Cohort Extraction"
echo "----------------------------------------"
python3 src/preprocessing/01_cohort_extraction.py -c $CONFIG_FILE
if [ $? -ne 0 ]; then
    echo "Error in Step 1: Cohort Extraction"
    exit 1
fi

# Step 2: User Identification
echo ""
echo "----------------------------------------"
echo "STEP 2: GLP-1 User Identification"
echo "----------------------------------------"
python3 src/preprocessing/02_user_identification.py -c $CONFIG_FILE
if [ $? -ne 0 ]; then
    echo "Error in Step 2: User Identification"
    exit 1
fi

# Step 3: Eligibility Criteria
echo ""
echo "----------------------------------------"
echo "STEP 3: Apply Eligibility Criteria"
echo "----------------------------------------"
python3 src/preprocessing/03_eligibility_criteria.py -c $CONFIG_FILE
if [ $? -ne 0 ]; then
    echo "Error in Step 3: Eligibility Criteria"
    exit 1
fi

# Step 4: Covariates Extraction
echo ""
echo "----------------------------------------"
echo "STEP 4: Extract Baseline Covariates"
echo "----------------------------------------"
python3 src/preprocessing/04_covariates_extraction.py -c $CONFIG_FILE
if [ $? -ne 0 ]; then
    echo "Error in Step 4: Covariates Extraction"
    exit 1
fi

# Step 5: Propensity Score Matching
echo ""
echo "----------------------------------------"
echo "STEP 5: Propensity Score Matching"
echo "----------------------------------------"
python3 src/preprocessing/05_propensity_matching.py -c $CONFIG_FILE
if [ $? -ne 0 ]; then
    echo "Error in Step 5: Propensity Score Matching"
    exit 1
fi

echo ""
echo "========================================"
echo "Pipeline Complete!"
echo "========================================"
echo "End time: $(date)"
echo ""
echo "Results saved to output/"
echo "- Cohorts: output/cohorts/"
echo "- Matched pairs: output/matched_pairs/"
echo ""
echo "Next: Run analysis scripts in src/analysis/"
echo ""

