#!/bin/bash

# OMOP GLP-1 Pipeline Setup Script

echo "========================================"
echo "OMOP GLP-1 Pipeline Setup"
echo "========================================"

# Check Python version
echo "Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"

# Create virtual environment (optional)
read -p "Create virtual environment? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
fi

# Install requirements
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create output directories
echo "Creating output directories..."
mkdir -p output/cohorts
mkdir -p output/matched_pairs
mkdir -p output/analysis_results
mkdir -p output/figures
mkdir -p logs

# Create concept sets directory structure
echo "Setting up concept sets directories..."
mkdir -p config/concept_sets/glp1

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo "1. Configure your OMOP data path in config/examples/*.json"
echo "2. Update concept sets in config/concept_sets/"
echo "3. Run pipeline: bash run_pipeline.sh config/examples/obesity_general.json"
echo ""
echo "For more information, see README.md"
echo ""

