#!/bin/bash
# ============================================================
# Task 1 — Linux File System & Pipeline Directory Setup
# Multi-Vendor Marketplace Project
# ============================================================

echo "============================================"
echo "  Multi-Vendor Marketplace — Linux Setup"
echo "============================================"

# Create logs directory first so we can log everything
mkdir -p logs

# Create structured pipeline directories
echo "[$(date)] Creating pipeline directory structure..." | tee -a logs/setup.log
mkdir -p data/raw
mkdir -p data/clean
mkdir -p data/spark_output
mkdir -p docs
echo "[$(date)] Directories created successfully" | tee -a logs/setup.log

# Set file permissions on all Python scripts
echo "[$(date)] Setting file permissions..." | tee -a logs/setup.log
chmod 755 scripts/*.py
echo "[$(date)] File permissions set to 755 for all scripts" | tee -a logs/setup.log

# Move raw CSVs to data/raw/
echo "[$(date)] Moving raw CSV files to data/raw/..." | tee -a logs/setup.log
mv data/*.csv data/raw/ 2>/dev/null
echo "[$(date)] Raw CSV files moved to data/raw/" | tee -a logs/setup.log

# Move clean CSVs to data/clean/
echo "[$(date)] Moving clean CSV files to data/clean/..." | tee -a logs/setup.log
mv data/raw/clean_*.csv data/clean/ 2>/dev/null
echo "[$(date)] Clean CSV files moved to data/clean/" | tee -a logs/setup.log

# Move spark output CSVs to data/spark_output/
echo "[$(date)] Moving Spark output files to data/spark_output/..." | tee -a logs/setup.log
mv data/raw/spark_*.csv data/spark_output/ 2>/dev/null
echo "[$(date)] Spark output files moved to data/spark_output/" | tee -a logs/setup.log

# Display final directory structure
echo "[$(date)] Final directory structure:" | tee -a logs/setup.log
find data/ -type f | tee -a logs/setup.log

echo "[$(date)] Setup complete!" | tee -a logs/setup.log
echo "============================================"
echo "  Setup Complete! Check logs/setup.log"
echo "============================================"