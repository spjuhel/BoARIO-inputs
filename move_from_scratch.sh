#!/bin/bash
#SBATCH --job-name=transfer             # Job name
#SBATCH --mail-type=END,FAIL                   # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=sjuhel@centre-cired.fr
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=4   # Number of CPU cores per task
#SBATCH --mem=2G# Memory per thread
#SBATCH --time=10:00:00

rsync -rv --include "*/" --include="*.json" --include="*.parquet" --include="*.log" --exclude="*" "/scratchu/sjuhel/Runs/BoARIO-runs/" "/data/sjuhel/Runs/BoARIO-runs"
