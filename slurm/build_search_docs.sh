#!/usr/bin/env bash

#SBATCH --job-name=build_search_docs
#SBATCH --output=logs/%j.out
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=1:00:00
#SBATCH --cpus-per-task=32
#SBATCH --mem=128gb

set -e

uv run offline/build_search_docs.py

# Stage the artifact for the backend container image build.
mkdir -p backend/build_data
cp output/search_docs.jsonl backend/build_data/search_docs.jsonl
