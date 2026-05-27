#!/usr/bin/env bash

#SBATCH --job-name=build_graph_csr
#SBATCH --output=logs/%j.out
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=1:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=512gb

set -e

uv run offline/build_graph_csr.py

# Stage the artifact for the backend container image build.
mkdir -p backend/build_data
cp output/graph.csr backend/build_data/graph.csr
