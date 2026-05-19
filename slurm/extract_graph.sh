#!/usr/bin/env bash

#SBATCH --job-name=extract_graph
#SBATCH --output=logs/%j.out
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=3:00:00
#SBATCH --cpus-per-task=192
#SBATCH --mem=512gb

uv run extract_graph.py
