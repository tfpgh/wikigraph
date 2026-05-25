#!/usr/bin/env bash

#SBATCH --job-name=render_edges
#SBATCH --output=logs/%j.out
#SBATCH --partition=general,nvgpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=1:00:00
#SBATCH --cpus-per-task=192
#SBATCH --mem=768gb

uv run python -m tiles.edges
