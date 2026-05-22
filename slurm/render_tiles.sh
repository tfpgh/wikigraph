#!/usr/bin/env bash

#SBATCH --job-name=render_tiles
#SBATCH --output=logs/%j.out
#SBATCH --partition=short
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=3:00:00
#SBATCH --cpus-per-task=128
#SBATCH --mem=256gb

uv run render_tiles.py
