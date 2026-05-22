#!/usr/bin/env bash

#SBATCH --job-name=render_tiles
#SBATCH --output=logs/%j.out
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=6:00:00
#SBATCH --cpus-per-task=192
#SBATCH --mem=512gb

uv run render_tiles.py
