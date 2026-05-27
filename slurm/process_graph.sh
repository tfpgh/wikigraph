#!/usr/bin/env bash

#SBATCH --job-name=extract_graph
#SBATCH --output=logs/%j.out
#SBATCH --partition=nvgpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=3:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=256gb
#SBATCH --gpus=1
#SBATCH --constraint=GPU_SKU:RTX6000


uv run offline/process_graph.py
