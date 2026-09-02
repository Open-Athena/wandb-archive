#!/bin/bash
#SBATCH --job-name=wandb-archive
#SBATCH --account=torch_pr_347_lzanna
#SBATCH --partition=cs
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=2-00:00:00
#SBATCH --open-mode=append

set -euo pipefail

: "${WANDB_API_KEY:?WANDB_API_KEY must be exported when the job is submitted}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID must be exported when the job is submitted}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY must be exported when the job is submitted}"

archive_root="/scratch/$USER/wandb-archive-runtime"
export TMPDIR="$archive_root/tmp/$SLURM_JOB_ID"
export UV_CACHE_DIR="$archive_root/uv-cache"
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
export AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
export PYTHONUNBUFFERED=1

mkdir -p "$TMPDIR" "$UV_CACHE_DIR"
cd "$SLURM_SUBMIT_DIR"

uv sync --locked --no-dev
uv run --no-sync wandb-archive backup examples/osn.yaml
uv run --no-sync wandb-archive verify examples/osn.yaml
