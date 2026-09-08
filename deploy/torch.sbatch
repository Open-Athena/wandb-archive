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

archive_base="${WANDB_ARCHIVE_RUNTIME_ROOT:-/var/tmp/wandb-archive-$USER}"
archive_root="$archive_base/$SLURM_JOB_ID"
export TMPDIR="$archive_root/tmp"
export UV_CACHE_DIR="$archive_root/uv-cache"
export UV_PROJECT_ENVIRONMENT="$archive_root/venv"
export XDG_CACHE_HOME="$archive_root/cache"
export WANDB_CACHE_DIR="$archive_root/cache/wandb"
export WANDB_DATA_DIR="$archive_root/data/wandb"
export WANDB_DIR="$archive_root/logs"
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
export AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
export PYTHONUNBUFFERED=1

mkdir -p \
  "$TMPDIR" \
  "$UV_CACHE_DIR" \
  "$WANDB_CACHE_DIR" \
  "$WANDB_DATA_DIR" \
  "$WANDB_DIR"
cd "$SLURM_SUBMIT_DIR"

uv sync --locked --no-dev

backup_attempts="${WANDB_ARCHIVE_JOB_ATTEMPTS:-3}"
backup_complete=0
for ((attempt = 1; attempt <= backup_attempts; attempt++)); do
  if uv run --no-sync wandb-archive backup examples/osn.yaml; then
    backup_complete=1
    break
  fi
  if ((attempt < backup_attempts)); then
    delay=$((30 * 2 ** (attempt - 1) + RANDOM % 30))
    echo "Backup attempt $attempt/$backup_attempts failed; retrying in ${delay}s" >&2
    sleep "$delay"
  fi
done

if ((backup_complete == 0)); then
  echo "Backup failed after $backup_attempts attempts" >&2
  exit 1
fi

uv run --no-sync wandb-archive verify examples/osn.yaml
