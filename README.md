# W&B Archive

Publish durable, queryable archives of [Weights & Biases](https://wandb.ai/)
experiments to object storage you control.

`wandb-archive` exports experiment histories, metadata, tables, media, files,
and artifacts through the W&B API; converts tabular data to Parquet; and
publishes it to local or S3-compatible storage. Repeated runs are idempotent,
so the same command can perform an initial backfill and later run on a monthly
schedule.

The resulting archive is designed for direct analysis with tools such as
DuckDB, Polars, Pandas, and PyArrow, and as a stable data source for a custom
experiment browser.

> [!WARNING]
> This project is in early development. The interfaces below describe the
> intended first release. Do not delete source data from W&B until
> `wandb-archive verify` reports that the relevant runs are deletion-ready.

## Why archive W&B?

Experiment trackers are excellent working tools, but research records often
need a different lifecycle:

- Storage quotas should not determine how long an experiment remains useful.
- Finished experiments should stay available after their original authors
  leave a project.
- Metrics should be queryable without sampling or a proprietary dashboard.
- Public research projects should be able to publish their experimental
  evidence alongside code, data, and papers.
- A recurring backup should transfer only new or changed data and recover
  safely after interruption.

`wandb-archive` is an export and publication tool, not a clone of the W&B user
interface. It preserves the underlying experiment data in documented formats
so other interfaces can be built on top of it.

## What is archived?

| W&B data | Archive representation |
| --- | --- |
| Run identity, state, timestamps, tags, group, notes, and sweep relationship | Parquet catalog and JSON |
| Nested run configuration and summary | Lossless JSON plus queryable catalog fields |
| Complete metric history | Original W&B Parquet export plus normalized Parquet |
| System metrics | Original export plus normalized Parquet |
| Histograms | Parquet with bins, counts, run, metric, and step |
| W&B Tables | One Parquet table per logged table version |
| Images, video, and other media | Original browser-native files with a Parquet index |
| Run files and logged artifacts | Original bytes, manifests, hashes, and lineage |
| Used artifacts | Lineage and metadata by default; contents are configurable |
| Zarr stores logged as files or artifacts | Original cloud-optimized store objects |

Every exported object is recorded in a manifest with its size and SHA-256
digest. The archive also records exclusions and incomplete API responses; it
does not silently describe a partial export as complete.

The first release does **not** attempt to preserve W&B workspace or report
layouts, access-control settings, alerting, or a restorable copy of the W&B
service itself. Data referenced by an artifact but stored externally is not
copied unless explicitly configured.

## Quick start

The project supports Python 3.12 or newer and is managed with
[`uv`](https://docs.astral.sh/uv/).

While developing from a checkout:

```bash
git clone https://github.com/Open-Athena/wandb-archive.git
cd wandb-archive
uv sync --locked

# See what would be exported. This reads W&B but writes nothing.
uv run wandb-archive plan archive.yaml

# Export and publish selected runs.
uv run wandb-archive backup archive.yaml

# Verify the published archive from the destination.
uv run wandb-archive verify archive.yaml
```

After releases are available, the CLI will also be installable independently:

```bash
uv tool install wandb-archive
wandb-archive plan archive.yaml
```

## Authentication

Authenticate to W&B with an API key in the environment:

```bash
export WANDB_API_KEY=...
```

For a writable S3 or S3-compatible destination, use the standard AWS
credential variables:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Only when using temporary credentials:
export AWS_SESSION_TOKEN=...
```

Credentials do not belong in the archive configuration. A public destination
can be read anonymously after publication, but still requires credentials for
writes.

## Configuration

The CLI accepts a validated YAML configuration. Unknown fields are errors, and
the resolved configuration is saved with each archive operation.

This example archives terminal runs from every project in a W&B entity to the
public [M2LInES](http://m2lines.github.io/) OSN pod:

```yaml
source:
  entity: ocean_emulators

  projects:
    include:
      - "*"
    exclude:
      - "scratch-*"

  runs:
    states:
      - finished
      - failed
      - crashed
      - killed
    created_after: null
    created_before: null
    include_tags: []
    exclude_tags: []

destination:
  type: s3
  endpoint_url: https://nyu1.osn.mghpcc.org
  bucket: m2lines-pubs
  prefix: Samudra/wandb
  public_url: https://nyu1.osn.mghpcc.org/m2lines-pubs/Samudra/wandb

archive:
  strict: true

  include:
    histories: true
    system_metrics: true
    media: true
    tables: true
    run_files: safe
    logged_artifacts: safe
    used_artifacts: references
    code: false
    console_logs: false

  security:
    profile: public-safe
    on_sensitive_value: fail

  transfers:
    concurrency: 4
    retries: 5
```

### Source selection

`source.entity` is required. Project names are selected with case-sensitive
glob patterns. An explicit list can be used instead of `"*"`:

```yaml
projects:
  include:
    - samudra
    - samudra-eval
  exclude: []
```

Run selection can be narrowed by state, creation time, or tags. Running runs
may be included, but they are recorded as mutable and incomplete. A later
backup will refresh them rather than treating the earlier generation as final.

For a W&B self-managed deployment, set its API URL explicitly:

```yaml
source:
  base_url: https://wandb.example.org
  entity: research-team
```

### Destinations

Any S3-compatible service can use the `s3` destination. `endpoint_url` may be
omitted for AWS S3. Development and offline exports can use an absolute local
directory:

```yaml
destination:
  type: local
  path: /data/wandb-archive
```

`public_url` is optional. When present, catalogs contain browser-readable URLs
in addition to storage URIs.

For a webpage hosted on another origin, the bucket must also allow cross-origin
`GET` and `HEAD` requests. Bucket visibility and CORS policy are infrastructure
settings; this tool records public URLs but deliberately does not change either
policy.

### Inclusion policies

Run files and artifact contents support these policies:

- `all`: copy all bytes reported by W&B;
- `safe`: include recognized tables and media, scan readable content, and
  reject unknown or disallowed file types;
- `metadata`: retain manifests and lineage without copying contents;
- `none`: omit the category.

Used artifacts default to `references` because shared input datasets can be
very large or already live in another durable store. Their identity, version,
digest, producer, and external references remain queryable.

`public-safe` excludes console output, source snapshots, sensitive machine
metadata, and unsafe file types by default. Text-like data is scanned for
credential-shaped values before upload. Binary files cannot be proven free of
embedded secrets; use an explicit allowlist or a non-public destination when
that distinction matters.

Every policy decision is written to the run manifest. A policy-complete
archive is therefore distinguishable from a byte-for-byte export.

## Commands

### Preview an operation

```bash
wandb-archive plan archive.yaml
```

`plan` authenticates to W&B and reports selected projects, runs, states,
estimated file bytes, existing archive generations, expected transfers, and
policy exclusions. It does not download run contents or write to the
destination.

### Back up runs

```bash
wandb-archive backup archive.yaml
```

The backup stages one run at a time, validates it locally, uploads missing
content-addressed objects, and commits a new run generation only after all of
its required data is present. Existing terminal runs with the same source
fingerprint and archive schema are skipped.

Useful targeted operations include:

```bash
wandb-archive backup archive.yaml --project samudra
wandb-archive backup archive.yaml --run ocean_emulators/samudra/abc123
wandb-archive backup archive.yaml --since 2026-08-01
```

### Verify an archive

```bash
wandb-archive verify archive.yaml
wandb-archive verify archive.yaml --deep
```

Normal verification checks manifests, object metadata, expected row counts,
and archive relationships. Deep verification reads every object, recomputes
its SHA-256 digest, and opens every Parquet file.

Verification can use anonymous reads when the destination is public; it does
not require W&B access unless source comparison is requested.

### Inspect a run

```bash
wandb-archive inspect archive.yaml ocean_emulators/samudra/abc123
```

`inspect` explains which generation is current, what it contains, why anything
was excluded, whether the source was mutable, and whether the run is eligible
for source deletion.

## Idempotency and recovery

A run is identified by its immutable W&B path:

```text
<entity>/<project>/<run-id>
```

Each exported generation has a fingerprint derived from source metadata,
history position, run-file inventory, artifact manifests, and archive schema.
Objects are stored by content hash and run generations are immutable. A small
pointer is published last to make a generation visible to readers.

Consequently:

- re-running an unchanged backup transfers no run data;
- changed or previously running runs receive a new generation;
- interrupted uploads are safely resumed;
- identical files can be shared across runs without duplicate storage;
- runs already archived are retained even after disappearing from W&B; and
- no remote archive object is automatically deleted.

## Archive layout

The public contract is a versioned catalog plus immutable run generations and
content-addressed files:

```text
<archive-root>/
├── archive.json
├── catalogs/<generation>/
│   ├── runs.parquet
│   ├── files.parquet
│   ├── artifacts.parquet
│   ├── tables.parquet
│   └── deletion_candidates.parquet
├── runs/<entity>/<project>/<run-id>/
│   ├── latest.json
│   └── generations/<fingerprint>/...
└── blobs/sha256/<prefix>/<digest>
```

`archive.json` identifies the archive schema and current catalog generation.
Catalogs are intended for cross-run queries. Each run generation also retains
the lossless source export needed to audit normalization or adopt a newer
schema later.

Normalized scalar histories use a stable long-form shape:

```text
entity, project, run_id, step, timestamp, metric, value
```

Metric names are treated as data rather than Parquet columns, so projects can
introduce new metrics without changing the shared schema.

### Querying catalogs

The root index points to the current immutable catalog generation. For a
public archive, fetch it first and pass the referenced Parquet URL to DuckDB:

```bash
ROOT=https://nyu1.osn.mghpcc.org/m2lines-pubs/Samudra/wandb
RUNS=$(curl -fsSL "$ROOT/archive.json" | jq -r '.catalogs.runs')

duckdb -c "
  SELECT project, state, count(*) AS runs
  FROM read_parquet('$ROOT/$RUNS')
  GROUP BY ALL
  ORDER BY project, state;
"
```

`files.parquet` maps each run-local logical object to its content-addressed
blob and public URL. Filter it by `kind = 'metrics'`, `system-metrics`,
`histograms`, `table`, or `run-file` to locate data for a run without listing
the bucket. Nested configuration and summary values are strict JSON strings,
which DuckDB, Polars, and PyArrow can parse without schema drift.

The archive assumes a single catalog writer at a time. The example scheduled
workflow enforces that with a GitHub Actions concurrency group. Independent
simultaneous writers could both publish valid run generations while racing to
advance `archive.json`.

## Deleting data from W&B

`wandb-archive` never deletes W&B data.

After verification, terminal runs that satisfy the configured completeness
policy are listed in `deletion_candidates.parquet`. A run is not eligible when
its history is incomplete, required files are missing, an upload or integrity
check failed, it was still running at export time, or the configured policy
requires data that was excluded.

Deletion remains a separate, deliberate human or administrative operation.
Keeping it outside this tool prevents a backup configuration mistake from
becoming a destructive W&B operation.

## GitHub Action

The same CLI is available as a composite GitHub Action. The repository
using the action owns the schedule, archive configuration, and credentials:

```yaml
name: Archive W&B

on:
  schedule:
    - cron: "23 8 1 * *"
  workflow_dispatch:

concurrency:
  group: wandb-archive
  cancel-in-progress: false

permissions:
  contents: read

jobs:
  archive:
    runs-on: ubuntu-latest
    timeout-minutes: 330

    steps:
      - uses: actions/checkout@<full-commit-sha>

      - uses: Open-Athena/wandb-archive@<full-commit-sha>
        with:
          config: .github/wandb-archive.yaml
          command: backup
        env:
          WANDB_API_KEY: ${{ secrets.WANDB_API_KEY }}
          AWS_ACCESS_KEY_ID: ${{ secrets.OSN_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.OSN_SECRET_ACCESS_KEY }}
```

Pin the action to a reviewed full commit SHA when giving it access to storage
and W&B credentials. Versioned `v1.x.y` releases will be provided for discovery
and testing.

The initial backfill may exceed the time or temporary disk available on a
GitHub-hosted runner and should normally be run from a workstation or cluster.
The idempotent monthly update is the intended GitHub Actions workload.

## Development

```bash
uv sync --dev
uv run pytest
uv run pre-commit run --all-files
```

Tests use fake W&B responses and local object storage by default. Live W&B
and S3-compatible integration tests will be opt-in and will never delete
source data.

## Project status

The first milestone is a verified Samudra archive on the public M2LInES OSN
pod. The next milestone is a static web application that reads the same public
catalog and media objects without requiring a W&B account or an application
server.
