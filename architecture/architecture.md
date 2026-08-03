# Insurance data pipeline — architecture

## 1. Sources → landing

Three tables (`Agent`, `Branch`, `Claim`) live in an **Azure SQL Database**.
`Customer` arrives as a **CSV** feed from a separate source system, and
`Policy` arrives as a **JSON** feed from another separate source. All three
land in one common **landing zone** in ADLS (Delta Lake storage), each table
in its own folder, before anything else in the pipeline touches them.

## 2. The ADF pipeline, activity by activity

The whole thing — ingestion and transformation — runs as a **single Azure
Data Factory pipeline**, parameterized with a `pipeline_run_id` that gets
passed down into every notebook:

1. **Lookup** — reads the active rows out of the `bronze_config` metadata
   table (which tables are enabled, where their source lives, what load
   strategy to use).
2. **ForEach** (with a **Copy activity** inside it) — iterates the rows the
   Lookup returned, and for each one, copies that table's SQL data into its
   landing folder. This is what actually moves `Agent`/`Branch`/`Claim` out
   of Azure SQL DB. (`Customer` and `Policy` land via their own separate CSV
   / JSON feeds, not through this Copy activity.)
3. **Bronze notebook** — reads each landing folder, applies FULL or
   INCREMENTAL load logic per table, writes to `insurance_bronze`.
4. **Silver notebook** — reads Bronze, cleanses/standardizes/validates,
   writes to `insurance_silver`.
5. **Gold notebook** — reads Silver, builds the dimensional model
   (`dim_*` / `fact_*`) in `insurance_gold`.
6. **SCD2 notebook** — for the tables flagged `scd_enabled`, compares
   current Gold rows against tracked history and writes changes into
   `dim_*_history` tables.
7. **Set Success** — marks the pipeline run complete once every stage above
   has finished, for downstream monitoring/alerting to key off of.

Steps 3–6 are each their own notebook, but every one of them loops over the
*same* `bronze_config` metadata table internally — see §4.

## 3. Load strategy: FULL vs. INCREMENTAL

Each table in `bronze_config` is configured as one or the other. This is a
per-table setting, not a per-layer one — it's decided once, at Bronze, and
Gold mirrors whatever Bronze chose for that table.

| Table    | Load strategy | Watermark column      |
|----------|---------------|------------------------|
| Agent    | FULL          | create_timestamp       |
| Branch   | FULL          | updated_at              |
| Claim    | INCREMENTAL   | LastUpdatedTimeStamp    |
| Customer | INCREMENTAL   | registration_date       |
| Policy   | FULL          | created_at               |

**FULL**: every run, the entire current source dataset is reloaded and the
Bronze table is completely overwritten (`mode="overwrite"`,
`overwriteSchema=true`). Simple and always internally consistent, at the
cost of reprocessing everything every time — fine for tables that are
small or don't change row-by-row often (Agent, Branch, Policy here).

**INCREMENTAL**: only new or changed rows since the last successful run are
pulled. A small control table, `pipeline_watermark`, stores the latest
value seen for each table's watermark column. Each run reads the source,
filters to rows where the watermark column is greater than the stored
value, appends only those rows (`mode="append"`), then advances the stored
watermark. This avoids reprocessing the full history every run — important
for tables that keep growing or updating (Claims accrue over time, the
Customer base keeps growing).

Silver, by contrast, always overwrites its own table completely on every
run — it isn't FULL/INCREMENTAL itself, it just re-derives fresh from
whatever is currently in Bronze at run time.

## 4. Notebook purposes

Two shared framework notebooks are pulled into every loader via `%run`:

- **00_Configuration** — catalog, schema, and landing-path constants.
- **01_Utility_Functions** — shared helpers: `add_metadata()`,
  `write_audit()`, duplicate/null-count checks, etc.

Four generic loader notebooks, each responsible for one layer, each driven
entirely by `bronze_config` rather than being written per-table:

- **Bronze loader** — raw ingest per table, FULL/INCREMENTAL branching,
  watermark read/update, data-quality counts, audit logging.
- **Silver loader** — generic cleansing (trim, dedupe on primary key),
  standardization (casing, whitespace), business validation (drop null
  keys/emails), tags metadata columns, writes to Silver.
- **Gold loader** — builds the dimensional model: `dim_*` tables for
  reference/dimension data, `fact_*` for transactional data like claims.
- **SCD2 loader** — for `scd_enabled=Y` tables only, compares current Gold
  rows to the current "is_current" history rows by `compare_columns`;
  unchanged rows are left alone, changed rows get their old version expired
  (`is_current=false`, `effective_end_date` set) and a new current version
  inserted. This preserves a full history of attribute changes over time
  for Agent, Branch, and Customer (Claim and Policy aren't SCD-tracked).

## 5. Why this is metadata-driven, and what that buys you

None of the four loader notebooks contain any table-specific code. Each one
does `for config in metadata_rows:` over the active rows in `bronze_config`
and applies the same generic logic to whichever table that row describes —
source folder, file format, primary key, load strategy, watermark column,
target Gold/history table names, SCD flag.

That means onboarding a new source table is a **metadata change, not a
code change** — you add one row to `bronze_config` and the same four
notebooks pick it up on the next run. The system doesn't get more complex
as it grows; only the config table does. Five tables today or fifty next
year run through the exact same Bronze/Silver/Gold/SCD2 notebooks with zero
new code.

## 6. Operational safety net

- Every table is processed inside its own `try/except` in each loader's
  loop — one table failing doesn't stop the others in the same run.
- Every table's read/write counts and status are logged to an **audit
  table** per `pipeline_run_id`, giving a queryable trail of exactly what
  happened on every run.
- Table ACLs are enabled on the cluster, so Delta will never silently
  auto-merge a schema mismatch — any drift between what a notebook is
  trying to write and what a table already has hard-fails loudly instead
  of corrupting data quietly. This caught three separate real issues during
  development (see below) instead of letting them pass silently.

## 7. Bugs found and fixed during development

- **Silver loader loop-scoping bug** — cleansing/validation/write code had
  been split into cells outside the main `for` loop during editing, so only
  the last table processed (Policy) ever actually reached Silver, and a
  leftover empty-loop cell was resetting the run summary to "0 tables
  processed." Fixed by consolidating all per-table logic back inside the
  loop and removing the dead cells.
- **Bronze watermark timestamp bug** — `to_timestamp()` with no explicit
  format threw under ANSI mode on Claim's `dd-MM-yyyy HH:mm` date strings,
  hard-failing that table's incremental load. Fixed with a tolerant parser
  that tries the default format first, falls back to the known alternate
  format, and never throws — logging a warning instead if a value truly
  can't be parsed.
- **Bronze schema-drift bug** — `inferSchema` has no memory across runs, so
  a batch where a numeric column happened to have no decimal values got
  inferred as a narrower type than what the existing Bronze table already
  had, and Delta refused to append the mismatch. Fixed generically: before
  writing, every incoming column is cast to match the existing table's
  already-committed type, for every table — not hardcoded to the one column
  that happened to break first.
- **SCD2 stale-schema bug** — `dim_agent_history` had been created earlier
  under an older metadata-column naming convention and never reconciled
  after that convention changed, so it no longer matched current Gold
  schema. Resolved as a one-time table rebuild (dropped, let the loader's
  existing "create on first run" logic regenerate it under current schema).
