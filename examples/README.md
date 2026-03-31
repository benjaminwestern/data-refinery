# Examples directory

This directory is split between ingest templates, analysis templates, and
rewrite configs. The important difference is that analysis templates feed the
base app config model, while ingest and rewrite examples are passed directly
to `data-refinery ingest -config ...` or
`data-refinery rewrite -config ...`.

## How to use the ingest examples

Ingest examples are portable job definitions for the normalization workflow.
You edit the checked-in config and mapping files in place, then run them with
the ingest subcommand.

The main ingest output can be `.csv`, `.json`, `.ndjson`, or `.jsonl`. Use
`.csv` only when the normalized rows stay scalar. If your mapping produces the
unified `Attributes` array, use one of the JSON-family outputs instead.

1. Pick the ingest example and update `paths`, `outputPath`, `logPath`, or
   `mappingFile` for your environment.
2. Update the matching rules in the YAML or JSON mapping file if your source
   files use different column or field names.
3. Run the job with `./data-refinery ingest -config <file>`.

For example:

```sh
./data-refinery ingest -config examples/ingest-config.json
```

The `ingest-simple-*` examples are the clearest starting point if you want to
see CSV, TSV, and JSON handled as first-class inputs. The
`ingest-complex-*` examples show the supported nested-mapping pattern for
building the unified `Attributes` array from explicit nested paths.

## How to use the analysis examples

Analysis examples are standard app config templates. The cleanest way to run
them is with `--app-config`, although you can still copy a file into one of
the implicit config locations if that fits your workflow better.

1. Pick the example you want to try and update `path`, `logPath`, and any
   target values for your environment.
2. Run `./data-refinery --app-config <file>` for the TUI or add `-headless`
   for a scriptable run.
3. Optional: Copy the example into `data-refinery.json` or `config.json` if
   you want to use implicit config discovery instead.

For example:

```sh
./data-refinery --app-config examples/test_full_advanced.json -headless
```

CLI flags still override values loaded from the app config file.

## How to use the rewrite examples

Rewrite examples are portable job definitions. You edit them in place if
needed, then pass them to the rewrite command with `-config`.

1. Pick the nearest rewrite example.
2. Update any path, value list, or backup settings you need.
3. Run it with `./data-refinery rewrite -config <file>`.
4. Start with `mode: "preview"` before you switch to `mode: "apply"`.

For example:

```sh
./data-refinery rewrite -config examples/rewrite-delete-config.json
```

## Pick the right example

Use this guide when you want the quickest route to the right file.

| File | Use it when you want to |
| --- | --- |
| `ingest-config.json` | Normalize the checked-in CSV and JSONL fixtures into one unified JSONL dataset. |
| `ingest-simple-config.json` | Run the simplest CSV, TSV, and JSON normalization walkthrough. |
| `ingest-simple-mappings.yaml` | Start from direct field-to-schema mappings for CSV, TSV, and JSON inputs. |
| `ingest-complex-config.json` | Run a nested JSON normalization walkthrough that populates the `Attributes` array. |
| `ingest-complex-mappings.yaml` | Start from explicit nested-path mappings for complex JSON sources. |
| `ingest-mappings.yaml` | Start from a structured ingest mapping that supports exact matches, globs, and automatic attribute capture. |
| `advanced_config.json` | Start from a production-style advanced analysis template with search, hashing, schema discovery, and derived outputs. |
| `schema_only_config.json` | Discover field shape without enabling duplicate checks or deletion rules. |
| `selective_hash_config.json` | Compare rows using only business-relevant fields. |
| `rewrite-delete-config.json` | Preview or apply top-level row deletion from a portable rewrite config. |
| `rewrite-update-config.json` | Preview or apply recursive updates with backups. |
| `comprehensive_test.json` | Exercise most advanced analysis features against the repository fixtures. |
| `test_advanced.json` | Run a minimal advanced analysis smoke test against `./test_data`. |
| `test_config.json` | Run an analysis smoke test with search, schema, and derived deletion output. |
| `test_full_advanced.json` | Run a compact all-in-one advanced analysis walkthrough against `./test_data`. |

## Path behavior

Ingest, analysis, and rewrite resolve paths differently, and that difference
matters when you copy examples into your own environment.

- Ingest examples use `paths`, `outputPath`, `mappingFile`, and `logPath`.
  When you run `data-refinery ingest -config ...`, those relative values are
  resolved from the ingest config file directory.
- Analysis examples use a plain `path` field. Data Refinery reads that value as
  part of the loaded app config, so update it before you run the example.
- Rewrite examples use `paths`, `logPath`, and `backupDir`. When you run
  `data-refinery rewrite -config ...`, those relative values are resolved from
  the config file directory.

## Recommended first runs

If you want a quick tour of the repository fixtures, these commands are the
shortest path.

1. Run `./data-refinery ingest -config examples/ingest-simple-config.json`.
2. Optional: Run `./data-refinery ingest -config examples/ingest-complex-config.json`.
3. Run `./data-refinery --app-config examples/test_full_advanced.json -headless`.
4. Review the saved output under `logPath`.
5. Run `./data-refinery rewrite -config examples/rewrite-delete-config.json`.
