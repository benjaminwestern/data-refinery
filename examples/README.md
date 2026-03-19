# Examples directory

This directory is split between analysis templates and rewrite configs. The
important difference is that analysis examples feed the base app config model,
while rewrite examples are passed directly to
`data-refinery rewrite -config ...`.

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

Analysis and rewrite resolve paths differently, and that difference matters
when you copy examples into your own environment.

- Analysis examples use a plain `path` field. Data Refinery reads that value as
  part of the loaded app config, so update it before you run the example.
- Rewrite examples use `paths`, `logPath`, and `backupDir`. When you run
  `data-refinery rewrite -config ...`, those relative values are resolved from
  the config file directory.

## Recommended first runs

If you want a quick tour of the repository fixtures, these commands are the
shortest path.

1. Run `./data-refinery --app-config examples/test_full_advanced.json -headless`.
2. Review the saved output under `logPath`.
3. Run `./data-refinery rewrite -config examples/rewrite-delete-config.json`.
