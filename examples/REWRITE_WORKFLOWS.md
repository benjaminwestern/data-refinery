# Rewrite workflows

This guide covers `data-refinery rewrite`, which is the operational side of
Data Refinery. Rewrite previews or applies streamed content changes, writes
backups in apply mode, and keeps the workflow scriptable through flags or a
portable JSON config file.

## What rewrite is for

Rewrite handles the current set of safe, record-oriented cleanup operations.

- Remove whole rows that match a key and value list.
- Remove nested array entries from each matching row.
- Update a value recursively anywhere in a row.
- Scope deletes or updates with optional state and ID filters.
- Store the job definition in a portable JSON config file.

Rewrite currently supports `.json`, `.ndjson`, `.jsonl`, and `.xml` inputs.
For XML, you must set `xmlRecordPath` so repeated elements are treated as
logical records.

The rewrite flow is CLI-only today. The TUI remains focused on analysis,
validation, and reporting.

## Safety model

Rewrite is designed to make operational cleanup predictable.

- `preview` reports what would change without writing anything.
- `apply` writes rewritten output only after it creates a timestamped backup
  snapshot.
- For JSON-family inputs, malformed or non-JSON lines are preserved as-is
  rather than dropped.
- XML inputs must parse as a valid document before rewrite begins.
- Relative `paths`, `logPath`, and `backupDir` values are resolved from the
  rewrite config file location.

For best results, use NDJSON, JSONL, single-line JSON objects, or XML with an
explicit `xmlRecordPath`.

## Portable config shape

The rewrite config is a flat JSON object. You can use either `path` or
`paths`, and you only need the fields for the operation you are running. Use
`xmlRecordPath` only for XML jobs.

```json
{
  "paths": ["../test_data/test1.json", "../test_data/test2.json"],
  "workers": 4,
  "logPath": "../logs",
  "xmlRecordPath": "orders.order",
  "mode": "preview",
  "backupDir": "../backups/rewrite",
  "topLevelKey": "customer_id",
  "topLevelValues": ["cust-456"],
  "arrayKey": "line_items",
  "arrayDeleteKey": "item_id",
  "arrayDeleteValues": ["li-001"],
  "stateKey": "status",
  "stateValue": "pending",
  "updateKey": "customer_id",
  "updateOldValue": "cust-123",
  "updateNewValue": "cust-999",
  "updateIDKey": "id",
  "updateIDValues": ["test1"],
  "updateStateKey": "status",
  "updateStateValue": "pending"
}
```

## Concrete examples

These examples cover the main rewrite jobs that the current engine supports.

### Preview top-level deletion

Use `rewrite-delete-config.json` when you want to preview which records would
be removed when `customer_id` matches a target list.

```sh
./data-refinery rewrite -config examples/rewrite-delete-config.json
```

### Apply recursive updates

Use `rewrite-update-config.json` when you want to update `customer_id` values
for a small, explicitly named set of records and keep a backup snapshot before
replacement.

```sh
./data-refinery rewrite -config examples/rewrite-update-config.json
```

### Preview an XML rewrite

Use an XML record path when each repeated element should behave like one
logical row in preview and apply mode.

```sh
./data-refinery rewrite \
  -path examples/smoke/rewrite/source/orders.xml \
  -xml-record-path orders.order \
  -top-level-key customer_id \
  -top-level-vals cust-delete \
  -mode preview
```

### Run directly from flags

Use flags when the job is one-off and you do not need a reusable config file.

```sh
./data-refinery rewrite \
  -path gs://example-bucket/orders \
  -top-level-key status \
  -top-level-vals archived,cancelled \
  -mode preview
```

### Use a CSV target list

Use a single-column CSV file when the target list is long enough that inline
CLI values are awkward to manage.

```sh
./data-refinery rewrite \
  -path ./test_data \
  -top-level-key customer_id \
  -top-level-vals ./ids.csv \
  -mode preview
```

The CSV reader expects a header row and one value per later row.

## Preview-first checklist

Use this checklist whenever you are preparing an applied rewrite.

1. Start with `mode: "preview"`.
2. Scope the job as tightly as possible with specific keys and value lists.
3. Confirm the preview count matches your expectation.
4. Switch to `mode: "apply"` only after you are comfortable with the result.
5. Keep `backupDir` set for every applied run.

## TUI and CLI split

Use the TUI for interactive analysis, validation, duplicate review, advanced
analysis configuration, and local purge flows. Use `data-refinery rewrite`
when you want to mutate source data.
