# Advanced analysis features

Advanced analysis extends the read-only side of Data Refinery. Use it when you
need more than duplicate counts and key validation, such as targeted search,
schema discovery, business-key dedupe rules, or derived cleanup artifacts that
help you plan a later rewrite.

## How advanced analysis is loaded

Advanced analysis lives inside the standard app config model. The recommended
way to run these files is with `--app-config`, although copying a template into
an implicit config location still works when you want that behavior.

```sh
./data-refinery --app-config examples/test_full_advanced.json -headless
```

Use CLI flags when you want to override a loaded setting for one run.

## What the advanced model covers

All advanced analysis settings live under the `advanced` block in your config.
The model is built around four questions:

- Which records or nested values match a business-defined target list?
- Which fields should define duplicate rows?
- What does the observed schema look like across folders or samples?
- Which derived artifacts do you want to review before a cleanup run?

```json
{
  "advanced": {
    "searchTargets": [],
    "hashingStrategy": {},
    "deletionRules": [],
    "schemaDiscovery": {},
    "backup": {},
    "output": {}
  }
}
```

## Core capabilities

This section maps the advanced settings to the behavior you get at runtime.

### Search targets

Search targets define what to look for and where to look for it.

| Type | Best for | Example path |
| --- | --- | --- |
| `direct` | Top-level key matching | `customer_id` |
| `nested_object` | Nested object traversal | `product.name` |
| `nested_array` | Matching inside repeated items | `line_items[*].item_id` |
| `jsonpath` | More expressive traversal patterns | `$.items[*].sku` |

Each target needs a unique `name`, a `type`, a `path`, and one or more
`targetValues`.

### Hashing strategy

Hashing controls what counts as a duplicate row.

| Mode | Behavior |
| --- | --- |
| `full_row` | Hash the complete row. |
| `selective` | Hash only the keys listed in `includeKeys`. |
| `exclude_keys` | Hash everything except the keys listed in `excludeKeys`. |

Use `exclude_keys` when operational timestamps or ingestion metadata change
often. Use `selective` when only a small set of business fields should define
identity.

### Deletion rules

Deletion rules in advanced analysis do not rewrite the source dataset. They
generate derived outputs that help you review or stage later cleanup work.

| Action | Result |
| --- | --- |
| `delete_row` | Emit artifacts for rows that should be removed entirely. |
| `delete_matches` | Emit artifacts for rows where matching nested values should be removed. |
| `mark_for_deletion` | Emit rows with deletion metadata for review workflows. |
| `delete_sub_key` | Target a specific nested sub-key for derived cleanup output. |

Each rule references a `searchTarget` by name.

### Schema discovery

Schema discovery samples records and records what fields appear, how often they
appear, and which types were observed.

Use it when you want to:

- understand a new dataset before choosing a dedupe key
- compare folder-level shape differences
- confirm whether a cleanup rule should be broad or tightly scoped

## Concrete examples

These examples show the quickest way to use the advanced model for real jobs.

### Schema-only exploration

Use `schema_only_config.json` when you want to map the shape of a dataset
before you decide how to dedupe or rewrite it.

```sh
./data-refinery --app-config examples/schema_only_config.json -headless
```

### Search plus derived cleanup review

Use `test_config.json` when you want to search for specific customers or line
items, inspect duplicate behavior, and generate deletion artifacts without
touching the source data.

```sh
./data-refinery --app-config examples/test_config.json -headless
```

### Business-key duplicate detection

Use `selective_hash_config.json` when timestamps or operational metadata should
not affect duplicate-row matching.

```json
{
  "advanced": {
    "hashingStrategy": {
      "mode": "selective",
      "includeKeys": ["customer_id", "product.name", "line_items"],
      "normalize": true
    }
  }
}
```

## Generated artifacts

Advanced analysis writes extra artifacts under `logPath` when the matching
feature is enabled.

- `search_results_*.json` for the combined search result set.
- `search_target_<name>_*.json` for per-target search output.
- `schema_report_*.json`, `schema_report_*.csv`, and
  `schema_report_*.yaml` for schema discovery.
- `deletion_stats_*.json` and `deletion_summary_*.txt` for deletion-rule
  output.
- `analysis_summary_*.txt`, `analysis_details_*.txt`, and
  `analysis_report_*.json` when standard report output is enabled.

## When to move to rewrite

Stay in advanced analysis when you still need to answer questions about scope,
shape, or candidate records. Move to `data-refinery rewrite` once the analysis
output tells you exactly which rows or values should change and you are ready
for preview plus backups.
