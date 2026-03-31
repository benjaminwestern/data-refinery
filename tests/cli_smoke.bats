#!/usr/bin/env bats

setup_file() {
  export REPO_ROOT
  REPO_ROOT="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)"
  export RUNTIME_ROOT="$REPO_ROOT/.tmp/smoke-cli"
  cd "$REPO_ROOT"
  rm -rf "$RUNTIME_ROOT"
  mkdir -p "$RUNTIME_ROOT"
  python3 scripts/generate_smoke_xlsx.py
  go build -o "$REPO_ROOT/data-refinery" ./cmd/data-refinery
}

setup() {
  cd "$REPO_ROOT"
}

runtime_dir() {
  local dir="$RUNTIME_ROOT/$1"
  rm -rf "$dir"
  mkdir -p "$dir"
  printf '%s\n' "$dir"
}

assert_output_contains() {
  local expected="$1"
  if [[ "$output" != *"$expected"* ]]; then
    echo "expected output to contain: $expected" >&2
    echo "$output" >&2
    return 1
  fi
}

assert_file_contains() {
  local file="$1"
  local expected="$2"
  grep -Fq "$expected" "$file"
}

json_value() {
  local file="$1"
  local expr="$2"
  python3 - "$file" "$expr" <<'PY'
import json
import sys

path, expr = sys.argv[1], sys.argv[2]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
value = data
for part in expr.split("."):
    if part.isdigit():
        value = value[int(part)]
    else:
        value = value[part]
if isinstance(value, (dict, list)):
    print(json.dumps(value, sort_keys=True))
else:
    print(value)
PY
}

@test "CLI help surfaces respond" {
  run ./data-refinery help analysis
  [ "$status" -eq 0 ]

  run ./data-refinery help ingest
  [ "$status" -eq 0 ]

  run ./data-refinery help rewrite
  [ "$status" -eq 0 ]
}

@test "analysis validation covers csv tsv xlsx json jsonl xml inputs" {
  run ./data-refinery \
    -validate \
    -output json \
    -path ./examples/smoke/analysis/customers.csv,./examples/smoke/analysis/customers.tsv,./examples/smoke/analysis/customers.xlsx,./examples/smoke/analysis/orders.json,./examples/smoke/analysis/events.jsonl,./examples/smoke/analysis/customers.xml \
    -key id \
    --xml-record-path customers.customer

  [ "$status" -eq 0 ]
  assert_output_contains '"isValidationReport": true'
  assert_output_contains '"totalRowsProcessed": 12'
}

@test "analysis headless reports duplicate ids across all supported analysis input types" {
  local dir
  dir="$(runtime_dir analysis-headless)"

  run ./data-refinery \
    --app-config examples/smoke/app-config.json \
    -headless \
    -output json

  [ "$status" -eq 0 ]
  assert_output_contains '"totalRowsProcessed": 12'
  assert_output_contains '"duplicateIds"'
  assert_output_contains 'smoke-dup-1'
  find "$REPO_ROOT/.tmp/smoke-cli/analysis" -type f -name '*.json' | grep -q .
  find "$REPO_ROOT/.tmp/smoke-cli/analysis" -type f -name '*.txt' | grep -q .
}

@test "analysis headless reports duplicate rows" {
  run ./data-refinery \
    --app-config examples/smoke/analysis-duplicate-rows.json \
    -headless \
    -output json

  [ "$status" -eq 0 ]
  assert_output_contains '"duplicateRows"'
  assert_output_contains '"duplicateRowInstances": 2'
}

@test "ingest ndjson output covers csv tsv xlsx xml json jsonl inputs" {
  run ./data-refinery ingest --config examples/smoke/ingest-config.json

  [ "$status" -eq 0 ]
  assert_output_contains 'Files processed: 6'
  assert_output_contains 'Rows written: 12'
  [ -f "$REPO_ROOT/.tmp/smoke-cli/ingest/unified.ndjson" ]
  [ -f "$REPO_ROOT/.tmp/smoke-cli/ingest/summary.json" ]
  [ "$(wc -l < "$REPO_ROOT/.tmp/smoke-cli/ingest/unified.ndjson")" -eq 12 ]
  assert_file_contains "$REPO_ROOT/.tmp/smoke-cli/ingest/unified.ndjson" '"Id":"ing-xlsx-1"'
  assert_file_contains "$REPO_ROOT/.tmp/smoke-cli/ingest/unified.ndjson" '"Id":"ing-xml-1"'
}

@test "ingest json output writes a json array" {
  local dir output_path summary_path
  dir="$(runtime_dir ingest-json)"
  output_path="$dir/unified.json"
  summary_path="$dir/summary.json"

  run ./data-refinery ingest \
    --config examples/smoke/ingest-config.json \
    --approved-output-root "$RUNTIME_ROOT" \
    --log-path "$dir/logs" \
    --output-path "$output_path" \
    --stats-output-path "$summary_path"

  [ "$status" -eq 0 ]
  [ -f "$output_path" ]
  [ -f "$summary_path" ]
  [ "$(json_value "$summary_path" rowsWritten)" = "12" ]
  [ "$(python3 - "$output_path" <<'PY'
import json, sys
with open(sys.argv[1], 'r', encoding='utf-8') as fh:
    print(len(json.load(fh)))
PY
)" = "12" ]
  assert_file_contains "$output_path" '"Id":"ing-json-1"'
  assert_file_contains "$output_path" '"Id":"ing-xml-2"'
}

@test "ingest jsonl output writes newline records" {
  local dir output_path summary_path
  dir="$(runtime_dir ingest-jsonl)"
  output_path="$dir/unified.jsonl"
  summary_path="$dir/summary.json"

  run ./data-refinery ingest \
    --config examples/smoke/ingest-config.json \
    --approved-output-root "$RUNTIME_ROOT" \
    --log-path "$dir/logs" \
    --output-path "$output_path" \
    --stats-output-path "$summary_path"

  [ "$status" -eq 0 ]
  [ -f "$output_path" ]
  [ "$(wc -l < "$output_path")" -eq 12 ]
  assert_file_contains "$output_path" '"Id":"ing-jsonl-1"'
}

@test "ingest xml input normalizes repeated records via xmlRecordPath" {
  local dir output_path summary_path
  dir="$(runtime_dir ingest-xml-only)"
  output_path="$dir/unified.ndjson"
  summary_path="$dir/summary.json"

  run ./data-refinery ingest \
    --path examples/smoke/ingest/contacts.xml \
    --mapping-file examples/smoke/ingest-mappings.yaml \
    --approved-output-root "$RUNTIME_ROOT" \
    --log-path "$dir/logs" \
    --output-path "$output_path" \
    --stats-output-path "$summary_path" \
    --require-mappings

  [ "$status" -eq 0 ]
  assert_output_contains 'Files processed: 1'
  assert_output_contains 'Rows written: 2'
  [ "$(json_value "$summary_path" rowsWritten)" = "2" ]
  [ "$(wc -l < "$output_path")" -eq 2 ]
  assert_file_contains "$output_path" '"Id":"ing-xml-1"'
  assert_file_contains "$output_path" '"DataSource":"xml-smoke"'
}

@test "ingest csv output works for scalar-only mixed inputs including xml" {
  run ./data-refinery ingest --config examples/smoke/ingest-scalar-config.json

  [ "$status" -eq 0 ]
  assert_output_contains 'Files processed: 6'
  assert_output_contains 'Rows written: 12'
  [ -f "$REPO_ROOT/.tmp/smoke-cli/ingest-scalar/unified.csv" ]
  [ "$(wc -l < "$REPO_ROOT/.tmp/smoke-cli/ingest-scalar/unified.csv")" -eq 13 ]
  assert_file_contains "$REPO_ROOT/.tmp/smoke-cli/ingest-scalar/unified.csv" 'scalar-xlsx-1'
  assert_file_contains "$REPO_ROOT/.tmp/smoke-cli/ingest-scalar/unified.csv" 'scalar-xml-2'
}

@test "rewrite preview leaves checked-in json jsonl ndjson fixtures unchanged" {
  local before_json before_jsonl before_ndjson
  before_json="$(cksum examples/smoke/rewrite/source/orders.json)"
  before_jsonl="$(cksum examples/smoke/rewrite/source/orders.jsonl)"
  before_ndjson="$(cksum examples/smoke/rewrite/source/orders.ndjson)"

  for source_path in \
    examples/smoke/rewrite/source/orders.json \
    examples/smoke/rewrite/source/orders.jsonl \
    examples/smoke/rewrite/source/orders.ndjson
  do
    run ./data-refinery rewrite \
      --path "$source_path" \
      --mode preview \
      --log-path "$RUNTIME_ROOT/rewrite-preview-json/logs" \
      --top-level-key customer_id \
      --top-level-vals cust-delete \
      --array-key line_items \
      --array-del-key item_id \
      --array-del-vals sku-remove
    [ "$status" -eq 0 ]
    assert_output_contains 'Rewrite complete (preview).'
    assert_output_contains 'Lines deleted: 1'
    assert_output_contains 'Lines modified: 1'
  done

  [ "$before_json" = "$(cksum examples/smoke/rewrite/source/orders.json)" ]
  [ "$before_jsonl" = "$(cksum examples/smoke/rewrite/source/orders.jsonl)" ]
  [ "$before_ndjson" = "$(cksum examples/smoke/rewrite/source/orders.ndjson)" ]
}

@test "rewrite apply updates disposable json jsonl ndjson copies and writes backups" {
  local ext dir source_copy

  for ext in json jsonl ndjson
  do
    dir="$(runtime_dir rewrite-apply-$ext)"
    mkdir -p "$dir/data" "$dir/logs" "$dir/backups"
    source_copy="$dir/data/orders.$ext"
    cp "$REPO_ROOT/examples/smoke/rewrite/source/orders.$ext" "$source_copy"

    run ./data-refinery rewrite \
      --app-config examples/smoke/app-config.json \
      --approved-output-root "$dir" \
      --path "$source_copy" \
      --mode apply \
      --log-path "$dir/logs" \
      --backup-dir "$dir/backups" \
      --update-key customer_id \
      --update-old-value cust-123 \
      --update-new-value cust-999 \
      --update-id-key id \
      --update-id-vals examples/smoke/rewrite/update-id-values.csv

    [ "$status" -eq 0 ]
    assert_output_contains 'Rewrite complete (apply).'
    assert_output_contains 'Files modified: 1'
    assert_output_contains 'Lines updated: 1'
    assert_file_contains "$source_copy" '"customer_id":"cust-999"'
    ! grep -Fq '"customer_id":"cust-123"' "$source_copy"
    find "$dir/backups" -type f | grep -q .
  done
}

@test "rewrite preview leaves checked-in xml fixture unchanged" {
  local before_xml
  before_xml="$(cksum examples/smoke/rewrite/source/orders.xml)"

  run ./data-refinery rewrite --config examples/smoke/rewrite-preview-xml.json

  [ "$status" -eq 0 ]
  assert_output_contains 'Rewrite complete (preview).'
  assert_output_contains 'Lines deleted: 1'
  assert_output_contains 'Lines modified: 1'
  [ "$before_xml" = "$(cksum examples/smoke/rewrite/source/orders.xml)" ]
}

@test "rewrite apply updates a disposable xml copy and writes backups" {
  local dir source_copy
  dir="$(runtime_dir rewrite-apply-xml)"
  mkdir -p "$dir/data" "$dir/logs" "$dir/backups"
  source_copy="$dir/data/orders.xml"
  cp "$REPO_ROOT/examples/smoke/rewrite/source/orders.xml" "$source_copy"

  run ./data-refinery rewrite \
    --app-config examples/smoke/app-config.json \
    --approved-output-root "$dir" \
    --path "$source_copy" \
    --xml-record-path orders.order \
    --mode apply \
    --log-path "$dir/logs" \
    --backup-dir "$dir/backups" \
    --update-key customer_id \
    --update-old-value cust-123 \
    --update-new-value cust-999 \
    --update-id-key id \
    --update-id-vals examples/smoke/rewrite/update-id-values.csv

  [ "$status" -eq 0 ]
  assert_output_contains 'Rewrite complete (apply).'
  assert_output_contains 'Files modified: 1'
  assert_output_contains 'Lines updated: 1'
  assert_file_contains "$source_copy" '<customer_id>cust-999</customer_id>'
  ! grep -Fq '<customer_id>cust-123</customer_id>' "$source_copy"
  find "$dir/backups" -type f | grep -q .
}
