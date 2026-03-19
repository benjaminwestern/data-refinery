# Advanced De-duplication Features

This document describes the advanced features added to the de-duplication system.

## New Features

### 1. Enhanced Configuration System

The system now supports advanced configuration through the `advanced` section in your config file:

```json
{
  "advanced": {
    "searchTargets": [...],
    "hashingStrategy": {...},
    "deletionRules": [...],
    "schemaDiscovery": {...}
  }
}
```

### 2. Schema Discovery

Automatically analyze and discover the schema of your JSON data:

```json
{
  "schemaDiscovery": {
    "enabled": true,
    "samplePercent": 0.1,
    "maxDepth": 5,
    "maxSamples": 50000,
    "outputFormats": ["json", "csv", "yaml"],
    "groupByFolder": true
  }
}
```

**Features:**
- Sample a percentage of your data for analysis
- Discover field types, occurrence rates, and examples
- Export schema in JSON, CSV, or YAML formats
- Group schema analysis by folder for comparative analysis

### 3. Advanced Search Engine

Search for specific values in complex nested structures:

```json
{
  "searchTargets": [
    {
      "name": "target_ids",
      "type": "direct",
      "path": "id",
      "targetValues": ["123", "1234", "12345"],
      "caseSensitive": false
    },
    {
      "name": "line_item_ids", 
      "type": "nested_array",
      "path": "line-items[*].line-item-id",
      "targetValues": ["li-001", "li-002"],
      "caseSensitive": false
    }
  ]
}
```

**Search Types:**
- `direct`: Simple key-value matching
- `nested_array`: Search within arrays
- `nested_object`: Deep object traversal
- `jsonpath`: JSONPath-like queries

### 4. Selective Hashing

Choose which fields to include in deduplication hashing:

```json
{
  "hashingStrategy": {
    "mode": "exclude_keys",
    "excludeKeys": ["version-id", "updated-at", "timestamp"]
  }
}
```

**Modes:**
- `full_row`: Hash entire row (default)
- `selective`: Hash only specified keys
- `exclude_keys`: Hash everything except specified keys

### 5. Deletion and Purging

Define rules for what to do with matched data:

```json
{
  "deletionRules": [
    {
      "searchTarget": "target_ids",
      "action": "delete_row",
      "outputPath": "deleted_rows.jsonl"
    },
    {
      "searchTarget": "line_item_ids",
      "action": "delete_matches", 
      "outputPath": "cleaned_data.jsonl"
    }
  ]
}
```

**Actions:**
- `delete_row`: Remove entire rows containing matches
- `delete_matches`: Remove only matching elements
- `mark_for_deletion`: Add metadata marking for deletion

## Usage Examples

### Example 1: Target ID Deletion

Delete rows containing specific IDs:

```json
{
  "advanced": {
    "searchTargets": [
      {
        "name": "target_ids",
        "type": "direct", 
        "path": "id",
        "targetValues": ["123", "1234", "12345"]
      }
    ],
    "deletionRules": [
      {
        "searchTarget": "target_ids",
        "action": "delete_row",
        "outputPath": "deleted_rows.jsonl"
      }
    ]
  }
}
```

### Example 2: Line Item Purging

Remove specific line items from arrays:

```json
{
  "advanced": {
    "searchTargets": [
      {
        "name": "line_item_ids",
        "type": "nested_array",
        "path": "line-items[*].line-item-id", 
        "targetValues": ["li-001", "li-002", "li-003", "..."]
      }
    ],
    "deletionRules": [
      {
        "searchTarget": "line_item_ids",
        "action": "delete_matches",
        "outputPath": "cleaned_data.jsonl"
      }
    ]
  }
}
```

### Example 3: Schema Discovery Only

Analyze data structure without processing:

```json
{
  "checkKey": false,
  "checkRow": false,
  "advanced": {
    "schemaDiscovery": {
      "enabled": true,
      "samplePercent": 0.05,
      "maxDepth": 10,
      "outputFormats": ["json", "csv"],
      "groupByFolder": true
    }
  }
}
```

### Example 4: Selective Deduplication

Hash only relevant fields for deduplication:

```json
{
  "advanced": {
    "hashingStrategy": {
      "mode": "exclude_keys",
      "excludeKeys": ["version-id", "updated-at", "timestamp", "last-modified"]
    }
  }
}
```

## Output Files

The system now generates additional output files:

- `schema_report_TIMESTAMP.json` - Schema analysis in JSON format
- `schema_report_TIMESTAMP.csv` - Schema analysis in CSV format
- `search_results_TIMESTAMP.json` - Search results summary
- `search_target_NAME_TIMESTAMP.json` - Results for specific search targets
- `deletion_stats_TIMESTAMP.json` - Deletion operation statistics
- `deletion_summary_TIMESTAMP.txt` - Human-readable deletion summary

## Performance Considerations

- **Sampling**: Use appropriate sample percentages to balance accuracy with performance
- **Depth Limits**: Set reasonable max depth values for nested analysis
- **Memory Usage**: Large datasets may require tuning of `maxSamples` parameter
- **Workers**: Adjust worker count based on your system capabilities

## Configuration Validation

The system validates your configuration and will report errors for:
- Invalid search target types
- Missing required fields
- Circular references in deletion rules
- Invalid hashing strategies

## Migration from Legacy

Your existing configurations will continue to work. Advanced features are opt-in through the `advanced` section.

## Examples Directory

See the `examples/` directory for complete configuration examples:
- `advanced_config.json` - Full advanced configuration
- `schema_only_config.json` - Schema discovery only
- `selective_hash_config.json` - Selective hashing example
- `comprehensive_test.json` - End-to-end sample config for local `test_data/`
- `test_advanced.json` - Minimal advanced config for local `test_data/`
- `test_config.json` - Manual test config covering search, deletion, and schema output
- `test_full_advanced.json` - Compact full-feature config for local `test_data/`
