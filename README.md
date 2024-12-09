# dbsync

A high-performance tool for synchronizing Databricks Unity Catalogs across environments, written in Rust.

## Overview

`dbsync` automates the process of synchronizing test catalogs (staging, development, etc.) with production catalogs in Databricks using SHALLOW CLONE operations. It leverages parallel processing to efficiently fetch Unity Catalog metadata and ensures data freshness based on configurable staleness thresholds.

## Features

- ⚡ High-performance parallel fetching of Unity Catalog metadata
- 🔄 Automated SHALLOW CLONE operations
- ⚙️ Configurable data staleness checks
- 📝 YAML-based configuration
- 🔄 Multiple catalog synchronization support

## Usage

```bash
dbsync -c <path/to/config.yaml> --num-request <NUM_PARALLEL_REQUESTS>
```

### Configuration

Create a YAML configuration file with the following structure:

```yaml
host: databricks-host.databricks.net
pat: TOKEN
catalogs:
  - catalog: prod
    pinned_catalogs:
      - staging
      - development
generation_config:
  max_staleness_duration_hours: 24
```

#### Configuration Parameters

- `host`: Your Databricks instance hostname
- `pat`: Personal Access Token for authentication
- `catalogs`: List of catalog configurations
  - `catalog`: Source catalog name (e.g., production)
  - `pinned_catalogs`: List of target catalogs to synchronize
- `generation_config`:
  - `max_staleness_duration_hours`: Maximum allowed time difference between source and target catalogs (in hours)
