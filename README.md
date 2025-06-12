# WAII Integration

This repository contains scripts integrating WAII (AI-based SQL assistant) and Keboola Connection.
In particular it pulls metadata from a specific Keboola project and adds it to semantic context. It can also delete semantic context based on saved statement ids (which are saved for each run of the adding semantic context).

## Overview

The WAII Integration project provides tools to:

1. Connect to WAII API using environment-based configuration
2. Import metadata from Keboola Connection into WAII semantic context
3. Manage semantic context statements efficiently
4. Test and evaluate the performance of WAII's SQL generation

## Project Workflow

This project performs the following end-to-end process:

1. **Keboola Metadata Extraction**: Uses the `KeboolaClient` to connect to Keboola Connection and extract metadata about tables, columns, and other objects
2. **Component Description Retrieval**: Fetches component descriptions from Keboola API using the `ComponentDescriptionManager`
3. **Metadata Processing**: Transforms Keboola metadata into a format suitable for WAII
4. **Semantic Context Creation**: Creates semantic statements based on best practices using the `WaiiSemanticContextManager`
5. **WAII Integration**: Adds the semantic statements to WAII's knowledge graph
6. **Statement ID Management**: Saves statement IDs to the `data/statement_ids` directory for future reference or deletion

## Key Components

### KeboolaMetadataCollector
Connects to Keboola and extracts metadata about tables, including:
- Table names and descriptions
- Column information
- Row counts
- Component lineage (which component created the table)
- Data freshness (last import/change dates)

### ComponentDescriptionManager
Retrieves human-readable descriptions of Keboola components to add meaningful context.

### WaiiSemanticContextManager
Handles all interactions with WAII:
- Creates semantic context statements from Keboola metadata
- Adds statements to WAII semantic context using best practices
- Manages statement IDs and tracking
- Implements semantic matching for more effective retrieval

## Requirements

- Python 3.10+
- WAII API access
- Keboola Connection access
- Environment configuration (see below)

## Installation

1. Clone this repository
2. Create a virtual environment: `python -m venv venv`
3. Activate the virtual environment: 
   - Windows: `virtualenv --download venv
   - Unix/Mac: `source venv/bin/activate`
4. Install dependencies: `pip install -e .`

## Configuration

Set the following environment variables (or use a `.env` file):

```
# WAII Configuration
WAII_API_URL=https://your-waii-instance.example.com
WAII_API_KEY=your-api-key
WAII_CONNECTION=your-connection-name

# Keboola Configuration
KEBOOLA_API_TOKEN=your-keboola-api-token
KEBOOLA_PROJECT_URL=https://connection.keboola.com/admin/projects/your-project-id
KEBOOLA_PROJECT_NAME=your-project-name
```

## Usage

### Quick Start with the Command-Line Script

For convenience, you can directly run the `semantic_context_add.py` script to add Keboola metadata from a specific project to WAII's semantic context:

```bash
# Make sure your .env file is configured with the required environment variables
cd src/keboola/waii-integration
python semantic_context_add.py
```

This script:
1. Reads configuration from environment variables
2. Connects to the specified Keboola project
3. Extracts metadata from tables
4. Retrieves component descriptions
5. Creates and adds semantic context statements to WAII
6. Saves the statement IDs for future reference

You can also specify a limit on the number of tables to process:

```bash
python semantic_context_add.py --limit 10
```

### Programmatic Workflow

For more advanced integration, you can use the components programmatically:

```python
from keboola_utils.keboola_client import KeboolaClient
from keboola_metadata_collector import KeboolaMetadataCollector
from waii_context_manager import WaiiSemanticContextManager

# Initialize Keboola client and collector
client = KeboolaClient(token, api_url)
collector = KeboolaMetadataCollector(api_token, project_url)

# Extract metadata from Keboola
metadata = collector.get_tables_metadata_sample()

# Initialize WAII manager
waii_manager = WaiiSemanticContextManager()

# Create semantic context statements
statements = waii_manager.create_semantic_context_statements(metadata)

# Add statements to WAII
waii_manager.add_to_semantic_context(statements)
```

## Project Structure

```
waii-integration/
├── src/
│   ├── keboola
│       ├── waii_integration
├── pyproject.toml                    # Project configuration
├── .env                              # Environment configuration (gitignored)
└── README.md                         # This file
```
