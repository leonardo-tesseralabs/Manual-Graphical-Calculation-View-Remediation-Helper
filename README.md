# [LEGACY] Graphical View Manual Remediation Helper

## THIS IS NOT MAINTAINED, ONLY HERE FOR REFERENCE RE: GENERATING STATUS REPORTS. WILL BE DEPRECATED

## Overview

This project provides Python helper scripts for **manual** remediation of SAP graphical calculation views during ECC to S/4HANA migrations. The tools assist with analyzing XML files, generating status reports, and applying descriptions and renamings based on string matching.

## File Structure

```
.
├── remediator.py                    # CLI wrapper script
├── src/
│   └── calculation_view/
│       ├── remediator.py                        # Main entry point
│       ├── view_remediator_engine.py            # Core engine and utilities
│       ├── remediation_report.py                # Report generation
│       ├── create_logical_model_renamings_mappings.py  # LMRM creation
│       ├── apply_logical_model_renamings_mappings.py   # LMRM application
│       └── apply_descriptions.py                # Description application
├── inputs/
│   ├── calculation_view/
│   │   ├── source/                  # Original ECC calculation views
│   │   ├── remediated/              # Remediated S/4HANA calculation views
│   │   └── logical_model_renamings/ # Logical model renamings extracted by --clmrm
│   ├── renamings/                   # Semantic field renaming CSV files
│   ├── view_mappings.yaml           # YAML configuration for batch report generation
│   ├── custom_tables.txt            # Custom table patterns
│   ├── transparent_tables.txt       # Transparent table list
│   ├── source-of-truth_mappings.csv # Field mappings (ECC → S/4HANA)
│   └── override_mappings.csv        # Override field mappings
├── outputs/
│   ├── calculation_view/
│   │   └── remediated/              # Output from --almrm and --ad (auto-created)
│   └── reports/                     # Generated Excel reports (auto-created)
├── agent_context/                   # Agent context files
└── legacy/                          # Legacy code files
```

## Helper Scripts

### 1. Core Engine (`view_remediator_engine.py`)

Provides reusable core functionality for all remediation tools.

**Key Classes**:
- `RemediationMappingEngine`: Main engine class that loads mappings and processes calculation views
- `FieldMapping`: Data class representing ECC→S/4HANA field mappings

**Key Functions**:
- `get_calculation_views()`: Find calculation view files in directories
- `select_single_view()` / `select_multiple_views()`: Interactive view selection
- `extract_field_descriptions()`: Parse field descriptions from XML
- `extract_field_hidden_status()`: Parse field hidden status from XML
- `load_semantic_renamings()`: Load CSV-based field renamings
- `validate_renamings_file()`: Validate renamings file paths

**Usage Pattern**:
```python
from view_remediator_engine import RemediationMappingEngine

# Initialize engine
engine = RemediationMappingEngine(
    custom_tables_file, transparent_tables_file,
    mappings_file, override_mappings_file
)

# Extract view data
attributes, measures = engine.extract_output_columns(view_path)
```

### 2. Report Generator (`remediation_report.py`)

Generates Excel reports analyzing calculation view XML files (--report flag).

**Key Functions**:
- `generate_remediation_report()`: Main entry point for report generation
- `process_field_comparison()`: Compare individual fields between source and target
- `generate_detailed_view_comparison()`: Create detailed comparison for single view
- `generate_union_view_comparison()`: Create union analysis for multi-input scenarios
- `resolve_recursive_data_sources()`: Recursively resolve calculation view dependencies
- `trace_field_lineage()`: Trace field lineage through calculation view hierarchy to source table
- `extract_field_source_lineage()`: Extract TABLE.FIELD lineage for all fields in a view

**Report Features**:
- Field analysis: Direct matches, mappings, semantic renamings, type mismatches
- Source field lineage: Traces each field back to original source table (e.g., `BSEG.KUNNR`)
- Calculated column detection: Identifies fields calculated with formulas
- Recursive view expansion: Traces through nested calculation views to find root tables
- Hidden field detection: Extracts and compares `hidden="true"` attributes
- Multi-input support: Union analysis across multiple source views
- Excel output: Multiple sheets with auto-sizing and formatting

**Lineage Tracing**:
The lineage tracing functionality:
- Recursively follows field mappings through calculation view nodes
- Expands nested calculation views (e.g., `CV_BSEG_BASE` → `BSEG` table)
- Identifies calculated columns (marked as "Calculated Column, NodeName")
- Handles both attributes (`<keyMapping>`) and measures (`<measureMapping>`)
- Traces from logical model renamings back to original source tables

### 3. Main Entry Point (`remediator.py`)

Command-line interface that orchestrates the helper scripts.

**Available Flags**:
- `--report`: Generate comprehensive Excel reports (requires `--output` or `--batch`)
- `--output file.xlsx`: Specify output file path (automatically saved to `outputs/reports/` directory)
- `--inputs N`: Select N input views for multi-view analysis
- `--batch`: Process all calculation views in a directory using YAML configuration (use with `--report`)
- `--renamings file.csv`: Apply semantic field renamings from `inputs/renamings/`
- `--clmrm`: Create logical model renamings mappings from calculation view logical models
- `--almrm`: Apply logical model renamings mappings with interactive remediation
- `--ad`: Apply descriptions from source views to remediated views
- `--debug`: Enable verbose output with detailed parsing information

**Output Flags**:
- `--output` / `-o`: Required for `--report` (unless using `--batch`). Supports `.csv` and `.xlsx` extensions
  - Files are automatically saved to the `outputs/reports/` subdirectory
  - CSV files are automatically converted to Excel format for multi-sheet support

### 4. Logical Model Renamings Applier (`apply_logical_model_renamings_mappings.py`)

Applies logical model renamings to remediated views based on string matching (--almrm flag).

**Key Functions**:
- `apply_logical_model_renamings_mappings()`: Main workflow orchestrator
- `trace_field_lineage()`: Trace field through calculation view hierarchy
- `resolve_conflict_menu()`: Interactive menu for conflict resolution
- `resolve_warning_menu()`: Interactive menu for warning resolution
- `apply_renaming_to_xml()`: Modify XML to apply renamings

**Workflow**:
1. **Select Source View**: Choose ECC view from `inputs/cv`
2. **Generate LMRM CSV**: Extract logical model renamings (where `id != columnName`)
3. **Select Remediated View**: Choose S4 view from `inputs/cv_remediated`
4. **Verification**: Apply ECC→S4 field mappings and categorize:
   - ✓ **Success**: Clean 1-to-1 mappings
   - ⚠️ **Conflicts**: Multiple ECC fields → same S4 field with different renamings
   - ⚠️ **Warnings**: One ECC field → multiple S4 fields
   - ✗ **Missing**: No S4 field found
5. **Interactive Resolution**: User selects which renamings to apply for conflicts/warnings
6. **Apply Renamings**: Copies view to `outputs/cv_remediated/` and modifies XML

**Example**:
```bash
# Basic usage
python remediator.py --almrm

# With semantic renamings for better matching
python remediator.py --almrm --renamings acdoca_base.csv

# With debug output
python remediator.py --almrm --renamings acdoca_base.csv --debug
```

**Extension Pattern**:
```python
# Add new flag implementation
if args.new_flag:
    from new_module import handle_new_flag
    handle_new_flag(engine, args)
```

### 5. Description Applier (`apply_descriptions.py`)

Copies field descriptions from source ECC views to remediated S/4HANA views based on string matching (--ad flag).

**Key Functions**:
- `apply_descriptions()`: Main workflow orchestrator
- `collect_description_mappings()`: Extract valid description mappings using report logic
- `apply_description_to_xml()`: Modify XML to apply descriptions using regex

**Workflow**:
1. **Select Source View(s)**: Choose one or more ECC views from `inputs/cv` (supports `--inputs`)
2. **Select Remediated View**: Choose S4 view from `inputs/cv_remediated`
3. **Generate Internal Report**: Uses report generation logic to map fields
4. **Collect Mappings**: Extracts valid description mappings where both source and target exist
   - Uses union sheet if multiple inputs
   - Only includes fields where descriptions differ
   - Empty source descriptions are valid (clears target description)
5. **Display Mappings**: Prints all description changes to user for review
6. **User Confirmation**: Prompts user to confirm application
7. **Apply Descriptions**: Copies view to `outputs/cv_remediated/` and modifies XML using regex

**Features**:
- Regex-based XML modification: Preserves original XML formatting (CRLF, namespaces, entities)
- Multi-input support: Can use union of descriptions from multiple source views
- Semantic renamings: Supports `--renamings` for better field matching
- Safe XML handling: Escapes special characters (`&`, `<`, `>`, `"`)
- Empty description support: Can apply empty descriptions to clear target descriptions

**Example**:
```bash
# Basic usage with single source view
python remediator.py --ad

# With multiple source views (uses union)
python remediator.py --ad --inputs 3

# With semantic renamings for better field matching
python remediator.py --ad --renamings acdoca_base.csv

# With debug output
python remediator.py --ad --inputs 2 --renamings acdoca_base.csv --debug
```

**Output Example**:
```
================================================================================
Found 5 description mapping(s) to apply:
================================================================================

  ATTRIBUTE: PROFIT_CTR_ID
    Current: ''
    New:     'Profit Center'

  ATTRIBUTE: CMPNY_ID
    Current: ''
    New:     'Company Code'

  MEASURE: LOCAL_CURR_AMT
    Current: ''
    New:     'Amount in Local Currency'

================================================================================
Apply these descriptions to a copy of CV_FSA_TRANSIT_DTL_ACDOCA_BASE_.calculationview? (y/n):
```

## Data Structures

### Field Mapping Format (`source-of-truth_mappings.csv`)
```csv
ecc_table,ecc_field,s4_table,s4_field,FLAGGED_FOR_REVIEW
BSEG,KUNNR,ACDOCA,KUNNR,FALSE
```

### Semantic Renamings Format (`inputs/renamings/*.csv`)
```csv
Original,Renamed
KUNNR_BSEG,KUNNR
BUKRS_COEP,RBUKRS
```

### Calculation View XML Structure
The engine parses SAP calculation view XML files to extract:
- **Attributes/Measures**: Field definitions in `<logicalModel>`
- **Descriptions**: `defaultDescription` attributes
- **Hidden Status**: `hidden="true"` attributes
- **Data Sources**: Referenced tables and views

## Report Output Structure

### Excel Sheets Generated:
1. **Summary**: Overview with field counts and hidden field statistics
2. **Source_ECC_View_N**: Detailed field comparison for each input view
3. **Union_All_Sources**: Combined analysis (when multiple inputs used)

### Column Structure (per detail sheet):
```
Source View Columns:
- COLUMN_NAME: Field name in the source (ECC) view
- COLUMN_DESCRIPTION: Field description (longest non-empty in union)
- SOURCE_FIELD: Original source (e.g., "BSEG.KUNNR", "Calculated Column, Aggregation_3")
                In union: list format ['BSEG.KUNNR', 'COEP.BUKRS', 'COBK.CPUDT']
- COLUMN_TYPE: ATTRIBUTE or MEASURE
- HIDDEN? (Y/N): Whether field is hidden in the view
                 In union: list format ['Y', 'N', 'Y']

Target View Columns:
- COLUMN_NAME: Field name in the target (S/4HANA) view
- COLUMN_DESCRIPTION: Field description
- SOURCE_FIELD: Original source (e.g., "ACDOCA.RCLNT", "Calculated Column, Aggregation_1")
- COLUMN_TYPE: ATTRIBUTE or MEASURE
- HIDDEN? (Y/N): Whether field is hidden in the view

Check Columns:
- RENAME (Y/N): Whether field was renamed between source and target
- MATCHING_DESCRIPTION (Y/N/N/A): Whether descriptions match
                                   Y if both match (including both empty)
                                   N if differ
                                   N/A if no valid comparison (missing source or target)
- MATCHING_TYPE (Y/N/TYPE_MISMATCH/N/A): Whether types match
- MATCHING_HIDDEN (Y/N/N/A): Whether hidden status matches
                              In union: N (not hidden) if ANY source view is not hidden
```

**SOURCE_FIELD Lineage Examples**:
- Single view: `BSEG.KUNNR` - Field sourced from BSEG table
- Single view: `Calculated Column, Aggregation_3` - Calculated field defined with formula
- Union view: `['BSEG.KUNNR', 'COEP.BUKRS', 'COBK.CPUDT']` - Field sourced from different tables in each view

## Adding New Functionality

### Pattern 1: New Command Flag

1. **Create new module** (e.g., `new_feature.py`):
```python
from view_remediator_engine import RemediationMappingEngine

def handle_new_feature(engine: RemediationMappingEngine, args):
    """Implement new feature logic"""
    # Use engine.extract_output_columns(), etc.
    pass
```

2. **Add to main entry point** (`remediator.py`):
```python
parser.add_argument('--new-feature', action='store_true',
                   help='Description of new feature')

if args.new_feature:
    from new_feature import handle_new_feature
    handle_new_feature(engine, args)
```

### Pattern 2: Extending Report Analysis

Modify `remediation_report.py` functions:
- `process_field_comparison()`: Add new field comparison logic
- `generate_detailed_view_comparison()`: Add new sheet columns
- Add new utility functions to `view_remediator_engine.py` if reusable

### Pattern 3: New Input Format

Add parser functions to `view_remediator_engine.py`:
```python
def load_new_input_format(file_path: str) -> Dict:
    """Load and parse new input format"""
    pass
```

## Design Principles

1. Separation of concerns: Engine logic separate from UI/output formatting
2. Reusability: Core functions designed for multiple use cases
3. Independence: Modular files work independently
4. Extensibility: Add new flags and features as needed
5. Legacy preservation: Original `remediation_mapping_engine.py` untouched

## Implementation Notes

### Report Generator
- Recursive data source resolution: Parses `<DataSource>` elements and extracts table names from `<columnObject>` elements
- Lineage tracing: Traces fields backwards through calculation view node hierarchy to original source tables
- Measure handling: Correctly uses `<measureMapping>` for measures vs `<keyMapping>` for attributes
- Calculated column detection: Checks `<calculatedViewAttribute>` before input mappings
- Union sheet support: Shows list format for SOURCE_FIELD and HIDDEN status when multiple inputs are used; selects longest non-empty description

### ALMRM Implementation
- Workflow: Source view selection, LMRM generation, remediated view selection, field mapping, interactive conflict/warning resolution
- Mapping logic: Handles 0, 1, or multiple S4 field mappings; detects conflicts and warnings
- XML preservation: Uses regex-based string replacement instead of ElementTree to preserve CRLF line endings, namespace prefixes, and entity encoding
- Duplicate prevention: Checks if target `id` already exists before applying renaming to avoid creating duplicate attribute/measure definitions

### Description Applier
- Workflow: Source view selection (single or multiple), remediated view selection, field mapping via internal report generation, user confirmation, XML modification
- XML modification: Regex-based approach preserving original formatting; handles both `<descriptions/>` and `<descriptions defaultDescription="..."/>`; escapes XML special characters
- Union logic: Selects longest non-empty description when multiple inputs are provided

## Field Analysis Logic

### Type Mismatch Detection
Detects when fields change type between source and target (Attribute → Measure or Measure → Attribute). Marked as `TYPE_MISMATCH` in reports.

### Semantic Renamings
CSV-based field renamings applied when exact mappings don't exist. Handles edge case of renamed columns in the logical model.
- Applied after direct field matching but before marking as missing
- Example: The ECC field BSEG.KUNNR maps to the S4 field ACDOCA.KUNNR per source-of-truth mappings. If KUNNR was renamed to KUNNR_BSEG in the unremediated ECC view's logical model, the tool won't automatically recognize it. However, if KUNNR_BSEG is mapped to KUNNR in the semantic renamings CSV, the tool will correctly parse it.

### Hidden Field Analysis
Extracts `hidden="true"` from calculation view XML, compares hidden status between source and target, and includes hidden field counts in summary statistics.

## File Organization and Output

### Directory Management
- Reports: Output files saved to `outputs/reports/` subdirectory
- Renamings: Semantic renaming files expected in `inputs/renamings/`
- Output directories created automatically if they don't exist

### File Formats
- Input: Supports relative paths (e.g., `report.xlsx`) or absolute paths
- Output: Files redirected to `outputs/reports/filename.xlsx`
- Format conversion: `.csv` extensions automatically converted to `.xlsx` for multi-sheet support

## Batch Mode

Batch mode processes multiple calculation views automatically based on a YAML configuration file. The configuration maps input (ECC) views to their corresponding remediated (S/4HANA) views.

### Configuration File (`inputs/view_mappings.yaml`)

The YAML file defines mappings between input and remediated views. View names should NOT include the `.calculationview` extension.

**Format**:
```yaml
mappings:
  remediated_view_name:
    - input_view_1
    - input_view_2
```

**Example**:
```yaml
mappings:
  CV_FSA_TRANSIT_DTL_ACDOCA_BASE:
    - CV_FSA_TRANSIT_DTL_BSEG_BASE
    - CV_FSA_TRANSIT_DTL_COEP_BASE
    - CV_FSA_TRANSIT_DTL_COEP_BASE_REV

  CV_FSA_TRANSIT_DTL_RE_BASE:
    - CV_FSA_TRANSIT_DTL_BSEG_RE_BASE
```

### Usage

```bash
# Basic batch processing
python remediator.py --report --batch

# With semantic renamings
python remediator.py --report --batch --renamings acdoca_base.csv

# With debug output
python remediator.py --report --batch --debug
```

### Workflow

1. User selects an input directory from `inputs/` subdirectories
2. Tool loads mappings from `inputs/view_mappings.yaml`
3. For each mapping, generates a report comparing input view(s) to remediated view
4. Reports are named after the remediated view (e.g., `CV_FSA_TRANSIT_DTL_ACDOCA_BASE.xlsx`)
5. All reports are saved to `outputs/reports/` directory

### Multi-Input Reports

When multiple input views map to one remediated view, the generated report includes:
- Summary sheet with statistics for all input views
- Individual comparison sheets for each input view (`Source_ECC_View_1`, `Source_ECC_View_2`, etc.)
- Union sheet combining all input views (if multiple inputs exist)

### Example Usage

**Generate Reports**:
```bash
# Interactive mode with single view
python remediator.py --report --output my_report.xlsx

# Interactive mode with multiple input views
python remediator.py --report --output my_report.xlsx --inputs 2

# Interactive mode with semantic renamings
python remediator.py --report -o report.xlsx --inputs 1 --renamings acdoca_base.csv

# Batch mode processing all configured mappings
python remediator.py --report --batch --renamings acdoca_base.csv
```

**Create Logical Model Renamings**:
```bash
# Extract renamings from a calculation view
python remediator.py --clmrm

# With debug output
python remediator.py --clmrm --debug
```

**Apply Logical Model Renamings**:
```bash
# Interactive remediation workflow
python remediator.py --almrm

# With semantic renamings for better field matching
python remediator.py --almrm --renamings acdoca_base.csv

# With debug output to trace lineage
python remediator.py --almrm --renamings acdoca_base.csv --debug
```

**Apply Descriptions**:
```bash
# Basic usage with single source view
python remediator.py --ad

# With multiple source views (uses union for longest description)
python remediator.py --ad --inputs 3

# With semantic renamings for better field matching
python remediator.py --ad --renamings acdoca_base.csv

# With multiple inputs and debug output
python remediator.py --ad --inputs 2 --renamings acdoca_base.csv --debug
```

## Testing

**Test Report Generation**:
```bash
# Generate a test report with debug output
python remediator.py --report --output test.xlsx --inputs 2 --renamings acdoca_base.csv --debug

```

**Test LMRM Creation**:
```bash
python remediator.py --clmrm --debug
```

**Test LMRM Application**:
```bash
python remediator.py --almrm --renamings acdoca_base.csv --debug
```

**Test Description Application**:
```bash
python remediator.py --ad --inputs 2 --renamings acdoca_base.csv --debug
```
