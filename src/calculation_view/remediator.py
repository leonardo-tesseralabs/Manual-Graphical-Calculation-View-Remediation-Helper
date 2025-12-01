#!/usr/bin/env python3
"""
Main Entry Point for View Remediator

Uses the modular structure with view_remediator_engine.py and remediation_report.py
"""

import os
import sys
import argparse
from pathlib import Path

# Handle both direct script execution and package import
if __name__ == "__main__" and __package__ is None:
    # Add parent directory to path for direct script execution
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from src.calculation_view.view_remediator_engine import (
        RemediationMappingEngine, get_calculation_views, select_calculation_views,
        select_single_view, select_multiple_views, validate_renamings_file
    )
    from src.calculation_view.remediation_report import generate_remediation_report, generate_batch_remediation_reports
    from src.calculation_view.create_logical_model_renamings_mappings import create_logical_model_renamings_mapping
    from src.calculation_view.apply_logical_model_renamings_mappings import apply_logical_model_renamings_mappings
    from src.calculation_view.apply_descriptions import apply_descriptions
else:
    from .view_remediator_engine import (
        RemediationMappingEngine, get_calculation_views, select_calculation_views,
        select_single_view, select_multiple_views, validate_renamings_file
    )
    from .remediation_report import generate_remediation_report, generate_batch_remediation_reports
    from .create_logical_model_renamings_mappings import create_logical_model_renamings_mapping
    from .apply_logical_model_renamings_mappings import apply_logical_model_renamings_mappings
    from .apply_descriptions import apply_descriptions


def main():
    """Main function with argument parsing and delegation to appropriate modules"""

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Remediation Mapping Engine for ECC to S/4HANA calculation view migration')

    parser.add_argument('--remediated', '-r', action='store_true',
                       help='Show remediated calculation views and their structure')
    parser.add_argument('--debug', '-d', action='store_true',
                       help='Enable debug output')
    parser.add_argument('--compare', '-c', action='store_true',
                       help='Compare original and remediated calculation views')
    parser.add_argument('--similarity', '-s', action='store_true',
                       help='Show similarity analysis between views')
    parser.add_argument('--output', '-o', type=str,
                       help='Output file for comparison results')
    parser.add_argument('--sources', action='store_true',
                       help='Show data sources used by calculation view')
    parser.add_argument('--inputs', '-i', type=int,
                       help='Number of input views to select for multi-view comparison')
    parser.add_argument('--report', action='store_true',
                       help='Generate a comprehensive CSV report of the remediation (requires --output with .csv suffix)')
    parser.add_argument('--renamings', type=str,
                       help='CSV file containing additional semantic renamings for report generation')
    parser.add_argument('--clmrm', action='store_true',
                       help='Create logical model renamings mappings - extract field renamings from calculation view logical model')
    parser.add_argument('--almrm', action='store_true',
                       help='Apply logical model renamings mappings - apply previously created logical model renamings to calculation views')
    parser.add_argument('--ad', action='store_true',
                       help='Apply descriptions - copy field descriptions from source views to remediated views')
    parser.add_argument('--batch', action='store_true',
                       help='Batch mode - process all calculation views in a directory (use with --report)')
    args = parser.parse_args()

    # Validate report flag requirements
    if args.report:
        # In batch mode, --output is not required (filenames are auto-generated)
        if not args.batch:
            if not args.output:
                print("Error: --report requires --output (-o) flag with a .csv or .xlsx file specified")
                print("       (use --batch to process multiple files without specifying --output)")
                sys.exit(1)
            if not (args.output.endswith('.csv') or args.output.endswith('.xlsx')):
                print("Error: --report requires output file to have .csv or .xlsx suffix")
                sys.exit(1)
        else:
            # In batch mode, --output should not be specified
            if args.output:
                print("Warning: --output flag is ignored in batch mode (filenames are auto-generated)")

        if args.renamings:
            project_root = Path(__file__).parent.parent.parent
            args.renamings = validate_renamings_file(args.renamings, project_root)

    # Validate apply descriptions flag requirements
    if args.ad:
        if args.renamings:
            project_root = Path(__file__).parent.parent.parent
            args.renamings = validate_renamings_file(args.renamings, project_root)

    # File paths for inputs - use project root, not script directory
    script_dir = Path(__file__).parent.parent.parent  # Go up to project root
    custom_tables_file = script_dir / "inputs/custom_tables.txt"
    transparent_tables_file = script_dir / "inputs/transparent_tables.txt"
    mappings_file = script_dir / "inputs/source-of-truth_mappings.csv"
    override_mappings_file = script_dir / "inputs/override_mappings.csv"

    # Check if input files exist
    for file_path, name in [(custom_tables_file, "Custom tables"),
                           (transparent_tables_file, "Transparent tables"),
                           (mappings_file, "Field mappings")]:
        if not file_path.exists():
            print(f"Error: {name} file not found: {file_path}")
            sys.exit(1)

    # Initialize engine
    if args.debug:
        print("Initializing Remediation Mapping Engine...")
    engine = RemediationMappingEngine(
        str(custom_tables_file),
        str(transparent_tables_file),
        str(mappings_file),
        str(override_mappings_file) if override_mappings_file.exists() else None
    )

    # Handle report mode
    if args.report:
        if args.batch:
            # Batch mode: process all views in a directory
            # num_inputs is ignored in batch mode (each view is processed individually)
            num_inputs = 1
            generate_batch_remediation_reports(engine, num_inputs, args.renamings, args.debug)
        else:
            # Normal mode: process selected views
            # Default to single input if --inputs not specified
            num_inputs = args.inputs if args.inputs else 1
            generate_remediation_report(engine, num_inputs, args.output, args.renamings, args.debug)
        return

    # Handle create logical model renamings mappings mode
    if args.clmrm:
        create_logical_model_renamings_mapping(engine, args.debug)
        return

    # Handle apply logical model renamings mappings mode
    if args.almrm:
        # Validate renamings file if provided
        renamings_file = None
        if args.renamings:
            renamings_file = validate_renamings_file(args.renamings, script_dir)
        apply_logical_model_renamings_mappings(engine, renamings_file, args.debug)
        return

    # Handle apply descriptions mode
    if args.ad:
        # Default to single input if --inputs not specified
        num_inputs = args.inputs if args.inputs else 1
        apply_descriptions(engine, num_inputs, args.renamings, args.debug)
        return

    # For now, only --report, --clmrm, --almrm, and --ad flags are implemented in the modular version
    print("Only --report, --clmrm, --almrm, and --ad flags are currently implemented in the modular version.")
    print("For other functionality, please use the original remediation_mapping_engine.py")
    sys.exit(1)


if __name__ == "__main__":
    main()