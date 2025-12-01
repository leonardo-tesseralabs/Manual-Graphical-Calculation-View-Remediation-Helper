#!/usr/bin/env python3
"""
View Remediator Engine

Core reusable logic for ECC to S/4HANA calculation view remediation.
Provides menu selection, multi-input selection, and common utilities.
"""

import os
import sys
import csv
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict, deque


@dataclass
class FieldMapping:
    """Represents a field mapping from ECC to S/4HANA"""
    ecc_table: str
    ecc_field: str
    s4_table: str
    s4_field: str
    flagged_for_review: bool = False


class RemediationMappingEngine:
    """Core engine for remediation mapping functionality"""

    def __init__(self, custom_tables_file: str, transparent_tables_file: str,
                 mappings_file: str, override_mappings_file: str = None):
        self.custom_tables = self.load_custom_tables(custom_tables_file)
        self.transparent_tables = self.load_transparent_tables(transparent_tables_file)
        self.field_mappings = self.load_field_mappings(mappings_file)

        # Apply override mappings if provided
        if override_mappings_file and os.path.exists(override_mappings_file):
            override_count = self.apply_override_mappings(override_mappings_file)
            print(f"Applied {override_count} override mappings")

        # Show summary
        print(f"Loaded {len(self.custom_tables)} custom table patterns")
        print(f"Loaded {len(self.transparent_tables)} transparent tables")
        print(f"Loaded {sum(len(mappings) for mappings in self.field_mappings.values())} field mappings for {len(self.field_mappings)} unique field combinations")

        # Create mapping from table names to their types
        mapping_tables = set()
        for mapping_list in self.field_mappings.values():
            for mapping in mapping_list:
                mapping_tables.add(mapping.ecc_table)
        print(f"M-TA contains {len(mapping_tables)} unique source tables from mappings")

    def load_custom_tables(self, file_path: str) -> List[str]:
        """Load custom table patterns from file"""
        tables = []
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        tables.append(line)
        except FileNotFoundError:
            print(f"Warning: Custom tables file not found: {file_path}")
        return tables

    def load_transparent_tables(self, file_path: str) -> Set[str]:
        """Load transparent tables from file"""
        tables = set()
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        tables.add(line)
        except FileNotFoundError:
            print(f"Warning: Transparent tables file not found: {file_path}")
        return tables

    def load_field_mappings(self, file_path: str) -> Dict[str, List[FieldMapping]]:
        """Load field mappings from CSV file"""
        mappings = defaultdict(list)
        try:
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ecc_table = row['ecc_table'].strip()
                    ecc_field = row['ecc_field'].strip()
                    s4_table = row['s4_table'].strip()
                    s4_field = row['s4_field'].strip()
                    flagged = row.get('FLAGGED_FOR_REVIEW', '').strip().upper() == 'TRUE'

                    mapping = FieldMapping(ecc_table, ecc_field, s4_table, s4_field, flagged)
                    key = f"{ecc_table}.{ecc_field}"
                    mappings[key].append(mapping)
        except FileNotFoundError:
            print(f"Error: Field mappings file not found: {file_path}")
            sys.exit(1)
        return dict(mappings)

    def apply_override_mappings(self, file_path: str) -> int:
        """Apply override mappings, replacing existing ones"""
        override_count = 0
        try:
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ecc_table = row['ecc_table'].strip()
                    ecc_field = row['ecc_field'].strip()
                    s4_table = row['s4_table'].strip()
                    s4_field = row['s4_field'].strip()
                    flagged = row.get('FLAGGED_FOR_REVIEW', '').strip().upper() == 'TRUE'

                    mapping = FieldMapping(ecc_table, ecc_field, s4_table, s4_field, flagged)
                    key = f"{ecc_table}.{ecc_field}"

                    # Override existing mappings
                    self.field_mappings[key] = [mapping]
                    override_count += 1
        except FileNotFoundError:
            print(f"Warning: Override mappings file not found: {file_path}")
        return override_count

    def extract_output_columns(self, calculation_view_path: str) -> Tuple[Set[str], Set[str]]:
        """Extract attribute and measure names from a calculation view"""
        attributes = set()
        measures = set()

        try:
            tree = ET.parse(calculation_view_path)
            root = tree.getroot()

            # Look for logicalModel to get output columns
            for logical_model in root.findall('.//logicalModel'):
                # Extract attributes
                for attr in logical_model.findall('.//attribute'):
                    attr_id = attr.get('id')
                    if attr_id:
                        attributes.add(attr_id)

                # Extract measures
                for measure in logical_model.findall('.//measure'):
                    measure_id = measure.get('id')
                    if measure_id:
                        measures.add(measure_id)

        except Exception as e:
            print(f"Error parsing {calculation_view_path}: {e}")

        return attributes, measures


def get_calculation_views(directory: str) -> List[str]:
    """Get list of calculation view files from directory"""
    views = []
    if os.path.exists(directory):
        for file in os.listdir(directory):
            if file.endswith('.calculationview'):
                views.append(os.path.join(directory, file))
    return sorted(views)


def select_calculation_views(views: List[str], view_type: str) -> List[str]:
    """Display menu and allow user to select calculation views"""
    if not views:
        print(f"No {view_type} calculation views found!")
        return []

    print(f"\nAvailable {view_type} Calculation Views:")
    for i, view in enumerate(views, 1):
        view_name = os.path.basename(view)
        print(f"{i}. {view_name} ({view})")

    return views


def select_single_view(views: List[str], prompt: str) -> str:
    """Select a single view from the list"""
    if not views:
        print("No views available!")
        sys.exit(1)

    while True:
        try:
            choice = int(input(f"\n{prompt} (number): ")) - 1
            if 0 <= choice < len(views):
                return views[choice]
            else:
                print(f"Please enter a number between 1 and {len(views)}")
        except (ValueError, EOFError):
            print("Please enter a valid number")
            sys.exit(1)


def select_multiple_views(views: List[str], num_inputs: int) -> List[str]:
    """Select multiple views from the list"""
    if not views:
        print("No views available!")
        sys.exit(1)

    selected_views = []

    for i in range(num_inputs):
        while True:
            try:
                choice = int(input(f"\nSelect input view {i+1} (number): ")) - 1
                if 0 <= choice < len(views):
                    selected_view = views[choice]
                    if selected_view in selected_views:
                        print("View already selected. Please choose a different view.")
                        continue
                    selected_views.append(selected_view)
                    break
                else:
                    print(f"Please enter a number between 1 and {len(views)}")
            except (ValueError, EOFError):
                print("Please enter a valid number")
                sys.exit(1)

    return selected_views


def select_directory(prompt: str = "INPUT", exclude_dirs: List[str] = None) -> str:
    """Allow user to select a directory from inputs/calculation_view/ subdirectories"""
    script_dir = Path(__file__).parent.parent.parent  # Project root
    inputs_dir = script_dir / "inputs" / "calculation_view"

    if not inputs_dir.exists():
        print(f"Error: calculation_view directory not found: {inputs_dir}")
        sys.exit(1)

    # Get all subdirectories, excluding specified directories
    if exclude_dirs is None:
        exclude_dirs = ['logical_model_renamings']

    subdirs = [d for d in inputs_dir.iterdir() if d.is_dir() and d.name not in exclude_dirs]

    if not subdirs:
        print(f"Error: No subdirectories found in {inputs_dir}")
        sys.exit(1)

    # Filter to only directories that contain calculation views
    dirs_with_views = []
    for subdir in sorted(subdirs):
        views = get_calculation_views(str(subdir))
        if views:
            dirs_with_views.append((subdir, len(views)))

    if not dirs_with_views:
        print(f"Error: No directories with calculation views found in {inputs_dir}")
        sys.exit(1)

    # Display menu
    print(f"\nAvailable {prompt} directories in inputs/calculation_view/:")
    for i, (directory, view_count) in enumerate(dirs_with_views, 1):
        dir_name = directory.name
        print(f"{i}. {dir_name} ({view_count} calculation view(s))")

    # Get user selection
    while True:
        try:
            choice = int(input(f"\nSelect {prompt} directory (number): ")) - 1
            if 0 <= choice < len(dirs_with_views):
                selected_dir = dirs_with_views[choice][0]
                return str(selected_dir)
            else:
                print(f"Please enter a number between 1 and {len(dirs_with_views)}")
        except (ValueError, EOFError, KeyboardInterrupt):
            print("\nOperation cancelled")
            sys.exit(1)


def setup_output_directory(script_dir: Path) -> Path:
    """Setup and return the reports output directory"""
    reports_dir = script_dir / "outputs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    return reports_dir


def validate_renamings_file(renamings_file: str, script_dir: Path) -> str:
    """Validate and return full path to renamings file"""
    renamings_dir = script_dir / "inputs/renamings"

    # If relative path given, assume it's in inputs/renamings
    if not os.path.isabs(renamings_file):
        full_renamings_path = renamings_dir / renamings_file
    else:
        full_renamings_path = Path(renamings_file)

    # Check if file exists
    if not full_renamings_path.exists():
        print(f"Error: Renamings file not found: {full_renamings_path}")
        if not os.path.isabs(renamings_file):
            print(f"Expected location: {renamings_dir}")
        sys.exit(1)

    return str(full_renamings_path)


def load_semantic_renamings(renamings_file: str, debug: bool = False) -> Dict[str, str]:
    """Load semantic renamings from CSV file"""
    semantic_renamings = {}

    if not renamings_file:
        return semantic_renamings

    if debug:
        print(f"Loading renamings from: {renamings_file}")

    try:
        with open(renamings_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                original = row.get('Original', '').strip()
                renamed = row.get('Renamed', '').strip()
                if original and renamed:
                    semantic_renamings[original] = renamed

        if debug:
            print(f"Loaded {len(semantic_renamings)} field renamings")
    except Exception as e:
        print(f"Error loading renamings file: {e}")
        sys.exit(1)

    return semantic_renamings


def extract_field_hidden_status(xml_file_path: str, debug: bool = False) -> Dict[str, bool]:
    """Extract field hidden status from calculation view XML"""
    hidden_status = {}

    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Look in logicalModel sections for attribute and measure definitions
        for logical_model in root.findall('.//logicalModel'):
            # Process attributes
            for attribute in logical_model.findall('.//attribute'):
                attr_id = attribute.get('id', '')
                hidden = attribute.get('hidden', 'false').lower() == 'true'
                if attr_id:
                    hidden_status[attr_id] = hidden

            # Process measures
            for measure in logical_model.findall('.//measure'):
                measure_id = measure.get('id', '')
                hidden = measure.get('hidden', 'false').lower() == 'true'
                if measure_id:
                    hidden_status[measure_id] = hidden

        if debug:
            hidden_count = sum(1 for h in hidden_status.values() if h)
            print(f"Extracted hidden status for {len(hidden_status)} fields ({hidden_count} hidden) from {xml_file_path}")

    except Exception as e:
        if debug:
            print(f"Error extracting hidden status from {xml_file_path}: {e}")

    return hidden_status


def extract_field_descriptions(xml_file_path: str, debug: bool = False) -> Dict[str, str]:
    """Extract field descriptions from the logicalModel section of a calculation view"""
    descriptions = {}

    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Look in logicalModel sections for attribute and measure descriptions
        for logical_model in root.findall('.//logicalModel'):
            # Process attributes
            for attribute in logical_model.findall('.//attribute'):
                attr_id = attribute.get('id', '')
                if attr_id:
                    # Look for descriptions element
                    desc_elem = attribute.find('.//descriptions')
                    if desc_elem is not None:
                        default_desc = desc_elem.get('defaultDescription', '')
                        if default_desc:
                            descriptions[attr_id] = default_desc

            # Process measures
            for measure in logical_model.findall('.//measure'):
                measure_id = measure.get('id', '')
                if measure_id:
                    # Look for descriptions element
                    desc_elem = measure.find('.//descriptions')
                    if desc_elem is not None:
                        default_desc = desc_elem.get('defaultDescription', '')
                        if default_desc:
                            descriptions[measure_id] = default_desc

        if debug:
            print(f"Extracted {len(descriptions)} field descriptions from {xml_file_path}")

    except Exception as e:
        if debug:
            print(f"Error extracting descriptions from {xml_file_path}: {e}")

    return descriptions