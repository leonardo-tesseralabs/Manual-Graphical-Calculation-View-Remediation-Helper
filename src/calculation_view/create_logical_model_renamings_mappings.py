#!/usr/bin/env python3
"""
Create Logical Model Renamings Mappings

Handles the --clmrm flag functionality for extracting renamings from calculation view
logical models where id != columnName, indicating field renamings.
"""

import os
import csv
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List

from .view_remediator_engine import (
    RemediationMappingEngine, get_calculation_views, select_calculation_views,
    select_single_view
)


def extract_logical_model_renamings(calculation_view_path: str, debug: bool = False) -> Dict[str, str]:
    """
    Extract field renamings from logical model where id != columnName

    Returns dictionary mapping original field names (columnName) to renamed field names (id)
    """
    renamings = {}

    try:
        tree = ET.parse(calculation_view_path)
        root = tree.getroot()

        # Look in logicalModel sections for attribute and measure definitions
        for logical_model in root.findall('.//logicalModel'):
            # Process attributes
            for attribute in logical_model.findall('.//attribute'):
                attr_id = attribute.get('id', '')

                # Look for keyMapping to get the original columnName
                key_mapping = attribute.find('.//keyMapping')
                if key_mapping is not None:
                    column_name = key_mapping.get('columnName', '')

                    # If id != columnName, this is a renaming
                    if attr_id and column_name and attr_id != column_name:
                        renamings[column_name] = attr_id
                        if debug:
                            print(f"  Attribute renaming: {column_name} -> {attr_id}")

            # Process measures
            for measure in logical_model.findall('.//measure'):
                measure_id = measure.get('id', '')

                # Look for measureMapping to get the original columnName
                measure_mapping = measure.find('.//measureMapping')
                if measure_mapping is not None:
                    column_name = measure_mapping.get('columnName', '')

                    # If id != columnName, this is a renaming
                    if measure_id and column_name and measure_id != column_name:
                        renamings[column_name] = measure_id
                        if debug:
                            print(f"  Measure renaming: {column_name} -> {measure_id}")

        if debug:
            print(f"Extracted {len(renamings)} field renamings from {calculation_view_path}")

    except Exception as e:
        if debug:
            print(f"Error extracting renamings from {calculation_view_path}: {e}")

    return renamings


def create_logical_model_renamings_mapping(engine: RemediationMappingEngine, debug: bool = False):
    """Create logical model renamings mapping for a selected calculation view"""

    # Find calculation views in cv directory
    script_dir = Path(__file__).parent.parent.parent  # Project root
    input_views = get_calculation_views(str(script_dir / "inputs/calculation_view/source"))

    if not input_views:
        print("No calculation views found in inputs/cv directory!")
        return

    # Display and select input view
    select_calculation_views(input_views, "Original")
    selected_view = select_single_view(input_views, "Select a calculation view to extract logical model renamings")

    print(f"\nExtracting logical model renamings from: {os.path.basename(selected_view)}")

    # Extract renamings from the selected view
    renamings = extract_logical_model_renamings(selected_view, debug)

    if not renamings:
        print("No logical model renamings found (all id values match columnName values)")
        return

    # Prepare output directory and filename
    output_dir = script_dir / "inputs/calculation_view/logical_model_renamings"
    output_dir.mkdir(exist_ok=True)

    # Generate output filename: remove .calculationview extension and add -lmrm.csv
    view_basename = os.path.basename(selected_view)
    if view_basename.endswith('.calculationview'):
        view_basename = view_basename[:-len('.calculationview')]
    output_filename = f"{view_basename}-lmrm.csv"
    output_path = output_dir / output_filename

    # Write renamings to CSV file
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Original', 'Renamed'])  # Header row

            # Sort by original field name for consistent output
            for original, renamed in sorted(renamings.items()):
                writer.writerow([original, renamed])

        print(f"\nLogical model renamings mapping created: {output_path}")
        print(f"Found {len(renamings)} field renamings:")

        for original, renamed in sorted(renamings.items()):
            print(f"  {original} -> {renamed}")

    except Exception as e:
        print(f"Error writing renamings file: {e}")
        return

    print(f"\nFile saved to: {output_path}")