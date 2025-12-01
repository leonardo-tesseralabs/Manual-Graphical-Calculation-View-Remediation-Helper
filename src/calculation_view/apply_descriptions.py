#!/usr/bin/env python3
"""
Apply Descriptions

Handles the --ad flag functionality for applying field descriptions from
source ECC views to remediated S/4HANA calculation views.
"""

import os
import re
import shutil
from pathlib import Path
from typing import Dict, List, Tuple

from .view_remediator_engine import (
    RemediationMappingEngine, get_calculation_views, select_calculation_views,
    select_single_view, select_multiple_views, load_semantic_renamings,
    extract_field_descriptions
)
from .remediation_report import (
    resolve_recursive_data_sources, generate_detailed_view_comparison,
    generate_union_view_comparison
)


def apply_description_to_xml(xml_path: str, field_id: str, field_type: str, description: str, debug: bool = False) -> bool:
    """
    Apply a description to an attribute or measure in the XML file using regex.
    Preserves original XML formatting.

    Args:
        xml_path: Path to the XML file
        field_id: The id of the attribute or measure
        field_type: Either 'attribute' or 'measure'
        description: The description to apply
        debug: Enable debug output

    Returns: True if description was applied, False otherwise
    """
    try:
        # Read the file as text to preserve formatting
        with open(xml_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Escape special XML characters in description
        description_escaped = description.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')

        # Pattern to match the attribute/measure with this id and its descriptions element
        # We need to match:
        # <attribute id="FIELD_ID" ...>
        #   <descriptions/>  OR  <descriptions defaultDescription="..."/>
        #   ...
        # </attribute>

        # First, find the opening tag for this field
        field_pattern = f'<{field_type}\\s+id="{re.escape(field_id)}"[^>]*>'

        # Search for the field
        field_match = re.search(field_pattern, content)
        if not field_match:
            if debug:
                print(f"    Warning: Could not find {field_type} with id={field_id}")
            return False

        # Find the position where this field starts
        field_start = field_match.start()

        # Find the closing tag for this field
        closing_tag = f'</{field_type}>'
        field_end_match = re.search(re.escape(closing_tag), content[field_start:])
        if not field_end_match:
            if debug:
                print(f"    Warning: Could not find closing tag for {field_type} {field_id}")
            return False

        field_end = field_start + field_end_match.end()
        field_content = content[field_start:field_end]

        # Now within this field, find and replace the descriptions element
        # Match either <descriptions/> or <descriptions defaultDescription="..."/>
        descriptions_pattern = r'<descriptions(?:\s+defaultDescription="[^"]*")?\s*/>'

        if description:
            # Replace with description
            new_descriptions = f'<descriptions defaultDescription="{description_escaped}"/>'
        else:
            # Keep empty
            new_descriptions = '<descriptions/>'

        new_field_content = re.sub(descriptions_pattern, new_descriptions, field_content)

        if new_field_content == field_content:
            if debug:
                print(f"    Warning: Could not find descriptions element in {field_type} {field_id}")
            return False

        # Replace the field content in the full file
        content = content[:field_start] + new_field_content + content[field_end:]

        # Write back to file
        with open(xml_path, 'w', encoding='utf-8') as f:
            f.write(content)

        if debug:
            print(f"    Applied description to {field_type} {field_id}: '{description}'")

        return True

    except Exception as e:
        print(f"Error applying description: {e}")
        return False


def collect_description_mappings(engine: RemediationMappingEngine, input_view_data: list,
                                remediated_view_path: str, remed_attributes: set, remed_measures: set,
                                semantic_renamings: dict = None, debug: bool = False) -> List[Tuple[str, str, str, str]]:
    """
    Collect valid description mappings from source to target views.

    Returns: List of tuples (field_id, field_type, source_description, target_field_exists)
    """
    if len(input_view_data) == 1:
        # Single input view
        detailed_data = generate_detailed_view_comparison(
            engine, input_view_data[0], remediated_view_path,
            remed_attributes, remed_measures, semantic_renamings, debug
        )
    else:
        # Multiple input views - use union
        detailed_data = generate_union_view_comparison(
            engine, input_view_data, remediated_view_path,
            remed_attributes, remed_measures, semantic_renamings, debug
        )

    # Extract valid mappings
    # Row format: [source_name, source_desc, source_field, source_type, source_hidden,
    #              target_name, target_desc, target_field, target_type, target_hidden,
    #              rename, match_desc, match_type, match_hidden]

    description_mappings = []

    for row in detailed_data:
        source_column_name = row[0]
        source_description = row[1]
        target_column_name = row[5]
        target_description = row[6]
        target_type = row[8]

        # Valid mapping requires both source and target to exist
        # Note: source_description can be empty - we still want to map it
        if source_column_name and target_column_name and target_type:
            # Determine field type
            field_type = 'attribute' if target_type == 'ATTRIBUTE' else 'measure'

            # Only include if descriptions differ (no need to apply if already matching)
            if source_description != target_description:
                description_mappings.append((
                    target_column_name,  # field_id in target
                    field_type,          # attribute or measure
                    source_description,  # description to apply
                    target_description   # current description (for display)
                ))

    return description_mappings


def apply_descriptions(engine: RemediationMappingEngine, num_inputs: int,
                      semantic_renaming_file: str = None, debug: bool = False):
    """Apply descriptions from source views to remediated view"""

    # Find calculation views in both directories
    script_dir = Path(__file__).parent.parent.parent  # Project root
    input_views = get_calculation_views(str(script_dir / "inputs/calculation_view/source"))
    remediated_views = get_calculation_views(str(script_dir / "inputs/calculation_view/remediated"))

    # Display and select input views
    select_calculation_views(input_views, "Original")

    selected_input_views = []
    if num_inputs == 1:
        selected_view = select_single_view(input_views, "Select an original calculation view")
        selected_input_views = [selected_view]
    else:
        print(f"\nSelect {num_inputs} original calculation views:")
        selected_input_views = select_multiple_views(input_views, num_inputs)

    # Display and select remediated view
    select_calculation_views(remediated_views, "Remediated")
    selected_remediated = select_single_view(remediated_views, "Select a remediated calculation view")

    # Load semantic renaming if provided
    semantic_renamings = load_semantic_renamings(semantic_renaming_file, debug)

    print("\n--- Analyzing field mappings and descriptions ---")

    # Extract data from all selected views
    input_view_data = []
    for i, view in enumerate(selected_input_views, 1):
        attributes, measures = engine.extract_output_columns(view)
        view_data = {
            'view_path': view,
            'view_name': os.path.basename(view),
            'view_number': i,
            'attributes': attributes,
            'measures': measures
        }
        input_view_data.append(view_data)

    # Extract data from remediated view
    remed_attributes, remed_measures = engine.extract_output_columns(selected_remediated)

    # Collect description mappings
    description_mappings = collect_description_mappings(
        engine, input_view_data, selected_remediated,
        remed_attributes, remed_measures, semantic_renamings, debug
    )

    if not description_mappings:
        print("\nNo description mappings found (all descriptions already match or no valid field mappings exist).")
        return

    # Print description mappings to user
    print(f"\n{'='*80}")
    print(f"Found {len(description_mappings)} description mapping(s) to apply:")
    print(f"{'='*80}\n")

    for field_id, field_type, source_desc, target_desc in description_mappings:
        print(f"  {field_type.upper()}: {field_id}")
        print(f"    Current: '{target_desc}'")
        print(f"    New:     '{source_desc}'")
        print()

    # Prompt user for confirmation
    print(f"{'='*80}")
    response = input(f"Apply these descriptions to a copy of {os.path.basename(selected_remediated)}? (y/n): ").strip().lower()

    if response != 'y':
        print("Description application cancelled.")
        return

    # Create output directory
    output_dir = script_dir / "outputs/calculation_view/remediated"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Copy remediated view to output directory
    output_path = output_dir / os.path.basename(selected_remediated)
    shutil.copy2(selected_remediated, output_path)
    print(f"\nCopied {os.path.basename(selected_remediated)} to {output_path}")

    # Apply descriptions
    print("\nApplying descriptions...")
    success_count = 0
    fail_count = 0

    for field_id, field_type, source_desc, target_desc in description_mappings:
        if apply_description_to_xml(str(output_path), field_id, field_type, source_desc, debug):
            success_count += 1
        else:
            fail_count += 1

    print(f"\nDescription application complete:")
    print(f"  Successfully applied: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"\nOutput saved to: {output_path}")
