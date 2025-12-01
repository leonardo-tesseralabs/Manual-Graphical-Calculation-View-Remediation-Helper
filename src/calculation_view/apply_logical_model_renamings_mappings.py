#!/usr/bin/env python3
"""
Apply Logical Model Renamings Mappings

Handles the --almrm flag functionality for applying logical model renamings
to remediated calculation views.
"""

import os
import re
import csv
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict

from .view_remediator_engine import (
    RemediationMappingEngine, get_calculation_views, select_calculation_views,
    select_single_view, load_semantic_renamings
)
from .create_logical_model_renamings_mappings import extract_logical_model_renamings


def extract_logical_model_column_names(calculation_view_path: str, debug: bool = False) -> Set[str]:
    """
    Extract all columnName values from attributes and measures in logical model

    Returns set of column names found in the logical model
    """
    column_names = set()

    try:
        tree = ET.parse(calculation_view_path)
        root = tree.getroot()

        # Look in logicalModel sections for attribute and measure definitions
        for logical_model in root.findall('.//logicalModel'):
            # Process attributes (use keyMapping)
            for attribute in logical_model.findall('.//attribute'):
                # Look for keyMapping to get the columnName
                key_mapping = attribute.find('.//keyMapping')
                if key_mapping is not None:
                    column_name = key_mapping.get('columnName', '')
                    if column_name:
                        column_names.add(column_name)
                        if debug:
                            print(f"  Found attribute columnName: {column_name}")

            # Process measures (use measureMapping, NOT keyMapping!)
            for measure in logical_model.findall('.//measure'):
                # Look for measureMapping to get the columnName
                measure_mapping = measure.find('.//measureMapping')
                if measure_mapping is not None:
                    column_name = measure_mapping.get('columnName', '')
                    if column_name:
                        column_names.add(column_name)
                        if debug:
                            print(f"  Found measure columnName: {column_name}")

        if debug:
            print(f"Extracted {len(column_names)} column names from {calculation_view_path}")

    except Exception as e:
        if debug:
            print(f"Error extracting column names from {calculation_view_path}: {e}")

    return column_names


def load_lmrm_csv(csv_path: str, debug: bool = False) -> Dict[str, str]:
    """
    Load logical model renamings mapping CSV file

    Returns dictionary mapping Original -> Renamed
    """
    renamings = {}

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                original = row.get('Original', '').strip()
                renamed = row.get('Renamed', '').strip()
                if original and renamed:
                    renamings[original] = renamed

        if debug:
            print(f"Loaded {len(renamings)} renamings from {csv_path}")

    except Exception as e:
        print(f"Error loading LMRM CSV: {e}")
        return {}

    return renamings


def find_s4_field_mappings(ecc_field_name: str, engine: RemediationMappingEngine, debug: bool = False) -> List[str]:
    """
    Find all S4 field names that the given ECC field maps to

    Args:
        ecc_field_name: The ECC field name to look up
        engine: RemediationMappingEngine instance with loaded field mappings

    Returns:
        List of S4 field names (could be empty, one, or multiple)
    """
    s4_fields = []

    # Search through all field mappings
    for key, mapping_list in engine.field_mappings.items():
        # key format is "ECC_TABLE.ECC_FIELD"
        for mapping in mapping_list:
            if mapping.ecc_field == ecc_field_name:
                s4_field = mapping.s4_field
                if s4_field not in s4_fields:
                    s4_fields.append(s4_field)
                    if debug:
                        print(f"  Found mapping: {mapping.ecc_table}.{mapping.ecc_field} -> {mapping.s4_table}.{s4_field}")

    return s4_fields


def resolve_conflict_menu(conflict: Dict) -> Optional[str]:
    """
    Present menu to user to resolve a conflict (multiple ECC fields want different renamings for same S4 field)

    Returns: chosen renaming, or None to skip
    """
    s4_field = conflict['s4_field']
    ecc_mappings = conflict['ecc_mappings']

    print(f"\n⚠️  CONFLICT: S4 field '{s4_field}' has multiple possible renamings:")
    for i, (ecc, renamed) in enumerate(ecc_mappings, 1):
        print(f"  {i}. Rename to '{renamed}' (from ECC field {ecc})")
    print(f"  {len(ecc_mappings) + 1}. Skip this field (do not rename)")

    while True:
        try:
            choice = int(input(f"\nSelect option (1-{len(ecc_mappings) + 1}): "))
            if 1 <= choice <= len(ecc_mappings):
                chosen_renaming = ecc_mappings[choice - 1][1]
                print(f"  → Will rename '{s4_field}' to '{chosen_renaming}'")
                return chosen_renaming
            elif choice == len(ecc_mappings) + 1:
                print(f"  → Skipping '{s4_field}'")
                return None
            else:
                print(f"Please enter a number between 1 and {len(ecc_mappings) + 1}")
        except (ValueError, EOFError):
            print("Please enter a valid number")
            return None


def resolve_warning_menu(mapping: Dict) -> Optional[str]:
    """
    Present menu to user to resolve a warning (1 ECC field maps to multiple S4 fields)

    Returns: chosen S4 field, or None to skip
    """
    ecc_field = mapping['ecc_field']
    renamed = mapping['renamed']
    found_s4_fields = mapping['found']

    print(f"\n⚠️  WARNING: ECC field '{ecc_field}' maps to multiple S4 fields:")
    for i, s4_field in enumerate(found_s4_fields, 1):
        print(f"  {i}. Rename '{s4_field}' to '{renamed}'")
    print(f"  {len(found_s4_fields) + 1}. Skip this renaming")

    while True:
        try:
            choice = int(input(f"\nSelect option (1-{len(found_s4_fields) + 1}): "))
            if 1 <= choice <= len(found_s4_fields):
                chosen_s4 = found_s4_fields[choice - 1]
                print(f"  → Will rename '{chosen_s4}' to '{renamed}'")
                return chosen_s4
            elif choice == len(found_s4_fields) + 1:
                print(f"  → Skipping this renaming")
                return None
            else:
                print(f"Please enter a number between 1 and {len(found_s4_fields) + 1}")
        except (ValueError, EOFError):
            print("Please enter a valid number")
            return None


def apply_renaming_to_xml(xml_path: str, column_name: str, new_id: str, debug: bool = False) -> bool:
    """
    Apply a renaming to the XML file by changing the id attribute of an attribute/measure
    with the matching columnName. Uses string-based replacement to preserve original XML formatting.

    Returns: True if renaming was applied, False otherwise
    """
    try:
        # First, use ElementTree to find what needs to be renamed
        tree = ET.parse(xml_path)
        root = tree.getroot()

        old_ids_to_rename = []
        existing_ids = set()

        # Look in logicalModel sections
        for logical_model in root.findall('.//logicalModel'):
            # First pass: collect all existing ids
            for attribute in logical_model.findall('.//attribute'):
                attr_id = attribute.get('id', '')
                if attr_id:
                    existing_ids.add(('attribute', attr_id))
            for measure in logical_model.findall('.//measure'):
                measure_id = measure.get('id', '')
                if measure_id:
                    existing_ids.add(('measure', measure_id))

            # Second pass: check attributes for renaming
            for attribute in logical_model.findall('.//attribute'):
                key_mapping = attribute.find('.//keyMapping')
                if key_mapping is not None:
                    if key_mapping.get('columnName', '') == column_name:
                        old_id = attribute.get('id', '')

                        # Check if target id already exists
                        if ('attribute', new_id) in existing_ids and old_id != new_id:
                            print(f"    ⚠️  SKIPPING: Cannot rename {old_id} -> {new_id} because attribute id='{new_id}' already exists (would create duplicate)")
                            return False

                        old_ids_to_rename.append(('attribute', old_id))
                        if debug:
                            print(f"    Will rename attribute: {old_id} (columnName={column_name}) -> {new_id}")

            # Third pass: check measures for renaming
            for measure in logical_model.findall('.//measure'):
                measure_mapping = measure.find('.//measureMapping')
                if measure_mapping is not None:
                    if measure_mapping.get('columnName', '') == column_name:
                        old_id = measure.get('id', '')

                        # Check if target id already exists
                        if ('measure', new_id) in existing_ids and old_id != new_id:
                            print(f"    ⚠️  SKIPPING: Cannot rename {old_id} -> {new_id} because measure id='{new_id}' already exists (would create duplicate)")
                            return False

                        old_ids_to_rename.append(('measure', old_id))
                        if debug:
                            print(f"    Will rename measure: {old_id} (columnName={column_name}) -> {new_id}")

        if not old_ids_to_rename:
            if debug:
                print(f"    Warning: Could not find attribute/measure with columnName={column_name}")
            return False

        # Read the file as text to preserve formatting
        with open(xml_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Apply string replacements for each id attribute
        for field_type, old_id in old_ids_to_rename:
            # Replace id="old_id" with id="new_id" in attribute or measure tags
            # Use word boundaries to avoid partial matches
            pattern = f'<{field_type}\\s+id="{re.escape(old_id)}"'
            replacement = f'<{field_type} id="{new_id}"'
            content = re.sub(pattern, replacement, content)

        # Write back to file with original formatting preserved
        with open(xml_path, 'w', encoding='utf-8') as f:
            f.write(content)

        return True

    except Exception as e:
        print(f"Error applying renaming: {e}")
        return False


def apply_logical_model_renamings_mappings(engine: RemediationMappingEngine, renamings_file: str = None, debug: bool = False):
    """Apply logical model renamings mappings workflow"""

    script_dir = Path(__file__).parent.parent.parent  # Project root

    # Load semantic renamings if provided
    semantic_renamings = {}
    if renamings_file:
        semantic_renamings = load_semantic_renamings(renamings_file, debug)
        print(f"\nLoaded {len(semantic_renamings)} semantic renamings from {renamings_file}")

    # Step 1: Select source view from inputs/cv
    print("\n=== Step 1: Select Source View ===")
    source_views = get_calculation_views(str(script_dir / "inputs/calculation_view/source"))

    if not source_views:
        print("No calculation views found in inputs/cv directory!")
        return

    select_calculation_views(source_views, "Source (ECC)")
    selected_source_view = select_single_view(source_views, "Select source view to extract logical model renamings")

    print(f"\nSelected source view: {os.path.basename(selected_source_view)}")

    # Step 2: Run --clmrm flow on selected view to generate CSV
    print("\n=== Step 2: Extracting Logical Model Renamings ===")
    renamings = extract_logical_model_renamings(selected_source_view, debug)

    if not renamings:
        print("No logical model renamings found in source view (all id values match columnName values)")
        print("Nothing to apply. Exiting.")
        return

    # Save renamings to CSV in inputs/cv_logical_model_renamings
    lmrm_dir = script_dir / "inputs/calculation_view/logical_model_renamings"
    lmrm_dir.mkdir(exist_ok=True)

    view_basename = os.path.basename(selected_source_view)
    if view_basename.endswith('.calculationview'):
        view_basename = view_basename[:-len('.calculationview')]
    lmrm_filename = f"{view_basename}-lmrm.csv"
    lmrm_path = lmrm_dir / lmrm_filename

    # Write renamings to CSV
    try:
        with open(lmrm_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Original', 'Renamed'])
            for original, renamed in sorted(renamings.items()):
                writer.writerow([original, renamed])

        print(f"\nGenerated LMRM file: {lmrm_path}")
        print(f"Found {len(renamings)} field renamings:")
        for original, renamed in sorted(renamings.items()):
            print(f"  {original} -> {renamed}")

    except Exception as e:
        print(f"Error writing LMRM CSV: {e}")
        return

    # Step 3: Select remediated view from inputs/cv_remediated
    print("\n=== Step 3: Select Remediated View ===")
    remediated_views = get_calculation_views(str(script_dir / "inputs/calculation_view/remediated"))

    if not remediated_views:
        print("No calculation views found in inputs/cv_remediated directory!")
        return

    select_calculation_views(remediated_views, "Remediated (S/4HANA)")
    selected_remediated_view = select_single_view(remediated_views, "Select remediated view to verify against")

    print(f"\nSelected remediated view: {os.path.basename(selected_remediated_view)}")

    # Step 4: Apply ECC -> S4 field mappings and verify
    print("\n=== Step 4: Applying ECC -> S4 Field Mappings ===")
    remediated_column_names = extract_logical_model_column_names(selected_remediated_view, debug)

    print(f"\nFound {len(remediated_column_names)} column names in remediated view")

    # Load the generated LMRM CSV
    lmrm_renamings = load_lmrm_csv(str(lmrm_path), debug)

    # Check each Original value from CSV
    print("\n--- Processing ECC fields from LMRM CSV ---")

    # Build mapping: S4 field -> list of (ECC field, renaming) tuples
    s4_to_ecc_mappings = defaultdict(list)
    ecc_to_s4_mappings = {}  # Track what S4 fields each ECC field maps to

    for original_ecc_field in sorted(lmrm_renamings.keys()):
        renamed_in_logical_model = lmrm_renamings[original_ecc_field]

        # Apply semantic renamings if available
        lookup_field = semantic_renamings.get(original_ecc_field, original_ecc_field)

        if debug and lookup_field != original_ecc_field:
            print(f"\n  Semantic renaming applied: {original_ecc_field} -> {lookup_field}")

        # Find all S4 field mappings for this ECC field
        s4_fields = find_s4_field_mappings(lookup_field, engine, debug)

        if not s4_fields:
            # Check if the original field exists directly in remediated view (no mapping needed)
            if original_ecc_field in remediated_column_names:
                s4_fields = [original_ecc_field]
            else:
                # No mapping found - track as missing
                ecc_to_s4_mappings[original_ecc_field] = []
                continue

        # Store the mapping
        ecc_to_s4_mappings[original_ecc_field] = s4_fields

        # For each S4 field this ECC field maps to, track the reverse mapping
        for s4_field in s4_fields:
            s4_to_ecc_mappings[s4_field].append((original_ecc_field, renamed_in_logical_model))

    # Now analyze the mappings
    success_mappings = []  # Exactly 1 S4 field found, no conflicts
    missing_mappings = []  # 0 S4 fields found in remediated view
    warning_mappings = []  # Multiple S4 fields found OR conflicts
    conflict_mappings = []  # Same S4 field needs multiple different renamings

    # Check for conflicts: same S4 field mapped from multiple ECC fields with different renamings
    for s4_field, ecc_renamings in s4_to_ecc_mappings.items():
        if s4_field in remediated_column_names and len(ecc_renamings) > 1:
            # Check if all renamings are the same
            unique_renamings = set(renamed for _, renamed in ecc_renamings)
            if len(unique_renamings) > 1:
                # Conflict: same S4 field needs different renamings
                conflict_mappings.append({
                    's4_field': s4_field,
                    'ecc_mappings': ecc_renamings,
                    'note': f'{len(ecc_renamings)} ECC fields map to same S4 field with different renamings'
                })

    # Process each ECC field
    for original_ecc_field, renamed_in_logical_model in sorted(lmrm_renamings.items()):
        s4_fields = ecc_to_s4_mappings.get(original_ecc_field, [])

        if not s4_fields:
            missing_mappings.append({
                'ecc_field': original_ecc_field,
                'renamed': renamed_in_logical_model,
                's4_fields': [],
                'note': 'No S4 mapping found'
            })
            continue

        # Check which S4 fields exist in remediated view
        found_s4_fields = [s4 for s4 in s4_fields if s4 in remediated_column_names]

        if len(found_s4_fields) == 0:
            missing_mappings.append({
                'ecc_field': original_ecc_field,
                'renamed': renamed_in_logical_model,
                's4_fields': s4_fields,
                'found': [],
                'note': f'None of the {len(s4_fields)} S4 mapping(s) found in remediated view'
            })
        elif len(found_s4_fields) == 1:
            # Check if this S4 field has a conflict
            s4_field = found_s4_fields[0]
            has_conflict = any(c['s4_field'] == s4_field for c in conflict_mappings)

            if not has_conflict:
                success_mappings.append({
                    'ecc_field': original_ecc_field,
                    'renamed': renamed_in_logical_model,
                    's4_fields': s4_fields,
                    'found': found_s4_fields,
                    'note': 'Success'
                })
            # If has conflict, it will be reported in conflict_mappings
        else:  # Multiple found
            warning_mappings.append({
                'ecc_field': original_ecc_field,
                'renamed': renamed_in_logical_model,
                's4_fields': s4_fields,
                'found': found_s4_fields,
                'note': f'Ambiguous: {len(found_s4_fields)} S4 fields found'
            })

    # Summary
    print("\n=== Verification Summary ===")
    print(f"Total renamings in LMRM CSV: {len(lmrm_renamings)}")
    print(f"Success (1 S4 field found, no conflicts): {len(success_mappings)}")
    print(f"Missing (0 S4 fields found): {len(missing_mappings)}")
    print(f"Warning (>1 S4 fields found): {len(warning_mappings)}")
    print(f"Conflicts (same S4 field needs different renamings): {len(conflict_mappings)}")

    if success_mappings:
        print(f"\n✓ SUCCESS MAPPINGS ({len(success_mappings)}):")
        for mapping in success_mappings:
            ecc = mapping['ecc_field']
            renamed = mapping['renamed']
            s4 = mapping['found'][0]
            print(f"  {ecc} -> {s4}")
            print(f"    Logical model renaming: {s4} should be renamed to '{renamed}'")

    if conflict_mappings:
        print(f"\n⚠️  CONFLICT WARNINGS ({len(conflict_mappings)}):")
        for conflict in conflict_mappings:
            s4_field = conflict['s4_field']
            ecc_mappings = conflict['ecc_mappings']
            print(f"  S4 field '{s4_field}' has conflicting renamings:")
            for ecc, renamed in ecc_mappings:
                print(f"    • {ecc} wants to rename it to '{renamed}'")
            print(f"    ⚠️  Cannot apply multiple different renamings to the same field!")

    if warning_mappings:
        print(f"\n⚠️  WARNING MAPPINGS ({len(warning_mappings)}):")
        for mapping in warning_mappings:
            ecc = mapping['ecc_field']
            renamed = mapping['renamed']
            found = mapping['found']
            print(f"  {ecc} -> MULTIPLE: {', '.join(found)}")
            print(f"    {mapping['note']}")
            print(f"    Logical model renaming unclear - which should be renamed to '{renamed}'?")

    if missing_mappings:
        print(f"\n✗ MISSING MAPPINGS ({len(missing_mappings)}):")
        for mapping in missing_mappings:
            ecc = mapping['ecc_field']
            renamed = mapping['renamed']
            s4_fields = mapping['s4_fields']
            if s4_fields:
                print(f"  {ecc} -> Expected S4 fields: {', '.join(s4_fields)}")
            else:
                print(f"  {ecc} -> {mapping['note']}")
            print(f"    Logical model renaming '{renamed}' cannot be applied - target field not found")

    print(f"\nVerification complete!")

    # Handle missing mappings
    if missing_mappings:
        print(f"\n⚠️  Warning: {len(missing_mappings)} field(s) could not be mapped")
        print("\nWhat would you like to do?")
        print("  1. Continue anyway (skip missing fields)")
        print("  2. Abort remediation")

        while True:
            try:
                choice = int(input("\nSelect option (1-2): "))
                if choice == 1:
                    print("  → Continuing with remediation (missing fields will be skipped)")
                    break
                elif choice == 2:
                    print("  → Aborting remediation")
                    return
                else:
                    print("Please enter 1 or 2")
            except (ValueError, EOFError):
                print("Please enter a valid number")
                print("  → Aborting remediation")
                return

    # Step 5: Apply Renamings
    print("\n=== Step 5: Apply Renamings ===")

    # Ask user if they want to apply renamings
    apply = input("\nDo you want to apply the renamings? (y/n): ").strip().lower()
    if apply != 'y':
        print("Renamings not applied. Exiting.")
        return

    # Create output directory
    output_dir = script_dir / "outputs/calculation_view/remediated"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Copy remediated view to output directory
    output_filename = os.path.basename(selected_remediated_view)
    output_path = output_dir / output_filename

    print(f"\nCopying {os.path.basename(selected_remediated_view)} to outputs/cv_remediated/...")
    shutil.copy2(selected_remediated_view, output_path)
    print(f"  → Created: {output_path}")

    # Track renamings to apply: {s4_field: new_name}
    renamings_to_apply = {}

    # 1. Add all success mappings
    print(f"\n--- Processing Success Mappings ---")
    for mapping in success_mappings:
        s4_field = mapping['found'][0]
        new_name = mapping['renamed']
        renamings_to_apply[s4_field] = new_name
        print(f"  Will rename '{s4_field}' to '{new_name}'")

    # 2. Resolve conflicts interactively
    if conflict_mappings:
        print(f"\n--- Resolving Conflicts ---")
        for conflict in conflict_mappings:
            s4_field = conflict['s4_field']
            chosen_renaming = resolve_conflict_menu(conflict)
            if chosen_renaming:
                renamings_to_apply[s4_field] = chosen_renaming

    # 3. Resolve warnings interactively
    if warning_mappings:
        print(f"\n--- Resolving Warnings ---")
        for warning in warning_mappings:
            renamed = warning['renamed']
            chosen_s4 = resolve_warning_menu(warning)
            if chosen_s4:
                renamings_to_apply[chosen_s4] = renamed

    # 4. Apply all renamings to the XML file
    if renamings_to_apply:
        print(f"\n--- Applying {len(renamings_to_apply)} Renamings to XML ---")
        applied_count = 0
        failed_count = 0

        for s4_field, new_name in sorted(renamings_to_apply.items()):
            print(f"  Renaming '{s4_field}' to '{new_name}'...")
            if apply_renaming_to_xml(str(output_path), s4_field, new_name, debug):
                applied_count += 1
            else:
                failed_count += 1
                print(f"    ⚠️  Failed to apply renaming")

        print(f"\n✓ Successfully applied {applied_count} renaming(s)")
        if failed_count > 0:
            print(f"⚠️  Failed to apply {failed_count} renaming(s)")

        print(f"\n✓ Remediated calculation view saved to: {output_path}")
    else:
        print("\nNo renamings to apply.")

    print(f"\nRemediation complete!")
