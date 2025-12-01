#!/usr/bin/env python3
"""
Remediation Report Generator

Handles the --report flag functionality for generating comprehensive
Excel reports comparing ECC and S/4HANA calculation views.
"""

import os
import csv
import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict

from .view_remediator_engine import (
    RemediationMappingEngine, get_calculation_views, select_calculation_views,
    select_single_view, select_multiple_views, setup_output_directory,
    load_semantic_renamings, extract_field_hidden_status, extract_field_descriptions,
    select_directory
)


class FieldLineage:
    """Represents the lineage of a field through calculation view nodes"""
    def __init__(self, field_name: str, node_id: str, source_field: str = None,
                 source_node: str = None, is_original_source: bool = False):
        self.field_name = field_name
        self.node_id = node_id
        self.source_field = source_field
        self.source_node = source_node
        self.is_original_source = is_original_source


def trace_field_lineage(xml_file_path: str, field_name: str, debug: bool = False) -> List:
    """
    Trace the lineage of a field through the calculation view hierarchy.
    Returns a list of FieldLineage objects showing the path from original source to final field.
    """
    import xml.etree.ElementTree as ET

    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        lineage = []

        # Find where this field first appears in the logical model
        ns = {'calc': 'http://www.sap.com/ndb/BiModelCalculation.ecore'}

        # Check if field exists in final output (logical model)
        logical_model = root.find('.//calc:logicalModel', ns)
        if logical_model is None:
            logical_model = root.find('.//logicalModel')

        field_found_in_output = False
        logical_model_source_field = None
        logical_model_source_node = None

        if logical_model is not None:
            # Check attributes
            for attr in logical_model.findall('.//attribute'):
                if attr.get('id') == field_name:
                    field_found_in_output = True
                    key_mapping = attr.find('.//keyMapping')
                    if key_mapping is not None:
                        logical_model_source_field = key_mapping.get('columnName')
                        logical_model_source_node = key_mapping.get('columnObjectName')
                    break
            # Check measures
            if not field_found_in_output:
                for measure in logical_model.findall('.//measure'):
                    if measure.get('id') == field_name:
                        field_found_in_output = True
                        measure_mapping = measure.find('.//measureMapping')
                        if measure_mapping is not None:
                            logical_model_source_field = measure_mapping.get('columnName')
                            logical_model_source_node = measure_mapping.get('columnObjectName')
                        break

        if not field_found_in_output:
            return lineage

        # If there's a logical model mapping, add it to lineage first
        if logical_model_source_field and logical_model_source_node:
            if logical_model_source_field != field_name:
                logical_model_entry = FieldLineage(
                    field_name=field_name,
                    node_id="LogicalModel",
                    source_field=logical_model_source_field,
                    source_node=logical_model_source_node,
                    is_original_source=False
                )
                lineage.append(logical_model_entry)
                current_field = logical_model_source_field
            else:
                current_field = field_name
        else:
            current_field = field_name

        # Start tracing from the determined current field and work backwards
        traced_nodes = set()

        def trace_backwards(field, current_node_id=None):
            """Recursively trace field backwards through nodes"""
            # Find calculation views
            calc_views = []
            calc_views.extend(root.findall('.//calc:calculationView', ns))
            if not calc_views:
                calc_views.extend(root.findall('.//calculationView'))

            if current_node_id is None:
                if calc_views:
                    current_node_id = calc_views[-1].get('id')

            if current_node_id in traced_nodes:
                return
            traced_nodes.add(current_node_id)

            # Find the calculation view with this ID
            target_calc_view = None
            for calc_view in calc_views:
                if calc_view.get('id') == current_node_id:
                    target_calc_view = calc_view
                    break

            if target_calc_view is None:
                return

            # Check if this field is a calculatedViewAttribute in the current node FIRST
            # (calculated columns might not be in viewAttributes)
            calc_attrs = []
            calc_attrs.extend(target_calc_view.findall('.//calc:calculatedViewAttribute', ns))
            if not calc_attrs:
                calc_attrs.extend(target_calc_view.findall('.//calculatedViewAttribute'))

            for calc_attr in calc_attrs:
                if calc_attr.get('id') == field:
                    # This is a calculated column - it's the original source
                    if debug:
                        print(f"Found calculated column: {field} in node {current_node_id}")
                    calc_entry = FieldLineage(
                        field_name=field,
                        node_id=current_node_id,
                        source_field=None,
                        source_node=f"Calculated Column, {current_node_id}",
                        is_original_source=True
                    )
                    lineage.append(calc_entry)
                    return  # Stop tracing, we found the source

            # Check if this field is defined in viewAttributes of this node
            view_attrs = target_calc_view.find('.//viewAttributes')
            field_in_node = False
            if view_attrs is not None:
                for attr in view_attrs.findall('.//viewAttribute'):
                    if attr.get('id') == field:
                        field_in_node = True
                        break

            if not field_in_node:
                return

            # Find input mappings for this field
            input_elements = []
            input_elements.extend(target_calc_view.findall('.//calc:input', ns))
            if not input_elements:
                input_elements.extend(target_calc_view.findall('.//input'))

            field_mapped = False
            for input_elem in input_elements:
                source_node_ref = input_elem.get('node')
                if not source_node_ref:
                    continue

                # Find mapping for this field
                mappings = []
                mappings.extend(input_elem.findall('.//calc:mapping', ns))
                if not mappings:
                    mappings.extend(input_elem.findall('.//mapping'))

                for mapping in mappings:
                    target_field = mapping.get('target')
                    source_field = mapping.get('source')

                    if target_field == field:
                        field_mapped = True
                        source_node_name = source_node_ref[1:] if source_node_ref.startswith('#') else source_node_ref

                        # Create lineage entry
                        is_renamed = (source_field != target_field)
                        lineage_entry = FieldLineage(
                            field_name=target_field,
                            node_id=current_node_id,
                            source_field=source_field if is_renamed else None,
                            source_node=source_node_name,
                            is_original_source=False
                        )
                        lineage.append(lineage_entry)

                        # Check if source is a DataSource (original table or calculation view)
                        is_datasource_table = False
                        is_datasource_calcview = False
                        is_calculated = False

                        datasources = []
                        datasources.extend(root.findall('.//calc:DataSource', ns))
                        if not datasources:
                            datasources.extend(root.findall('.//DataSource'))

                        for ds in datasources:
                            if ds.get('id') == source_node_name:
                                ds_type = ds.get('type', '')
                                if ds_type == 'DATA_BASE_TABLE':
                                    # This is an actual table - original source
                                    is_datasource_table = True
                                    lineage_entry.is_original_source = True
                                elif ds_type == 'CALCULATION_VIEW':
                                    # This is a calculation view - need to recursively expand
                                    is_datasource_calcview = True
                                    resource_uri_elem = ds.find('.//resourceUri')
                                    if resource_uri_elem is not None and resource_uri_elem.text:
                                        # Get the calculation view filename
                                        resource_uri = resource_uri_elem.text.strip()
                                        view_name = resource_uri.split('/')[-1]
                                        nested_view_path = os.path.join(os.path.dirname(xml_file_path),
                                                                       view_name + '.calculationview')
                                        if os.path.exists(nested_view_path):
                                            # Recursively trace in the nested calculation view
                                            nested_lineage = trace_field_lineage(nested_view_path, source_field, debug)
                                            # Add the nested lineage to our current lineage
                                            if nested_lineage:
                                                lineage.extend(nested_lineage)
                                            else:
                                                if debug:
                                                    print(f"Warning: Could not trace field {source_field} in nested view {view_name}")
                                break

                        # If not a datasource table or calcview, check if it's a calculated column
                        if not is_datasource_table and not is_datasource_calcview:
                            # Find the source node to check for calculated columns
                            source_calc_view = None
                            for calc_view in calc_views:
                                if calc_view.get('id') == source_node_name:
                                    source_calc_view = calc_view
                                    break

                            if source_calc_view is not None:
                                # Check if source_field is a calculatedViewAttribute
                                calc_attrs = []
                                calc_attrs.extend(source_calc_view.findall('.//calc:calculatedViewAttribute', ns))
                                if not calc_attrs:
                                    calc_attrs.extend(source_calc_view.findall('.//calculatedViewAttribute'))

                                for calc_attr in calc_attrs:
                                    if calc_attr.get('id') == source_field:
                                        is_calculated = True
                                        lineage_entry.is_original_source = True
                                        # Mark this as a calculated column in the source_node
                                        lineage_entry.source_node = f"Calculated Column, {source_node_name}"
                                        break

                        # If not a datasource table, calcview, or calculated column, continue tracing backwards
                        if not is_datasource_table and not is_datasource_calcview and not is_calculated:
                            trace_backwards(source_field, source_node_name)
                        break

                if field_mapped:
                    break

        # Start the backwards trace
        if logical_model_source_node:
            trace_backwards(current_field, logical_model_source_node)
        else:
            trace_backwards(current_field)

        # Reverse the lineage to show from source to target
        lineage.reverse()

        return lineage

    except Exception as e:
        if debug:
            print(f"Error tracing lineage for field {field_name}: {e}")
        return []


def extract_field_source_lineage(xml_file_path: str, debug: bool = False) -> Dict[str, str]:
    """
    Extract source field lineage (TABLE.FIELD) from calculation view XML
    by tracing through the calculation view hierarchy to find the original source table.

    Returns dictionary mapping field_id -> source_table.source_field
    """
    import xml.etree.ElementTree as ET
    lineage_map = {}

    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Get all fields from logical model
        for logical_model in root.findall('.//logicalModel'):
            # Process attributes
            for attribute in logical_model.findall('.//attribute'):
                attr_id = attribute.get('id', '')
                if attr_id:
                    lineage = trace_field_lineage(xml_file_path, attr_id, debug)
                    # Find the original source from lineage
                    for entry in lineage:
                        if entry.is_original_source:
                            # Check if it's a calculated column
                            if entry.source_node.startswith("Calculated Column,"):
                                # For calculated columns, use the full description
                                lineage_map[attr_id] = entry.source_node
                            else:
                                # For regular fields, use TABLE.FIELD format
                                source_field = entry.source_field if entry.source_field else entry.field_name
                                lineage_map[attr_id] = f"{entry.source_node}.{source_field}"
                            break

            # Process measures
            for measure in logical_model.findall('.//measure'):
                measure_id = measure.get('id', '')
                if measure_id:
                    lineage = trace_field_lineage(xml_file_path, measure_id, debug)
                    # Find the original source from lineage
                    for entry in lineage:
                        if entry.is_original_source:
                            # Check if it's a calculated column
                            if entry.source_node.startswith("Calculated Column,"):
                                # For calculated columns, use the full description
                                lineage_map[measure_id] = entry.source_node
                            else:
                                # For regular fields, use TABLE.FIELD format
                                source_field = entry.source_field if entry.source_field else entry.field_name
                                lineage_map[measure_id] = f"{entry.source_node}.{source_field}"
                            break

        if debug:
            print(f"Extracted lineage for {len(lineage_map)} fields from {xml_file_path}")

    except Exception as e:
        if debug:
            print(f"Error extracting lineage from {xml_file_path}: {e}")

    return lineage_map


def resolve_recursive_data_sources(engine: RemediationMappingEngine, calculation_view_path: str) -> Set[str]:
    """Recursively resolve all data sources from a calculation view"""
    visited = set()
    all_sources = set()

    def _resolve_sources(view_path: str):
        if view_path in visited:
            return
        visited.add(view_path)

        try:
            import xml.etree.ElementTree as ET
            tree = ET.parse(view_path)
            root = tree.getroot()

            # Find all DataSource elements (capital D)
            for data_source in root.findall('.//DataSource'):
                ds_type = data_source.get('type', '')

                if ds_type == 'DATA_BASE_TABLE':
                    # Extract table name from columnObject
                    column_obj = data_source.find('.//columnObject')
                    if column_obj is not None:
                        table_name = column_obj.get('columnObjectName', '')
                        if table_name:
                            all_sources.add(table_name)

                elif ds_type == 'CALCULATION_VIEW':
                    # Get resourceUri as child element, not attribute
                    resource_uri_elem = data_source.find('.//resourceUri')
                    if resource_uri_elem is not None and resource_uri_elem.text:
                        resource_uri = resource_uri_elem.text.strip()
                        # Extract view name from URI like /POC.TESSERA/calculationviews/CV_NAME
                        view_name = resource_uri.split('/')[-1]
                        # Try to find the nested view file
                        nested_view_path = os.path.join(os.path.dirname(view_path),
                                                       view_name + '.calculationview')
                        if os.path.exists(nested_view_path):
                            _resolve_sources(nested_view_path)

        except Exception as e:
            print(f"Warning: Could not resolve data sources from {view_path}: {e}")

    _resolve_sources(calculation_view_path)
    return all_sources


def process_field_comparison(engine: RemediationMappingEngine, field_name: str, field_type: str,
                           source_descriptions: dict, remed_descriptions: dict,
                           remed_attributes: set, remed_measures: set, remed_data_sources: set,
                           source_hidden_status: dict = None, remed_hidden_status: dict = None,
                           semantic_renamings: dict = None, source_lineage: dict = None,
                           remed_lineage: dict = None, debug: bool = False) -> list:
    """Process a single field comparison and return row data"""

    # Initialize row with source field data
    source_column_name = field_name
    source_description = source_descriptions.get(field_name, '')

    # Handle source_field: could be a string (single view) or list (union)
    if source_lineage:
        source_field_raw = source_lineage.get(field_name, '')
        if isinstance(source_field_raw, list):
            # Union case: format as [A, B, C]
            source_field = str(source_field_raw)
        else:
            # Single view case: use as-is
            source_field = source_field_raw
    else:
        source_field = ''

    source_type = field_type

    # Initialize target fields (will be populated if mapping found)
    target_column_name = ''
    target_description = ''
    target_source_field = ''
    target_type = ''

    # Initialize check flags
    rename_flag = 'N'
    matching_description = 'N/A'
    matching_type = 'N/A'

    # Check for direct field presence in remediated view
    target_fields = remed_attributes if field_type == 'ATTRIBUTE' else remed_measures
    opposite_fields = remed_measures if field_type == 'ATTRIBUTE' else remed_attributes

    if field_name in target_fields:
        # Direct match found with same type
        target_column_name = field_name
        target_description = remed_descriptions.get(field_name, '')
        target_source_field = remed_lineage.get(field_name, '') if remed_lineage else ''
        target_type = field_type
        rename_flag = 'N'
        matching_type = 'Y'
        # Check if descriptions match (empty is a valid description)
        matching_description = 'Y' if source_description == target_description else 'N'
    elif field_name in opposite_fields:
        # Field exists but with different type (type mismatch)
        target_column_name = field_name
        target_description = remed_descriptions.get(field_name, '')
        target_source_field = remed_lineage.get(field_name, '') if remed_lineage else ''
        target_type = 'MEASURE' if field_type == 'ATTRIBUTE' else 'ATTRIBUTE'
        rename_flag = 'N'
        matching_type = 'TYPE_MISMATCH'
        # Check if descriptions match (empty is a valid description)
        matching_description = 'Y' if source_description == target_description else 'N'
    else:
        # Check for mapping
        mapping_found = None
        mapping_type_mismatch = False
        for key, mapping_list in engine.field_mappings.items():
            for mapping in mapping_list:
                if mapping.ecc_field == field_name and mapping.s4_table in remed_data_sources:
                    if mapping.s4_field in target_fields:
                        mapping_found = mapping
                        break
                    elif mapping.s4_field in opposite_fields:
                        mapping_found = mapping
                        mapping_type_mismatch = True
                        break
            if mapping_found:
                break

        if mapping_found:
            # Mapping found
            target_column_name = mapping_found.s4_field
            target_description = remed_descriptions.get(mapping_found.s4_field, '')
            target_source_field = remed_lineage.get(mapping_found.s4_field, '') if remed_lineage else ''
            if mapping_type_mismatch:
                target_type = 'MEASURE' if field_type == 'ATTRIBUTE' else 'ATTRIBUTE'
                matching_type = 'TYPE_MISMATCH'
            else:
                target_type = field_type
                matching_type = 'Y'
            rename_flag = 'Y' if field_name != mapping_found.s4_field else 'N'
            # Check if descriptions match (empty is a valid description)
            matching_description = 'Y' if source_description == target_description else 'N'
        else:
            # Check for semantic renaming
            semantic_renaming_found = False
            semantic_type_mismatch = False
            if semantic_renamings:
                renamed_field = semantic_renamings.get(field_name)
                if renamed_field:
                    if renamed_field in target_fields:
                        # Semantic renaming found with same type
                        semantic_renaming_found = True
                    elif renamed_field in opposite_fields:
                        # Semantic renaming found with type mismatch
                        semantic_renaming_found = True
                        semantic_type_mismatch = True

                    if semantic_renaming_found:
                        target_column_name = renamed_field
                        target_description = remed_descriptions.get(renamed_field, '')
                        target_source_field = remed_lineage.get(renamed_field, '') if remed_lineage else ''
                        if semantic_type_mismatch:
                            target_type = 'MEASURE' if field_type == 'ATTRIBUTE' else 'ATTRIBUTE'
                            matching_type = 'TYPE_MISMATCH'
                        else:
                            target_type = field_type
                            matching_type = 'Y'
                        rename_flag = 'Y'
                        # Check if descriptions match (empty is a valid description)
                        matching_description = 'Y' if source_description == target_description else 'N'

            if not semantic_renaming_found:
                # No mapping or semantic renaming found - field is missing
                target_column_name = ''
                target_description = ''
                target_source_field = ''
                target_type = ''
                rename_flag = 'N/A'
                matching_type = 'N/A'

    # Get hidden status - could be boolean (single view) or list (union)
    if source_hidden_status:
        source_hidden_raw = source_hidden_status.get(field_name, False)
        if isinstance(source_hidden_raw, list):
            # Union case: format as ['Y', 'N', 'Y'] etc.
            source_hidden_list = ['Y' if h else 'N' for h in source_hidden_raw]
            source_hidden = str(source_hidden_list)
            # For CHECK: not hidden if ANY view is not hidden (i.e., ALL must be hidden to consider it hidden)
            source_hidden_for_check = all(source_hidden_raw)  # False if at least one is not hidden
        else:
            # Single view case
            source_hidden = 'Y' if source_hidden_raw else 'N'
            source_hidden_for_check = source_hidden_raw
    else:
        source_hidden = 'N'
        source_hidden_for_check = False

    target_hidden = 'Y' if remed_hidden_status and target_column_name and remed_hidden_status.get(target_column_name, False) else ('N' if target_column_name else 'N/A')
    target_hidden_for_check = remed_hidden_status.get(target_column_name, False) if remed_hidden_status and target_column_name else False

    # Check if hidden status matches (using the check logic)
    if target_column_name:
        # Convert to comparable format: True = hidden, False = not hidden
        matching_hidden = 'Y' if source_hidden_for_check == target_hidden_for_check else 'N'
    else:
        matching_hidden = 'N/A'

    # Create row data (added SOURCE_FIELD and HIDDEN columns and checks)
    row_data = [
        source_column_name, source_description, source_field, source_type, source_hidden,
        target_column_name, target_description, target_source_field, target_type, target_hidden,
        rename_flag, matching_description, matching_type, matching_hidden
    ]

    return row_data


def generate_detailed_view_comparison(engine: RemediationMappingEngine, source_view_data: dict,
                                    remediated_view_path: str, remed_attributes: set, remed_measures: set,
                                    semantic_renamings: dict = None, debug: bool = False) -> list:
    """Generate detailed comparison data for a specific source view"""

    # Get data sources from remediated view
    remed_data_sources = resolve_recursive_data_sources(engine, remediated_view_path)

    # Extract descriptions, hidden status, and lineage from both views
    source_descriptions = extract_field_descriptions(source_view_data['view_path'], debug)
    remed_descriptions = extract_field_descriptions(remediated_view_path, debug)
    source_hidden_status = extract_field_hidden_status(source_view_data['view_path'], debug)
    remed_hidden_status = extract_field_hidden_status(remediated_view_path, debug)
    source_lineage = extract_field_source_lineage(source_view_data['view_path'], debug)
    remed_lineage = extract_field_source_lineage(remediated_view_path, debug)

    detailed_rows = []

    # Process attributes
    for attr in source_view_data['attributes']:
        row_data = process_field_comparison(engine, attr, 'ATTRIBUTE', source_descriptions, remed_descriptions,
                                          remed_attributes, remed_measures, remed_data_sources, source_hidden_status,
                                          remed_hidden_status, semantic_renamings, source_lineage, remed_lineage, debug)
        detailed_rows.append(row_data)

    # Process measures
    for measure in source_view_data['measures']:
        row_data = process_field_comparison(engine, measure, 'MEASURE', source_descriptions, remed_descriptions,
                                          remed_attributes, remed_measures, remed_data_sources, source_hidden_status,
                                          remed_hidden_status, semantic_renamings, source_lineage, remed_lineage, debug)
        detailed_rows.append(row_data)

    # Add surplus fields from remediated view (fields that don't exist in source)
    all_source_fields = set(list(source_view_data['attributes']) + list(source_view_data['measures']))

    # Surplus attributes
    for attr in remed_attributes:
        if attr not in all_source_fields:
            # Check if this is a mapping target
            is_mapping_target = False
            for source_field in all_source_fields:
                for key, mapping_list in engine.field_mappings.items():
                    for mapping in mapping_list:
                        if mapping.ecc_field == source_field and mapping.s4_field == attr:
                            if mapping.s4_table in remed_data_sources:
                                is_mapping_target = True
                                break
                    if is_mapping_target:
                        break
                if is_mapping_target:
                    break

            # Check if this is a semantic renaming target
            is_semantic_target = False
            if not is_mapping_target and semantic_renamings:
                for source_field, renamed_field in semantic_renamings.items():
                    if renamed_field == attr and source_field in all_source_fields:
                        is_semantic_target = True
                        break

            # Check if this is a type mismatch (field exists in source but as different type)
            is_type_mismatch = attr in source_view_data['measures']

            if not is_mapping_target and not is_semantic_target and not is_type_mismatch:
                # This is a surplus field
                target_hidden = 'Y' if remed_hidden_status.get(attr, False) else 'N'
                surplus_row = ['', '', '', '', 'N/A',
                             attr, remed_descriptions.get(attr, ''), remed_lineage.get(attr, ''), 'ATTRIBUTE', target_hidden,
                             'N/A', 'N/A', 'N/A', 'N/A']
                detailed_rows.append(surplus_row)

    # Surplus measures
    for measure in remed_measures:
        if measure not in all_source_fields:
            # Check if this is a mapping target
            is_mapping_target = False
            for source_field in all_source_fields:
                for key, mapping_list in engine.field_mappings.items():
                    for mapping in mapping_list:
                        if mapping.ecc_field == source_field and mapping.s4_field == measure:
                            if mapping.s4_table in remed_data_sources:
                                is_mapping_target = True
                                break
                    if is_mapping_target:
                        break
                if is_mapping_target:
                    break

            # Check if this is a semantic renaming target
            is_semantic_target = False
            if not is_mapping_target and semantic_renamings:
                for source_field, renamed_field in semantic_renamings.items():
                    if renamed_field == measure and source_field in all_source_fields:
                        is_semantic_target = True
                        break

            # Check if this is a type mismatch (field exists in source but as different type)
            is_type_mismatch = measure in source_view_data['attributes']

            if not is_mapping_target and not is_semantic_target and not is_type_mismatch:
                # This is a surplus field
                target_hidden = 'Y' if remed_hidden_status.get(measure, False) else 'N'
                surplus_row = ['', '', '', '', 'N/A',
                             measure, remed_descriptions.get(measure, ''), remed_lineage.get(measure, ''), 'MEASURE', target_hidden,
                             'N/A', 'N/A', 'N/A', 'N/A']
                detailed_rows.append(surplus_row)

    return detailed_rows


def generate_union_view_comparison(engine: RemediationMappingEngine, input_view_data: list,
                                 remediated_view_path: str, remed_attributes: set, remed_measures: set,
                                 semantic_renamings: dict = None, debug: bool = False) -> list:
    """Generate union comparison data for all source views combined"""

    # Get data sources from remediated view
    remed_data_sources = resolve_recursive_data_sources(engine, remediated_view_path)

    # Extract descriptions, hidden status, and lineage from remediated view
    remed_descriptions = extract_field_descriptions(remediated_view_path, debug)
    remed_hidden_status = extract_field_hidden_status(remediated_view_path, debug)
    remed_lineage = extract_field_source_lineage(remediated_view_path, debug)

    # Create union of all source fields
    union_attributes = set()
    union_measures = set()
    # Store descriptions and hidden status from each view separately
    union_descriptions_list = []
    union_hidden_status_list = []
    # Store lineage from each view as a list - union_lineage_list[i] contains lineage for view i
    union_lineage_list = []

    for view_data in input_view_data:
        union_attributes.update(view_data['attributes'])
        union_measures.update(view_data['measures'])

        # Extract descriptions, hidden status, and lineage from this source view
        source_descriptions = extract_field_descriptions(view_data['view_path'], debug)
        source_hidden_status = extract_field_hidden_status(view_data['view_path'], debug)
        source_lineage = extract_field_source_lineage(view_data['view_path'], debug)

        union_descriptions_list.append(source_descriptions)
        union_hidden_status_list.append(source_hidden_status)
        union_lineage_list.append(source_lineage)

    # Reconcile descriptions: select longest non-empty description for each field
    union_descriptions = {}
    all_union_fields = union_attributes.union(union_measures)
    for field in all_union_fields:
        descriptions = [desc_dict.get(field, '') for desc_dict in union_descriptions_list]
        non_empty_descriptions = [d for d in descriptions if d]
        if non_empty_descriptions:
            # Select the longest description
            union_descriptions[field] = max(non_empty_descriptions, key=len)
        else:
            union_descriptions[field] = ''

    # Create union hidden status map: field_name -> list of hidden statuses (one per view)
    union_hidden_status = {}
    for field in all_union_fields:
        hidden_values = []
        for hidden_dict in union_hidden_status_list:
            hidden_values.append(hidden_dict.get(field, False))
        union_hidden_status[field] = hidden_values

    # Create union lineage map: field_name -> list of lineage values (one per view)
    union_lineage_map = {}
    for field in all_union_fields:
        lineage_values = []
        for source_lineage in union_lineage_list:
            lineage_values.append(source_lineage.get(field, ''))
        union_lineage_map[field] = lineage_values

    detailed_rows = []

    # Process union attributes
    for attr in sorted(union_attributes):
        row_data = process_field_comparison(engine, attr, 'ATTRIBUTE', union_descriptions, remed_descriptions,
                                          remed_attributes, remed_measures, remed_data_sources, union_hidden_status,
                                          remed_hidden_status, semantic_renamings, union_lineage_map, remed_lineage, debug)
        detailed_rows.append(row_data)

    # Process union measures
    for measure in sorted(union_measures):
        row_data = process_field_comparison(engine, measure, 'MEASURE', union_descriptions, remed_descriptions,
                                          remed_attributes, remed_measures, remed_data_sources, union_hidden_status,
                                          remed_hidden_status, semantic_renamings, union_lineage_map, remed_lineage, debug)
        detailed_rows.append(row_data)

    # Add surplus fields from remediated view (fields that don't exist in union)
    all_union_fields = union_attributes.union(union_measures)

    # Surplus attributes
    for attr in remed_attributes:
        if attr not in all_union_fields:
            # Check if this is a mapping target
            is_mapping_target = False
            for source_field in all_union_fields:
                for key, mapping_list in engine.field_mappings.items():
                    for mapping in mapping_list:
                        if mapping.ecc_field == source_field and mapping.s4_field == attr:
                            if mapping.s4_table in remed_data_sources:
                                is_mapping_target = True
                                break
                    if is_mapping_target:
                        break
                if is_mapping_target:
                    break

            # Check if this is a semantic renaming target
            is_semantic_target = False
            if not is_mapping_target and semantic_renamings:
                for source_field, renamed_field in semantic_renamings.items():
                    if renamed_field == attr and source_field in all_union_fields:
                        is_semantic_target = True
                        break

            # Check if this is a type mismatch (field exists in union but as different type)
            is_type_mismatch = attr in union_measures

            if not is_mapping_target and not is_semantic_target and not is_type_mismatch:
                # This is a surplus field
                target_hidden = 'Y' if remed_hidden_status.get(attr, False) else 'N'
                surplus_row = ['', '', '', 'N/A', attr, remed_descriptions.get(attr, ''), 'ATTRIBUTE', target_hidden, 'N/A', 'N/A', 'N/A', 'N/A']
                detailed_rows.append(surplus_row)

    # Surplus measures
    for measure in remed_measures:
        if measure not in all_union_fields:
            # Check if this is a mapping target
            is_mapping_target = False
            for source_field in all_union_fields:
                for key, mapping_list in engine.field_mappings.items():
                    for mapping in mapping_list:
                        if mapping.ecc_field == source_field and mapping.s4_field == measure:
                            if mapping.s4_table in remed_data_sources:
                                is_mapping_target = True
                                break
                    if is_mapping_target:
                        break
                if is_mapping_target:
                    break

            # Check if this is a semantic renaming target
            is_semantic_target = False
            if not is_mapping_target and semantic_renamings:
                for source_field, renamed_field in semantic_renamings.items():
                    if renamed_field == measure and source_field in all_union_fields:
                        is_semantic_target = True
                        break

            # Check if this is a type mismatch (field exists in union but as different type)
            is_type_mismatch = measure in union_attributes

            if not is_mapping_target and not is_semantic_target and not is_type_mismatch:
                # This is a surplus field
                target_hidden = 'Y' if remed_hidden_status.get(measure, False) else 'N'
                surplus_row = ['', '', '', 'N/A', measure, remed_descriptions.get(measure, ''), 'MEASURE', target_hidden, 'N/A', 'N/A', 'N/A', 'N/A']
                detailed_rows.append(surplus_row)

    return detailed_rows


def generate_remediation_report(engine: RemediationMappingEngine, num_inputs: int, output_file: str,
                              semantic_renaming_file: str = None, debug: bool = False):
    """Generate a comprehensive remediation report"""

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

    # Generate report data
    print("Generating remediation report...")

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

    # Create summary data
    summary_data = []

    # Add input view statistics
    for view_data in input_view_data:
        # Extract hidden status for this view
        source_hidden_status = extract_field_hidden_status(view_data['view_path'], debug)
        source_hidden_count = sum(1 for field in list(view_data['attributes']) + list(view_data['measures'])
                                if source_hidden_status.get(field, False))

        summary_data.append({
            'View Type': f'Source ECC View {view_data["view_number"]}',
            'View Name': view_data['view_name'],
            'Attributes': len(view_data['attributes']),
            'Measures': len(view_data['measures']),
            'Total Columns': len(view_data['attributes']) + len(view_data['measures']),
            'Hidden Columns': source_hidden_count
        })

    # Add remediated view statistics
    remed_hidden_status = extract_field_hidden_status(selected_remediated, debug)
    remed_hidden_count = sum(1 for field in list(remed_attributes) + list(remed_measures)
                           if remed_hidden_status.get(field, False))

    summary_data.append({
        'View Type': 'Remediated S4 View',
        'View Name': os.path.basename(selected_remediated),
        'Attributes': len(remed_attributes),
        'Measures': len(remed_measures),
        'Total Columns': len(remed_attributes) + len(remed_measures),
        'Hidden Columns': remed_hidden_count
    })

    # Setup output directory
    reports_dir = setup_output_directory(script_dir)
    output_filename = os.path.basename(output_file)
    output_file = str(reports_dir / output_filename)

    # Generate Excel output
    # If CSV extension provided, convert to xlsx
    if output_file.endswith('.csv'):
        output_file = output_file.replace('.csv', '.xlsx')
        print(f"Note: Converting to Excel format: {output_file}")

    # Create Excel writer object for multiple sheets
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Write summary sheet
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

        # Auto-adjust column widths for Summary sheet
        summary_worksheet = writer.sheets['Summary']
        for column in summary_worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)  # Add padding but cap at 50
            summary_worksheet.column_dimensions[column_letter].width = adjusted_width

        # Create individual sheets for each input view
        for view_data in input_view_data:
            sheet_name = f"Source_ECC_View_{view_data['view_number']}"

            # Generate detailed comparison data for this view
            detailed_data = generate_detailed_view_comparison(engine, view_data, selected_remediated, remed_attributes, remed_measures, semantic_renamings, debug)

            if detailed_data:
                # Create header rows
                source_view_name = view_data['view_name'].replace('.calculationview', '')
                target_view_name = os.path.basename(selected_remediated).replace('.calculationview', '')

                # Create the header structure (added SOURCE_FIELD and HIDDEN columns)
                # Source: cols 0-4, Target: cols 5-9, CHECK: cols 10-13
                header_row_1 = [source_view_name, '', '', '', '', target_view_name, '', '', '', '', 'CHECK', '', '', '']
                header_row_2 = [
                    'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'SOURCE_FIELD', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
                    'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'SOURCE_FIELD', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
                    'RENAME (Y/N)', 'MATCHING_DESCRIPTION (Y/N)', 'MATCHING_TYPE (Y/N)', 'MATCHING_HIDDEN (Y/N)'
                ]

                # Create DataFrame with proper structure
                df_data = [header_row_1, header_row_2] + detailed_data
                detailed_df = pd.DataFrame(df_data)
                detailed_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)

                # Auto-adjust column widths
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)  # Add padding but cap at 50
                    worksheet.column_dimensions[column_letter].width = adjusted_width

        # Add union sheet if multiple inputs
        if len(input_view_data) > 1:
            union_sheet_name = "Union_All_Sources"
            union_data = generate_union_view_comparison(engine, input_view_data, selected_remediated, remed_attributes, remed_measures, semantic_renamings, debug)

            if union_data:
                # Create header rows for union sheet
                union_header_row_1 = ['All Source Views (Union)', '', '', '', '', os.path.basename(selected_remediated).replace('.calculationview', ''), '', '', '', '', 'CHECK', '', '', '']
                union_header_row_2 = [
                    'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'SOURCE_FIELD', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
                    'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'SOURCE_FIELD', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
                    'RENAME (Y/N)', 'MATCHING_DESCRIPTION (Y/N)', 'MATCHING_TYPE (Y/N)', 'MATCHING_HIDDEN (Y/N)'
                ]

                # Create DataFrame with proper structure
                union_df_data = [union_header_row_1, union_header_row_2] + union_data
                union_df = pd.DataFrame(union_df_data)
                union_df.to_excel(writer, sheet_name=union_sheet_name, index=False, header=False)

                # Auto-adjust column widths for union sheet
                union_worksheet = writer.sheets[union_sheet_name]
                for column in union_worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)  # Add padding but cap at 50
                    union_worksheet.column_dimensions[column_letter].width = adjusted_width

    print(f"Remediation report generated: {output_file}")


def load_batch_mappings(yaml_file: str) -> Dict[str, List[str]]:
    """Load batch mapping configuration from YAML file"""
    try:
        with open(yaml_file, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('mappings', {})
    except FileNotFoundError:
        print(f"Error: Batch mapping file not found: {yaml_file}")
        return {}
    except yaml.YAMLError as e:
        print(f"Error: Failed to parse YAML file: {e}")
        return {}


def generate_batch_remediation_reports(engine: RemediationMappingEngine, num_inputs: int,
                                       semantic_renaming_file: str = None, debug: bool = False):
    """Generate remediation reports for all calculation views using YAML mapping configuration"""

    # Get user to select input directory
    print("\n=== Batch Report Generation Mode ===")
    input_directory = select_directory(prompt="INPUT (source)", exclude_dirs=['logical_model_renamings'])

    # Find calculation views in selected directory
    input_views = get_calculation_views(input_directory)

    if not input_views:
        print(f"No calculation views found in directory: {input_directory}")
        return

    print(f"\nFound {len(input_views)} INPUT calculation view(s):")
    for view in input_views:
        print(f"  - {os.path.basename(view)}")

    # Get user to select remediated/output directory
    remediated_directory = select_directory(prompt="OUTPUT (remediated)", exclude_dirs=['logical_model_renamings'])

    # Find calculation views in remediated directory
    remediated_views = get_calculation_views(remediated_directory)

    if not remediated_views:
        print(f"No calculation views found in directory: {remediated_directory}")
        return

    print(f"\nFound {len(remediated_views)} OUTPUT calculation view(s):")
    for view in remediated_views:
        print(f"  - {os.path.basename(view)}")

    # Load view mapping configuration
    script_dir = Path(__file__).parent.parent.parent  # Project root
    yaml_file = script_dir / "inputs/view_mappings.yaml"

    if not yaml_file.exists():
        print(f"\nError: View mapping configuration not found: {yaml_file}")
        print("Please create a view_mappings.yaml file in the inputs/ directory.")
        return

    mappings = load_batch_mappings(str(yaml_file))

    if not mappings:
        print("\nError: No mappings found in view_mappings.yaml")
        return

    print(f"\nLoaded {len(mappings)} mapping(s) from configuration:")
    for remediated, inputs in mappings.items():
        print(f"  {remediated} <- {', '.join(inputs)}")

    # Create a dict of view name (without extension) to full path
    input_views_dict = {}
    for view_path in input_views:
        view_name = os.path.basename(view_path).replace('.calculationview', '')
        input_views_dict[view_name] = view_path

    remediated_views_dict = {}
    for view_path in remediated_views:
        view_name = os.path.basename(view_path).replace('.calculationview', '')
        remediated_views_dict[view_name] = view_path

    # Setup output directory
    reports_dir = setup_output_directory(script_dir)

    # Process mappings from YAML
    print(f"\n=== Processing Mappings ===")

    processed_count = 0
    skipped_count = 0
    error_count = 0

    for remediated_name, input_names in mappings.items():
        # Find remediated view
        if remediated_name not in remediated_views_dict:
            print(f"\n✗ Skipping: Remediated view '{remediated_name}' not found in inputs/cv_remediated/")
            skipped_count += 1
            continue

        selected_remediated = remediated_views_dict[remediated_name]

        # Find all input views for this mapping
        matched_input_views = []
        missing_inputs = []

        for input_name in input_names:
            if input_name in input_views_dict:
                matched_input_views.append((input_name, input_views_dict[input_name]))
            else:
                missing_inputs.append(input_name)

        if missing_inputs:
            print(f"\n⚠ Warning: Some input views not found for {remediated_name}:")
            for missing in missing_inputs:
                print(f"    - {missing}")

        if not matched_input_views:
            print(f"✗ Skipping: No input views found for {remediated_name}")
            skipped_count += 1
            continue

        # Process this mapping
        print(f"\n[{processed_count + 1}] Processing mapping: {remediated_name}")
        print(f"    Input view(s): {', '.join([name for name, _ in matched_input_views])}")

        # Automatically load semantic renamings based on remediated view name
        # Try to find a matching CSV file in inputs/renamings/ (try both exact case and lowercase)
        renaming_file_path = script_dir / "inputs" / "renamings" / f"{remediated_name}.csv"
        if not renaming_file_path.exists():
            renaming_file_path = script_dir / "inputs" / "renamings" / f"{remediated_name.lower()}.csv"

        if renaming_file_path.exists():
            print(f"    Loading semantic renamings: {renaming_file_path.name}")
            semantic_renamings = load_semantic_renamings(str(renaming_file_path), debug)
        elif semantic_renaming_file:
            # Use the globally provided semantic renaming file if no view-specific one exists
            semantic_renamings = load_semantic_renamings(semantic_renaming_file, debug)
        else:
            semantic_renamings = {}

        # Generate output filename based on remediated view name
        output_filename = f"{remediated_name}.xlsx"
        output_file = str(reports_dir / output_filename)

        try:
            # Extract data from all input views
            input_view_data = []
            for idx, (input_name, input_view_path) in enumerate(matched_input_views, 1):
                attributes, measures = engine.extract_output_columns(input_view_path)
                view_data = {
                    'view_path': input_view_path,
                    'view_name': os.path.basename(input_view_path),
                    'view_number': idx,
                    'attributes': attributes,
                    'measures': measures
                }
                input_view_data.append(view_data)

            # Extract data from remediated view
            remed_attributes, remed_measures = engine.extract_output_columns(selected_remediated)

            # Create summary data
            summary_data = []

            # Add input view statistics
            for view_data in input_view_data:
                source_hidden_status = extract_field_hidden_status(view_data['view_path'], debug)
                source_hidden_count = sum(1 for field in list(view_data['attributes']) + list(view_data['measures'])
                                        if source_hidden_status.get(field, False))

                summary_data.append({
                    'View Type': f'Source ECC View {view_data["view_number"]}',
                    'View Name': view_data['view_name'],
                    'Attributes': len(view_data['attributes']),
                    'Measures': len(view_data['measures']),
                    'Total Columns': len(view_data['attributes']) + len(view_data['measures']),
                    'Hidden Columns': source_hidden_count
                })

            # Add remediated view statistics
            remed_hidden_status = extract_field_hidden_status(selected_remediated, debug)
            remed_hidden_count = sum(1 for field in list(remed_attributes) + list(remed_measures)
                                   if remed_hidden_status.get(field, False))

            summary_data.append({
                'View Type': 'Remediated S4 View',
                'View Name': os.path.basename(selected_remediated),
                'Attributes': len(remed_attributes),
                'Measures': len(remed_measures),
                'Total Columns': len(remed_attributes) + len(remed_measures),
                'Hidden Columns': remed_hidden_count
            })

            # Generate Excel output
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Write summary sheet
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)

                # Auto-adjust column widths for Summary sheet
                summary_worksheet = writer.sheets['Summary']
                for column in summary_worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    summary_worksheet.column_dimensions[column_letter].width = adjusted_width

                # Create individual sheets for each input view
                for view_data in input_view_data:
                    sheet_name = f"Source_ECC_View_{view_data['view_number']}"

                    # Generate detailed comparison data for this view
                    detailed_data = generate_detailed_view_comparison(engine, view_data, selected_remediated,
                                                                     remed_attributes, remed_measures,
                                                                     semantic_renamings, debug)

                    if detailed_data:
                        # Create header rows
                        source_view_name = view_data['view_name'].replace('.calculationview', '')
                        target_view_name = os.path.basename(selected_remediated).replace('.calculationview', '')

                        header_row_1 = [source_view_name, '', '', '', '', target_view_name, '', '', '', '', 'CHECK', '', '', '']
                        header_row_2 = [
                            'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'SOURCE_FIELD', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
                            'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'SOURCE_FIELD', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
                            'RENAME (Y/N)', 'MATCHING_DESCRIPTION (Y/N)', 'MATCHING_TYPE (Y/N)', 'MATCHING_HIDDEN (Y/N)'
                        ]

                        # Create DataFrame with proper structure
                        df_data = [header_row_1, header_row_2] + detailed_data
                        detailed_df = pd.DataFrame(df_data)
                        detailed_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)

                        # Auto-adjust column widths
                        worksheet = writer.sheets[sheet_name]
                        for column in worksheet.columns:
                            max_length = 0
                            column_letter = column[0].column_letter
                            for cell in column:
                                try:
                                    if len(str(cell.value)) > max_length:
                                        max_length = len(str(cell.value))
                                except:
                                    pass
                            adjusted_width = min(max_length + 2, 50)
                            worksheet.column_dimensions[column_letter].width = adjusted_width

                # Add union sheet if multiple inputs
                if len(input_view_data) > 1:
                    union_sheet_name = "Union_All_Sources"
                    union_data = generate_union_view_comparison(engine, input_view_data, selected_remediated,
                                                               remed_attributes, remed_measures,
                                                               semantic_renamings, debug)

                    if union_data:
                        # Create header rows for union sheet
                        union_header_row_1 = ['All Source Views (Union)', '', '', '', '', os.path.basename(selected_remediated).replace('.calculationview', ''), '', '', '', '', 'CHECK', '', '', '']
                        union_header_row_2 = [
                            'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'SOURCE_FIELD', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
                            'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'SOURCE_FIELD', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
                            'RENAME (Y/N)', 'MATCHING_DESCRIPTION (Y/N)', 'MATCHING_TYPE (Y/N)', 'MATCHING_HIDDEN (Y/N)'
                        ]

                        # Create DataFrame with proper structure
                        union_df_data = [union_header_row_1, union_header_row_2] + union_data
                        union_df = pd.DataFrame(union_df_data)
                        union_df.to_excel(writer, sheet_name=union_sheet_name, index=False, header=False)

                        # Auto-adjust column widths for union sheet
                        union_worksheet = writer.sheets[union_sheet_name]
                        for column in union_worksheet.columns:
                            max_length = 0
                            column_letter = column[0].column_letter
                            for cell in column:
                                try:
                                    if len(str(cell.value)) > max_length:
                                        max_length = len(str(cell.value))
                                except:
                                    pass
                            adjusted_width = min(max_length + 2, 50)
                            union_worksheet.column_dimensions[column_letter].width = adjusted_width

            print(f"    ✓ Report generated: {output_filename}")
            processed_count += 1

        except Exception as e:
            print(f"    ✗ Error processing {remediated_name}: {str(e)}")
            if debug:
                import traceback
                traceback.print_exc()
            error_count += 1

    print(f"\n=== Batch Processing Complete ===")
    print(f"Successfully processed: {processed_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Errors: {error_count}")
    print(f"Reports saved to: {reports_dir}")