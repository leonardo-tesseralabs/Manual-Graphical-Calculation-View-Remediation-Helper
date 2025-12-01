#!/usr/bin/env python3
"""
Remediation Mapping Engine

This program implements the requirements specified in the PRD for converting
calculation view table mappings from ECC to S/4HANA based on field-level mappings.

Input:
- Calculation view (selected via menu)
- Universal list of custom tables (SCHEMA.TABLE format)
- Universal list of transparent tables (one per line)
- Universal source-of-truth mappings (CSV with ecc_table, ecc_field, s4_table, s4_field, FLAGGED_FOR_REVIEW)

Output:
- Adjacency list representation of original tables
- Adjacency list representation of remapped tables
- Summary of all changes
- List of fields flagged for manual review
"""

import os
import sys
import csv
import xml.etree.ElementTree as ET
import argparse
import subprocess
import shutil
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict, deque
from contextlib import contextmanager


@contextmanager
def pager_output():
    """Context manager to pipe output to a pager (less/more) for better readability"""
    # Try to find a suitable pager
    pager_cmd = None
    
    # Check for 'less' first (better features)
    if shutil.which('less'):
        pager_cmd = ['less', '-R', '-S', '-F', '-X']  # -R: colors, -S: no wrap, -F: quit if one screen, -X: no clear
    elif shutil.which('more'):
        pager_cmd = ['more']
    
    if pager_cmd:
        try:
            # Start pager process
            pager_process = subprocess.Popen(
                pager_cmd, 
                stdin=subprocess.PIPE, 
                text=True,
                bufsize=1
            )
            
            # Temporarily redirect stdout to pager
            original_stdout = sys.stdout
            sys.stdout = pager_process.stdin
            
            yield
            
        except Exception as e:
            # If pager fails, fall back to normal output
            sys.stdout = original_stdout
            print(f"Pager failed: {e}, falling back to normal output")
            yield
        finally:
            # Restore original stdout and close pager
            sys.stdout = original_stdout
            if pager_process.stdin and not pager_process.stdin.closed:
                pager_process.stdin.close()
            pager_process.wait()
    else:
        # No pager available, use normal output
        yield


@dataclass
class FieldMapping:
    """Represents a field mapping from ECC to S/4HANA"""
    ecc_table: str
    ecc_field: str
    s4_table: str
    s4_field: str
    flagged_for_review: bool


@dataclass
class TableMappingResult:
    """Result of table mapping analysis"""
    case: str  # "1.1", "1.2", "2.1", "2.2"
    original_table: str
    target_tables: List[str]
    mapped_fields: List[FieldMapping]
    missing_fields: List[str]
    is_fragmented: bool = False


@dataclass
class DataSource:
    """Represents a data source in the calculation view"""
    id: str
    type: str
    schema: str
    table_name: str
    fields: Set[str]


@dataclass
class Node:
    """Represents a node in the adjacency list"""
    name: str
    type: str  # 'table', 'operation', 'missing'
    dependencies: List[str]  # What this node depends on (sources FROM)
    dependents: List[str]    # What depends on this node (sources TO)


class RemediationMappingEngine:
    """Main engine for remediation mapping"""
    
    def __init__(self, custom_tables_file: str, transparent_tables_file: str, mappings_file: str, override_mappings_file: str = None):
        # Initialize input data structures
        self.custom_tables = self._load_custom_tables(custom_tables_file)
        self.transparent_tables = self._load_transparent_tables(transparent_tables_file)
        self.field_mappings = self._load_field_mappings(mappings_file, override_mappings_file)
        
        # Create M-TA (unique source table names from mappings, excluding custom and transparent)
        self.m_ta = self._create_m_ta()
        
        # Results
        self.original_adjacency_list: Dict[str, Node] = {}
        self.remapped_adjacency_list: Dict[str, Node] = {}
        self.summary: List[str] = []
        self.csv_rows_written = 0
        self.flagged_fields: List[FieldMapping] = []
        self.actual_changes: int = 0  # Track only actual transformations, not unchanged tables
        
        # Track table schemas for custom table matching
        self.table_schemas: Dict[str, str] = {}
        
    def _load_custom_tables(self, file_path: str) -> Set[str]:
        """Load custom tables from file, ignoring comment lines starting with #"""
        custom_tables = set()
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if '.*' in line:
                            # Handle SCHEMA.* pattern - for simplicity, we'll store as-is
                            custom_tables.add(line)
                        else:
                            custom_tables.add(line)
            print(f"Loaded {len(custom_tables)} custom table patterns")
            return custom_tables
        except FileNotFoundError:
            print(f"Warning: Custom tables file not found: {file_path}")
            return set()
    
    def _load_transparent_tables(self, file_path: str) -> Set[str]:
        """Load transparent tables from file, ignoring comment lines starting with #"""
        transparent_tables = set()
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        transparent_tables.add(line)
            print(f"Loaded {len(transparent_tables)} transparent tables")
            return transparent_tables
        except FileNotFoundError:
            print(f"Warning: Transparent tables file not found: {file_path}")
            return set()
    
    def _load_field_mappings(self, file_path: str, override_file_path: str = None) -> Dict[Tuple[str, str], List[FieldMapping]]:
        """Load field mappings from CSV file, with optional override mappings"""
        mappings = defaultdict(list)
        
        # Load main mappings file
        try:
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if all(key in row for key in ['ecc_table', 'ecc_field', 's4_table', 's4_field', 'FLAGGED_FOR_REVIEW']):
                        mapping = FieldMapping(
                            ecc_table=row['ecc_table'],
                            ecc_field=row['ecc_field'],
                            s4_table=row['s4_table'],
                            s4_field=row['s4_field'],
                            flagged_for_review=row['FLAGGED_FOR_REVIEW'].upper() == 'Y'
                        )
                        key = (mapping.ecc_table, mapping.ecc_field)
                        mappings[key].append(mapping)
            
            total_mappings = sum(len(v) for v in mappings.values())
            print(f"Loaded {total_mappings} field mappings for {len(mappings)} unique field combinations")
        except FileNotFoundError:
            print(f"Error: Field mappings file not found: {file_path}")
            sys.exit(1)
        
        # Load and apply override mappings if provided
        if override_file_path:
            try:
                override_count = 0
                with open(override_file_path, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Check for required columns (same as main mappings file)
                        if all(key in row for key in ['ecc_table', 'ecc_field', 's4_table', 's4_field', 'FLAGGED_FOR_REVIEW']):
                            override_mapping = FieldMapping(
                                ecc_table=row['ecc_table'],
                                ecc_field=row['ecc_field'],
                                s4_table=row['s4_table'],
                                s4_field=row['s4_field'],
                                flagged_for_review=row['FLAGGED_FOR_REVIEW'].upper() == 'Y'
                            )
                            key = (override_mapping.ecc_table, override_mapping.ecc_field)
                            # Override existing mappings completely
                            mappings[key] = [override_mapping]
                            override_count += 1
                        else:
                            print(f"Error: Override mappings file has incorrect column format. Expected: ecc_table, ecc_field, s4_table, s4_field, FLAGGED_FOR_REVIEW")
                            sys.exit(1)
                print(f"Applied {override_count} override mappings")
            except FileNotFoundError:
                print(f"Override mappings file not found: {override_file_path}")
        
        return dict(mappings)
    
    def _create_m_ta(self) -> Set[str]:
        """Create M-TA: unique source table names from mappings (excluding custom and transparent)"""
        source_tables = set()
        for (ecc_table, _), _ in self.field_mappings.items():
            source_tables.add(ecc_table)
        
        # Remove custom and transparent tables
        # Note: We can't use _is_custom_table here since table_schemas isn't populated yet
        # So we'll handle this more simply - assuming mappings don't contain custom table entries
        source_tables -= self.transparent_tables
        
        print(f"M-TA contains {len(source_tables)} unique source tables from mappings")
        return source_tables
    
    def parse_calculation_view(self, xml_file_path: str, debug: bool = False) -> Tuple[Set[str], Set[str], Set[str]]:
        """Parse calculation view XML and extract data sources"""
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            # Extract data sources (DS)
            data_sources = set()
            table_sources = set()  # DS-TA: subset of DS that only includes tables
            calc_view_sources = set()  # Calculation view data sources
            
            # Find all DataSource elements - try different namespace approaches
            datasource_elements = []
            
            # Try with namespace
            ns = {'calc': 'http://www.sap.com/ndb/BiModelCalculation.ecore'}
            datasource_elements.extend(root.findall('.//calc:dataSources/calc:DataSource', ns))
            
            # Try without namespace prefix (in case it's using default namespace)
            if not datasource_elements:
                datasource_elements.extend(root.findall('.//dataSources/DataSource'))
            
            # Try finding DataSource elements anywhere in the tree
            if not datasource_elements:
                datasource_elements.extend(root.findall('.//DataSource'))
            
            for ds in datasource_elements:
                ds_type = ds.get('type')
                ds_id = ds.get('id')
                
                if ds_id:
                    data_sources.add(ds_id)
                    
                    # Check if this is a table (not another calculation view)
                    if ds_type == 'DATA_BASE_TABLE':
                        # Extract table name from columnObject
                        col_obj = ds.find('.//columnObject')
                        if col_obj is not None:
                            table_name = col_obj.get('columnObjectName')
                            if table_name:
                                table_sources.add(table_name)
                                # Also store schema info for custom table matching
                                schema_name = col_obj.get('schemaName')
                                if schema_name and hasattr(self, 'table_schemas'):
                                    self.table_schemas[table_name] = schema_name
                    elif ds_type == 'CALCULATION_VIEW':
                        # This is another calculation view
                        calc_view_sources.add(ds_id)
            
            if debug:
                print(f"Found {len(data_sources)} data sources, {len(table_sources)} table sources, {len(calc_view_sources)} calculation view sources")
            return data_sources, table_sources, calc_view_sources
            
        except ET.ParseError as e:
            print(f"Error parsing XML file: {e}")
            sys.exit(1)
        except FileNotFoundError:
            print(f"Error: XML file not found: {xml_file_path}")
            sys.exit(1)
    
    def extract_measures_from_calculation_view(self, xml_file_path: str) -> Dict[str, Dict[str, str]]:
        """Extract measure fields (non-attributes with aggregation types) from calculation view
        Returns: Dict[node_name, Dict[field_name, aggregation_type]]"""
        measures_by_node = {}
        
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            # Find all calculation views that have aggregation
            ns = {'calc': 'http://www.sap.com/ndb/BiModelCalculation.ecore'}
            calc_view_elements = []
            calc_view_elements.extend(root.findall('.//calc:calculationView', ns))
            if not calc_view_elements:
                calc_view_elements.extend(root.findall('.//calculationView'))
            
            for calc_view in calc_view_elements:
                view_id = calc_view.get('id')
                view_type = calc_view.get('{http://www.w3.org/2001/XMLSchema-instance}type')
                
                if not view_id:
                    continue
                
                measures = {}
                
                # Look for viewAttributes with aggregationType
                view_attrs = calc_view.findall('.//viewAttribute')
                for attr in view_attrs:
                    attr_id = attr.get('id')
                    aggregation_type = attr.get('aggregationType')
                    
                    if attr_id and aggregation_type:
                        measures[attr_id] = aggregation_type
                
                if measures:
                    measures_by_node[view_id] = measures
            
            return measures_by_node
            
        except ET.ParseError as e:
            print(f"Error parsing XML for measures extraction: {e}")
            return {}
        except FileNotFoundError:
            print(f"Error: XML file not found: {xml_file_path}")
            return {}

    def extract_output_columns(self, xml_file_path: str) -> Tuple[Set[str], Set[str]]:
        """Extract output attributes and measures from calculation view logical model"""
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            attributes = set()
            measures = set()
            
            # Find logical model - try different namespace approaches
            logical_model = None
            
            # Try with namespace
            ns = {'calc': 'http://www.sap.com/ndb/BiModelCalculation.ecore'}
            logical_model = root.find('.//calc:logicalModel', ns)
            
            # Try without namespace prefix
            if logical_model is None:
                logical_model = root.find('.//logicalModel')
            
            if logical_model is not None:
                # Extract attributes
                for attr in logical_model.findall('.//attribute'):
                    attr_id = attr.get('id')
                    if attr_id:
                        attributes.add(attr_id)
                
                # Extract measures from baseMeasures
                for measure in logical_model.findall('.//baseMeasures/measure'):
                    measure_id = measure.get('id')
                    if measure_id:
                        measures.add(measure_id)
            
            return attributes, measures
            
        except Exception as e:
            print(f"Error extracting output columns from {xml_file_path}: {e}")
            return set(), set()

    def extract_field_usage(self, xml_file_path: str, ds_ta: Set[str]) -> Dict[str, Set[str]]:
        """Extract which fields are actually used from each table in the calculation view"""
        table_fields = {table: set() for table in ds_ta}
        
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            # Find field usage from mapping elements
            ns = {'calc': 'http://www.sap.com/ndb/BiModelCalculation.ecore'}
            
            # Find all input elements and their mappings
            input_elements = []
            input_elements.extend(root.findall('.//calc:input', ns))
            if not input_elements:
                input_elements.extend(root.findall('.//input'))
            
            for input_elem in input_elements:
                node_ref = input_elem.get('node')
                if node_ref and node_ref.startswith('#'):
                    table_name = node_ref[1:]  # Remove '#' prefix
                    if table_name in table_fields:
                        # Find all mappings within this input element
                        mappings_in_input = []
                        mappings_in_input.extend(input_elem.findall('.//calc:mapping', ns))
                        if not mappings_in_input:
                            mappings_in_input.extend(input_elem.findall('.//mapping'))
                        
                        for mapping in mappings_in_input:
                            source_field = mapping.get('source')
                            if source_field:
                                table_fields[table_name].add(source_field)
            
            # Also check viewAttributes in DataSource elements for fields referenced
            datasource_elements = []
            datasource_elements.extend(root.findall('.//calc:DataSource', ns))
            if not datasource_elements:
                datasource_elements.extend(root.findall('.//DataSource'))
            
            for ds in datasource_elements:
                ds_id = ds.get('id')
                if ds_id and ds_id in table_fields:
                    # Check for viewAttributes that aren't "allViewAttributes"
                    view_attrs = ds.find('viewAttributes')
                    if view_attrs is not None:
                        all_view_attrs = view_attrs.get('allViewAttributes')
                        if all_view_attrs != 'true':
                            # Look for specific viewAttribute elements
                            for attr in view_attrs.findall('.//viewAttribute'):
                                attr_id = attr.get('id')
                                if attr_id:
                                    table_fields[ds_id].add(attr_id)
            
            # Print summary of extracted field usage
            for table, fields in table_fields.items():
                if fields:
                    print(f"  {table}: {len(fields)} fields used ({', '.join(sorted(fields))})")
                else:
                    print(f"  {table}: using all fields (allViewAttributes=true)")
            
            return table_fields
            
        except ET.ParseError as e:
            print(f"Error parsing XML for field extraction: {e}")
            return table_fields
        except FileNotFoundError:
            print(f"Error: XML file not found: {xml_file_path}")
            return table_fields
    
    def validate_ds_ta(self, ds_ta: Set[str], debug: bool = False) -> bool:
        """Verify that (DS-TA minus U-CT) is a perfect subset of M-TA union U-TT"""
        # Remove custom tables from DS-TA
        ds_ta_filtered = set(ds_ta)
        
        # Remove custom tables from DS-TA using the _is_custom_table method
        ds_ta_filtered = {t for t in ds_ta_filtered if not self._is_custom_table(t)}
        
        # Check if remaining tables are in M-TA or transparent tables
        required_set = self.m_ta.union(self.transparent_tables)
        missing_tables = ds_ta_filtered - required_set
        
        if missing_tables:
            print(f"ERROR: The following tables are missing from mappings or transparent tables:")
            for table in missing_tables:
                print(f"  - {table}")
            return False
        
        if debug:
            print("✓ All non-custom tables in DS-TA have either mappings or are transparent tables")
        return True
    
    def create_original_adjacency_list(self, xml_file_path: str, ds: Set[str], ds_ta: Set[str], debug: bool = False):
        """Create adjacency list representation of the original hierarchy"""
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            # Initialize nodes for data sources
            for table in ds_ta:
                self.original_adjacency_list[table] = Node(
                    name=table,
                    type='table',
                    dependencies=[],
                    dependents=[]
                )
            
            # Process calculation views to understand hierarchy
            calc_view_elements = []
            
            # Try with namespace
            ns = {'calc': 'http://www.sap.com/ndb/BiModelCalculation.ecore'}
            calc_view_elements.extend(root.findall('.//calc:calculationView', ns))
            
            # Try without namespace
            if not calc_view_elements:
                calc_view_elements.extend(root.findall('.//calculationView'))
            
            for calc_view in calc_view_elements:
                view_id = calc_view.get('id')
                if not view_id:
                    continue
                
                dependencies = []
                
                # Find input nodes - try different approaches
                input_elements = []
                input_elements.extend(calc_view.findall('.//calc:input', ns))
                if not input_elements:
                    input_elements.extend(calc_view.findall('.//input'))
                
                for input_node in input_elements:
                    node_ref = input_node.get('node')
                    if node_ref and node_ref.startswith('#'):
                        dep = node_ref[1:]  # Remove '#' prefix
                        dependencies.append(dep)
                
                # Create node for this calculation view
                self.original_adjacency_list[view_id] = Node(
                    name=view_id,
                    type='operation',
                    dependencies=dependencies,
                    dependents=[]
                )
                
                # Update dependents for the nodes this depends on
                for dep in dependencies:
                    if dep in self.original_adjacency_list:
                        self.original_adjacency_list[dep].dependents.append(view_id)
            
            if debug:
                print(f"Created original adjacency list with {len(self.original_adjacency_list)} nodes")
            
        except Exception as e:
            print(f"Error creating original adjacency list: {e}")
    
    def analyze_table_mappings(self, ds_ta: Set[str], table_field_usage: Dict[str, Set[str]] = None) -> List[TableMappingResult]:
        """Analyze table mappings according to the PRD cases"""
        results = []
        
        for table in ds_ta:
            if self._is_custom_table(table) or self._is_transparent_table(table):
                # Skip custom and transparent tables for mapping analysis
                continue
            
            # Find all field mappings for this table
            table_mappings = []
            for (ecc_table, ecc_field), mappings in self.field_mappings.items():
                if ecc_table == table:
                    table_mappings.extend(mappings)
            
            if not table_mappings:
                # Case 1.1: All fields deprecated (no mappings found)
                result = TableMappingResult(
                    case="1.1",
                    original_table=table,
                    target_tables=[],
                    mapped_fields=[],
                    missing_fields=[],
                    is_fragmented=False
                )
                results.append(result)
                continue
            
            # Group mappings by target table
            target_table_groups = defaultdict(list)
            for mapping in table_mappings:
                if mapping.s4_table:  # Only consider non-empty target tables
                    target_table_groups[mapping.s4_table].append(mapping)
            
            unique_target_tables = list(target_table_groups.keys())
            
            if len(unique_target_tables) == 1:
                # Case 1.2: All fields mapped to same target table
                target_table = unique_target_tables[0]
                
                # Check for fields used in CV but not in mappings at all
                missing_fields = []
                if table_field_usage and table in table_field_usage:
                    used_fields = table_field_usage[table]
                    if used_fields:  # Only check if we have specific field usage (not allViewAttributes)
                        mapped_field_names = {mapping.ecc_field for mapping in table_mappings}
                        unmapped_fields = used_fields - mapped_field_names
                        for field in unmapped_fields:
                            missing_fields.append(f"{table}.{field}")
                
                # Check for mappings with empty s4_table/s4_field
                complete_mappings = []
                for mapping in table_mappings:
                    if not mapping.s4_table or not mapping.s4_field:
                        missing_fields.append(f"{mapping.ecc_table}.{mapping.ecc_field}")
                    else:
                        complete_mappings.append(mapping)
                
                if missing_fields:
                    # Actually Case 2.2: Some fields have mappings, others don't
                    result = TableMappingResult(
                        case="2.2",
                        original_table=table,
                        target_tables=[target_table],
                        mapped_fields=complete_mappings,
                        missing_fields=missing_fields,
                        is_fragmented=True
                    )
                else:
                    # True Case 1.2: All fields mapped to same target table
                    result = TableMappingResult(
                        case="1.2",
                        original_table=table,
                        target_tables=[target_table],
                        mapped_fields=table_mappings,
                        missing_fields=[],
                        is_fragmented=False
                    )
                results.append(result)
                
            elif len(unique_target_tables) > 1:
                # Case 2: Multiple target tables
                # Check for missing mappings (fields that map to empty s4_table)
                missing_fields = []
                complete_mappings = []
                
                for mapping in table_mappings:
                    if not mapping.s4_table or not mapping.s4_field:
                        missing_fields.append(f"{mapping.ecc_table}.{mapping.ecc_field}")
                    else:
                        complete_mappings.append(mapping)
                
                # Check for fields used in CV but not in mappings at all
                if table_field_usage and table in table_field_usage:
                    used_fields = table_field_usage[table]
                    if used_fields:  # Only check if we have specific field usage (not allViewAttributes)
                        mapped_field_names = {mapping.ecc_field for mapping in table_mappings}
                        unmapped_fields = used_fields - mapped_field_names
                        for field in unmapped_fields:
                            missing_fields.append(f"{table}.{field}")
                
                if missing_fields:
                    # Case 2.2: Some fields have mappings, others don't
                    result = TableMappingResult(
                        case="2.2",
                        original_table=table,
                        target_tables=unique_target_tables,
                        mapped_fields=complete_mappings,
                        missing_fields=missing_fields,
                        is_fragmented=True
                    )
                else:
                    # Case 2.1: All fields have mappings
                    result = TableMappingResult(
                        case="2.1",
                        original_table=table,
                        target_tables=unique_target_tables,
                        mapped_fields=table_mappings,
                        missing_fields=[],
                        is_fragmented=False
                    )
                
                results.append(result)
        
        return results
    
    def create_remapped_adjacency_list(self, mapping_results: List[TableMappingResult]):
        """Create adjacency list for remapped tables"""
        # Start with original adjacency list as base
        self.remapped_adjacency_list = {}
        
        # Copy all non-table nodes first
        for name, node in self.original_adjacency_list.items():
            if node.type == 'operation':
                self.remapped_adjacency_list[name] = Node(
                    name=name,
                    type=node.type,
                    dependencies=list(node.dependencies),
                    dependents=list(node.dependents)
                )
        
        # Process table mappings
        for result in mapping_results:
            if result.case == "1.1":
                # Deletable table - remove from adjacency list
                self.summary.append(f"DELETED: {result.original_table} (all fields deprecated)")
                self.actual_changes += 1
                
            elif result.case == "1.2":
                # Simple substitution
                target_table = result.target_tables[0]
                self.remapped_adjacency_list[target_table] = Node(
                    name=target_table,
                    type='table',
                    dependencies=[],
                    dependents=[]
                )
                
                # Update any dependencies that referenced the original table
                self._update_dependencies(result.original_table, target_table)
                self.summary.append(f"SUBSTITUTED: {result.original_table} -> {target_table}")
                self.actual_changes += 1
                
            elif result.case in ["2.1", "2.2"]:
                # Multiple target tables - create proper bilateral joins
                target_tables = result.target_tables.copy()
                
                # Add each target table to the adjacency list
                for target_table in target_tables:
                    self.remapped_adjacency_list[target_table] = Node(
                        name=target_table,
                        type='table',
                        dependencies=[],
                        dependents=[]
                    )
                
                if result.is_fragmented:
                    # Case 2.2: Create MISSING_ node and add to tables
                    missing_node_name = f"MISSING_{result.original_table}"
                    self.remapped_adjacency_list[missing_node_name] = Node(
                        name=missing_node_name,
                        type='missing',
                        dependencies=[],
                        dependents=[]
                    )
                    target_tables.append(missing_node_name)
                    
                    self.summary.append(f"FRAGMENTED: {result.original_table} -> {missing_node_name} + {'_'.join(result.target_tables)}")
                    self.summary.append(f"  Missing fields: {', '.join(result.missing_fields)}")
                else:
                    self.summary.append(f"SPLIT: {result.original_table} -> {' + '.join(target_tables)}")
                
                self.actual_changes += 1
                
                # Create bilateral joins for multiple tables
                final_join_node = self._create_bilateral_joins(target_tables, result.original_table)
                
                # Update references to original table to point to final join
                self._update_dependencies(result.original_table, final_join_node)
        
        # Handle custom and transparent tables (map as-is)
        for name, node in self.original_adjacency_list.items():
            if node.type == 'table' and (self._is_custom_table(name) or self._is_transparent_table(name)):
                self.remapped_adjacency_list[name] = Node(
                    name=name,
                    type='table',
                    dependencies=[],
                    dependents=[]
                )
                table_type = "custom" if self._is_custom_table(name) else "transparent"
                self.summary.append(f"UNCHANGED: {name} ({table_type} table)")
    
    def _create_bilateral_joins(self, tables: List[str], original_table: str) -> str:
        """Create bilateral joins for multiple tables and return the final join node name"""
        if len(tables) < 2:
            return tables[0] if tables else ""
        
        if len(tables) == 2:
            # Simple bilateral join
            join_name = f"JOIN_{tables[0]}_{tables[1]}"
            self.remapped_adjacency_list[join_name] = Node(
                name=join_name,
                type='operation',
                dependencies=tables,
                dependents=[]
            )
            
            # Update dependents for joined tables
            for table in tables:
                if table in self.remapped_adjacency_list:
                    self.remapped_adjacency_list[table].dependents.append(join_name)
            
            return join_name
        
        # For 3+ tables, create a hierarchy of bilateral joins
        # Join first two tables
        left_table = tables[0]
        right_table = tables[1]
        join_name = f"JOIN_{left_table}_{right_table}"
        
        self.remapped_adjacency_list[join_name] = Node(
            name=join_name,
            type='operation',
            dependencies=[left_table, right_table],
            dependents=[]
        )
        
        # Update dependents
        for table in [left_table, right_table]:
            if table in self.remapped_adjacency_list:
                self.remapped_adjacency_list[table].dependents.append(join_name)
        
        # Recursively join with remaining tables
        remaining_tables = [join_name] + tables[2:]
        return self._create_bilateral_joins(remaining_tables, original_table)
    
    def _is_custom_table(self, table_name: str) -> bool:
        """Check if a table is a custom table"""
        schema_name = self.table_schemas.get(table_name)
        if not schema_name:
            return False
        
        # Build full schema.table name
        full_table_name = f"{schema_name}.{table_name}"
        
        # Check for exact schema.table match
        if full_table_name in self.custom_tables:
            return True
        
        # Check for schema.* patterns
        for custom_table in self.custom_tables:
            if '.*' in custom_table:
                schema_prefix = custom_table.replace('.*', '')
                if schema_name == schema_prefix:
                    return True
        
        return False
    
    def _is_transparent_table(self, table_name: str) -> bool:
        """Check if a table is a transparent table"""
        return table_name in self.transparent_tables
    
    def _update_dependencies(self, old_name: str, new_name: str):
        """Update dependencies that reference old_name to reference new_name"""
        for node in self.remapped_adjacency_list.values():
            if old_name in node.dependencies:
                node.dependencies = [new_name if dep == old_name else dep for dep in node.dependencies]
            if old_name in node.dependents:
                node.dependents = [new_name if dep == old_name else dep for dep in node.dependents]
    
    def collect_flagged_fields(self, mapping_results: List[TableMappingResult]):
        """Collect fields flagged for manual review"""
        for result in mapping_results:
            # Add mapped fields that are flagged for review
            for mapping in result.mapped_fields:
                if mapping.flagged_for_review:
                    self.flagged_fields.append(mapping)
            
            # Add missing fields as flagged for review
            for missing_field in result.missing_fields:
                # Create a FieldMapping object for missing fields
                if '.' in missing_field:
                    table_name, field_name = missing_field.rsplit('.', 1)
                    missing_mapping = FieldMapping(
                        ecc_table=table_name,
                        ecc_field=field_name,
                        s4_table='',
                        s4_field='',
                        flagged_for_review=True
                    )
                    self.flagged_fields.append(missing_mapping)
    
    def print_pretty_mappings(self, mapping_results: List[TableMappingResult], table_field_usage: Dict[str, Set[str]], remediated_mode: bool = False):
        """Print pretty-formatted mappings: ECC_TABLE.ECC_FIELD --> S4_TABLE.S4_FIELD (one per line)
        Only shows mappings for fields actually used in the calculation view
        When remediated_mode is True, skip mappings where table maps to itself (e.g., BSEG.AUGCP --> BSEG.AUGCP)"""
        print("\n" + "="*80)
        print("EXHAUSTIVE LIST OF 1:1 FIELD MAPPINGS (ECC to S4) FOR THIS CALCULATION VIEW")
        print("="*80)
        
        all_mappings = []
        
        # Collect all mappings from results, but only for fields used in the calculation view
        for result in mapping_results:
            # Skip custom and transparent tables
            if self._is_custom_table(result.original_table) or self._is_transparent_table(result.original_table):
                continue
                
            if result.case == "1.1":
                # All fields deprecated - no mappings to show
                continue
            else:
                # Get the fields actually used in the calculation view for this table
                table_used_fields = table_field_usage.get(result.original_table, set())
                
                # If no specific fields are tracked (allViewAttributes=true), include all mapped fields
                if not table_used_fields:
                    # Add all mapped fields
                    for mapping in result.mapped_fields:
                        if mapping.s4_table and mapping.s4_field:
                            # Skip table-to-itself mappings in remediated mode
                            if remediated_mode and mapping.ecc_table == mapping.s4_table and mapping.ecc_field == mapping.s4_field:
                                continue
                            all_mappings.append(mapping)
                    
                    # Add missing fields with empty targets
                    for missing_field in result.missing_fields:
                        if '.' in missing_field:
                            table_name, field_name = missing_field.rsplit('.', 1)
                            missing_mapping = FieldMapping(
                                ecc_table=table_name,
                                ecc_field=field_name,
                                s4_table='[MISSING]',
                                s4_field='[MISSING]',
                                flagged_for_review=True
                            )
                            all_mappings.append(missing_mapping)
                else:
                    # Only include mappings for fields that are actually used in the calculation view
                    for mapping in result.mapped_fields:
                        if mapping.ecc_field in table_used_fields and mapping.s4_table and mapping.s4_field:
                            # Skip table-to-itself mappings in remediated mode
                            if remediated_mode and mapping.ecc_table == mapping.s4_table and mapping.ecc_field == mapping.s4_field:
                                continue
                            all_mappings.append(mapping)
                    
                    # Add missing fields with empty targets (only if they were used in the CV)
                    for missing_field in result.missing_fields:
                        if '.' in missing_field:
                            table_name, field_name = missing_field.rsplit('.', 1)
                            # Only add if this field was actually used in the calculation view
                            if field_name in table_used_fields:
                                missing_mapping = FieldMapping(
                                    ecc_table=table_name,
                                    ecc_field=field_name,
                                    s4_table='[MISSING]',
                                    s4_field='[MISSING]',
                                    flagged_for_review=True
                                )
                                all_mappings.append(missing_mapping)
        
        # Sort mappings by ECC table and field for consistent output
        all_mappings.sort(key=lambda m: (m.ecc_table, m.ecc_field))
        
        # Print each mapping on one line
        for mapping in all_mappings:
            ecc_part = f"{mapping.ecc_table}.{mapping.ecc_field}"
            s4_part = f"{mapping.s4_table}.{mapping.s4_field}"
            flag_indicator = " [FLAGGED]" if mapping.flagged_for_review else ""
            print(f"{ecc_part} --> {s4_part}{flag_indicator}")
        
        print(f"\nTotal mappings displayed: {len(all_mappings)}")
        flagged_count = sum(1 for m in all_mappings if m.flagged_for_review)
        print(f"Flagged for review: {flagged_count}")

    def print_field_extraction_summary(self, table_field_usage: Dict[str, Set[str]]):
        """Print a summary of extracted field usage from calculation view"""
        print("\n" + "="*80)
        print("FIELD EXTRACTION SUMMARY")
        print("="*80)
        
        total_fields_extracted = 0
        for table, fields in table_field_usage.items():
            if fields:
                total_fields_extracted += len(fields)
                print(f"{table}: {len(fields)} fields used")
                # Show a few example fields
                field_list = sorted(list(fields))
                if len(field_list) <= 5:
                    print(f"  Fields: {', '.join(field_list)}")
                else:
                    print(f"  Fields: {', '.join(field_list[:5])}... (showing first 5 of {len(field_list)})")
            else:
                print(f"{table}: using all fields (allViewAttributes=true)")
        
        print(f"\nTotal unique fields extracted: {total_fields_extracted}")

    def print_measures_with_mappings(self, xml_file_path: str, mapping_results: List[TableMappingResult]):
        """Print measures (non-attribute fields) with their aggregation types and mappings"""
        print("\n" + "="*80)
        print("MEASURES (NON-ATTRIBUTES) WITH AGGREGATION TYPES AND MAPPINGS")
        print("="*80)
        
        # Extract measures from calculation view
        measures_by_node = self.extract_measures_from_calculation_view(xml_file_path)
        
        if not measures_by_node:
            print("No measures found in this calculation view.")
            return
        
        # For each node with measures, find the source table and show mappings
        for node_name, measures in measures_by_node.items():
            print(f"\nNode: {node_name}")
            print("-" * 40)
            
            # Try to find the source table for this node by examining input mappings
            source_table = self._find_source_table_for_node(xml_file_path, node_name)
            
            for measure_field, aggregation_type in measures.items():
                print(f"  Measure: {measure_field} (aggregationType: {aggregation_type})")
                
                # Find mapping for this field
                mapping_found = False
                if source_table:
                    # Look through mapping results to find this field
                    for result in mapping_results:
                        if result.original_table == source_table:
                            for mapping in result.mapped_fields:
                                if mapping.ecc_field == measure_field:
                                    flag_indicator = " [FLAGGED]" if mapping.flagged_for_review else ""
                                    print(f"    Mapping: {mapping.ecc_table}.{mapping.ecc_field} --> {mapping.s4_table}.{mapping.s4_field}{flag_indicator}")
                                    mapping_found = True
                                    break
                            break
                
                if not mapping_found:
                    if source_table:
                        print(f"    Mapping: {source_table}.{measure_field} --> [NO MAPPING FOUND]")
                    else:
                        print(f"    Mapping: [SOURCE UNKNOWN].{measure_field} --> [NO MAPPING FOUND]")

    def _find_source_table_for_node(self, xml_file_path: str, node_name: str) -> Optional[str]:
        """Find the primary source table for a given calculation view node"""
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            # Find the specific calculation view node
            ns = {'calc': 'http://www.sap.com/ndb/BiModelCalculation.ecore'}
            calc_views = root.findall('.//calc:calculationView', ns)
            if not calc_views:
                calc_views = root.findall('.//calculationView')
            
            for calc_view in calc_views:
                if calc_view.get('id') == node_name:
                    # Look for input nodes
                    input_elements = calc_view.findall('.//calc:input', ns)
                    if not input_elements:
                        input_elements = calc_view.findall('.//input')
                    
                    if input_elements:
                        # Return the first input node (primary source)
                        first_input = input_elements[0]
                        node_ref = first_input.get('node')
                        if node_ref and node_ref.startswith('#'):
                            return node_ref[1:]  # Remove '#' prefix
            
            return None
            
        except Exception as e:
            print(f"Error finding source table for node {node_name}: {e}")
            return None
    
    def print_results(self, mapping_results: List[TableMappingResult] = None, hide_remapped_adjacency: bool = False, show_adjacency: bool = False):
        """Print all results"""
        print("\n" + "="*80)
        print("REMEDIATION MAPPING RESULTS")
        print("="*80)
        
        section_num = 1
        
        if show_adjacency:
            print(f"\n{section_num}. ORIGINAL ADJACENCY LIST:")
            print("-" * 40)
            print("   Format: Node (type) <- dependencies")
            for name, node in self.original_adjacency_list.items():
                deps = " <- " + ", ".join(node.dependencies) if node.dependencies else ""
                print(f"  {name} ({node.type}){deps}")
            section_num += 1
            
            if not hide_remapped_adjacency:
                print(f"\n{section_num}. REMAPPED ADJACENCY LIST:")
                print("-" * 40)
                print("   Format: Node (type) <- dependencies")
                for name, node in self.remapped_adjacency_list.items():
                    deps = " <- " + ", ".join(node.dependencies) if node.dependencies else ""
                    print(f"  {name} ({node.type}){deps}")
                section_num += 1
        
        print(f"\n{section_num}. SUMMARY OF CHANGES:")
        print("-" * 40)
        for change in self.summary:
            print(f"  {change}")
        
        print(f"\n{section_num + 1}. FIELDS FLAGGED FOR MANUAL REVIEW:")
        print("-" * 40)
        if self.flagged_fields:
            for field in self.flagged_fields:
                print(f"  {field.ecc_table}.{field.ecc_field} -> {field.s4_table}.{field.s4_field}")
        else:
            print("  None")
        
        # Calculate total fields processed (same logic as CSV generation)
        total_fields_processed = 0
        if mapping_results:
            for result in mapping_results:
                # Skip transparent and custom tables (same as CSV logic)
                if self._is_custom_table(result.original_table) or self._is_transparent_table(result.original_table):
                    continue
                
                if result.case == "1.1":
                    # Deletable table - one row
                    total_fields_processed += 1
                else:
                    # Tables with mappings and missing fields
                    total_fields_processed += len(result.mapped_fields) + len(result.missing_fields)
        
        print(f"\nTotal source tables impacted: {self.actual_changes}")
        print(f"Total fields processed: {total_fields_processed}")
        print(f"Total flagged fields: {len(self.flagged_fields)}")
    
    def save_results_to_files(self, xml_file_path: str, mapping_results: List[TableMappingResult]):
        """Save all results to files in a mirror directory structure under outputs/"""
        # Get the script directory (attempt-1)
        script_dir = Path(__file__).parent
        
        # Get relative path from inputs/cv/ to create mirror structure in outputs/cv/
        inputs_cv_dir = script_dir / "inputs" / "cv"
        relative_path = os.path.relpath(os.path.dirname(xml_file_path), str(inputs_cv_dir))
        
        # Create mirror output directory
        output_dir = script_dir / "outputs" / "cv" / relative_path
        output_dir.mkdir(parents=True, exist_ok=True)
        
        view_name = os.path.splitext(os.path.basename(xml_file_path))[0]

        # 1. Save original adjacency list
        original_adj_file = os.path.join(output_dir, f"{view_name}_original_adjacency_list.csv")
        self._save_adjacency_list_csv(original_adj_file, self.original_adjacency_list)
        
        # 2. Save remapped adjacency list
        remapped_adj_file = os.path.join(output_dir, f"{view_name}_remapped_adjacency_list.csv")
        self._save_adjacency_list_csv(remapped_adj_file, self.remapped_adjacency_list)
        
        # 3. Save detailed field mapping report (excluding transparent/custom tables)
        field_mapping_file = os.path.join(output_dir, f"{view_name}_field_mappings_detail.csv")
        csv_rows_written = self._save_field_mapping_report(field_mapping_file, mapping_results)
        
        # Store the actual CSV row count for statistics
        self.csv_rows_written = csv_rows_written
        
        # 4. Save summary report (after CSV is generated to get correct row count)
        summary_file = os.path.join(output_dir, f"{view_name}_remediation_summary.txt")
        self._save_summary_report(summary_file, view_name, mapping_results)
        
        print(f"\nFiles saved to {output_dir}:")
        print(f"  - {os.path.basename(summary_file)}")
        print(f"  - {os.path.basename(original_adj_file)}")
        print(f"  - {os.path.basename(remapped_adj_file)}")
        print(f"  - {os.path.basename(field_mapping_file)}")
    
    def _save_summary_report(self, file_path: str, view_name: str, mapping_results: List[TableMappingResult] = None):
        """Save summary report to text file"""
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"REMEDIATION MAPPING SUMMARY REPORT\n")
            f.write(f"Calculation View: {view_name}\n")
            f.write(f"Generated: {os.path.basename(__file__)}\n")
            f.write("=" * 80 + "\n\n")
            
            f.write("SUMMARY OF CHANGES:\n")
            f.write("-" * 40 + "\n")
            for change in self.summary:
                f.write(f"{change}\n")
            
            f.write(f"\nFIELDS FLAGGED FOR MANUAL REVIEW:\n")
            f.write("-" * 40 + "\n")
            if self.flagged_fields:
                for field in self.flagged_fields:
                    f.write(f"{field.ecc_table}.{field.ecc_field} -> {field.s4_table}.{field.s4_field}\n")
            else:
                f.write("None\n")
            
            f.write(f"\nSTATISTICS:\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total source tables impacted: {self.actual_changes}\n")
            f.write(f"Total fields processed: {self.csv_rows_written}\n")
            f.write(f"Total flagged fields: {len(self.flagged_fields)}\n")
            f.write(f"Original nodes: {len(self.original_adjacency_list)}\n")
            f.write(f"Remapped nodes: {len(self.remapped_adjacency_list)}\n")
    
    def _save_adjacency_list_csv(self, file_path: str, adjacency_list: Dict[str, Node]):
        """Save adjacency list to CSV file"""
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Node_Name', 'Node_Type', 'Dependencies', 'Dependents'])
            
            for name, node in adjacency_list.items():
                dependencies = ';'.join(node.dependencies) if node.dependencies else ''
                dependents = ';'.join(node.dependents) if node.dependents else ''
                writer.writerow([name, node.type, dependencies, dependents])
    
    def _save_field_mapping_report(self, file_path: str, mapping_results: List[TableMappingResult]) -> int:
        """Save detailed field mapping report excluding transparent/custom tables"""
        rows_written = 0
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Original_Table', 'Case', 'ECC_Field', 'S4_Table', 'S4_Field', 'Flagged_For_Review', 'Status'])
            
            for result in mapping_results:
                # Skip transparent and custom tables
                if self._is_custom_table(result.original_table) or self._is_transparent_table(result.original_table):
                    continue
                
                if result.case == "1.1":
                    # Deletable table
                    writer.writerow([result.original_table, result.case, 'ALL_FIELDS', '', '', 'N', 'DEPRECATED'])
                    rows_written += 1
                else:
                    # Tables with mappings
                    for mapping in result.mapped_fields:
                        status = 'FRAGMENTED' if result.is_fragmented else 'MAPPED'
                        writer.writerow([
                            result.original_table,
                            result.case,
                            mapping.ecc_field,
                            mapping.s4_table,
                            mapping.s4_field,
                            'Y' if mapping.flagged_for_review else 'N',
                            status
                        ])
                        rows_written += 1
                    
                    # Add missing fields for fragmented tables
                    if result.missing_fields:
                        for missing_field in result.missing_fields:
                            # Extract just the field name from table.field format
                            field_name = missing_field.split('.')[-1] if '.' in missing_field else missing_field
                            writer.writerow([
                                result.original_table,
                                result.case,
                                field_name,
                                '',
                                '',
                                'Y',
                                'MISSING'
                            ])
                            rows_written += 1
        return rows_written


def find_calculation_views(base_dir: str) -> List[str]:
    """Find all .calculationview files"""
    calc_views = []
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.calculationview'):
                calc_views.append(os.path.join(root, file))
    return calc_views


def apply_mappings_to_fields(engine: 'RemediationMappingEngine', fields: Set[str], data_sources: Set[str], include_table_prefix: bool = False) -> Set[str]:
    """Apply field mappings to transform ECC fields to S4 fields"""
    mapped_fields = set()
    
    for field in fields:
        if include_table_prefix and '.' in field:
            # For TABLE.FIELD format, split and check mappings
            table, field_name = field.split('.', 1)
            field_mapped = False
            
            for key, mapping_list in engine.field_mappings.items():
                for mapping in mapping_list:
                    if mapping.ecc_table == table and mapping.ecc_field == field_name:
                        # Only include mappings where target table exists in data sources
                        if mapping.s4_table in data_sources:
                            mapped_fields.add(f"{mapping.s4_table}.{mapping.s4_field}")
                            field_mapped = True
            
            # If no mappings found, keep the original TABLE.FIELD
            if not field_mapped:
                mapped_fields.add(field)
        else:
            # For regular fields (output fields), check by field name only
            field_mapped = False
            for key, mapping_list in engine.field_mappings.items():
                for mapping in mapping_list:
                    if mapping.ecc_field == field:
                        # Only include mappings where target table exists in data sources
                        if mapping.s4_table in data_sources:
                            mapped_fields.add(mapping.s4_field)
                            field_mapped = True
            
            # If no mappings found, keep the original field
            if not field_mapped:
                mapped_fields.add(field)
    
    return mapped_fields


def calculate_similarity_scores(engine: 'RemediationMappingEngine', file1: str, file2: str, debug: bool = False):
    """Calculate multiple similarity scores between two calculation views"""
    
    if debug:
        print(f"\nCalculating similarity between:")
        print(f"  File 1: {os.path.basename(file1)}")
        print(f"  File 2: {os.path.basename(file2)}")
    
    # Extract field usage (input fields from data sources)
    _, ds_ta1, _ = engine.parse_calculation_view(file1, debug)
    _, ds_ta2, _ = engine.parse_calculation_view(file2, debug)
    
    table_fields1 = engine.extract_field_usage(file1, ds_ta1)
    table_fields2 = engine.extract_field_usage(file2, ds_ta2)
    
    # Flatten to get all input fields with table prefixes
    input_fields1 = set()
    for table, fields in table_fields1.items():
        for field in fields:
            input_fields1.add(f"{table}.{field}")
    
    input_fields2 = set()
    for table, fields in table_fields2.items():
        for field in fields:
            input_fields2.add(f"{table}.{field}")
    
    # Extract output fields (attributes and measures)
    output_attrs1, output_measures1 = engine.extract_output_columns(file1)
    output_attrs2, output_measures2 = engine.extract_output_columns(file2)
    
    output_fields1 = output_attrs1.union(output_measures1)
    output_fields2 = output_attrs2.union(output_measures2)
    
    # Get file names for display
    file1_name = os.path.basename(file1)
    file2_name = os.path.basename(file2)
    
    print("\n" + "="*80)
    print("SIMILARITY ANALYSIS REPORT")
    print("="*80)
    print(f"File 1: {file1_name}")
    print(f"File 2: {file2_name}")
    print("="*80)
    
    # Score 1: Input fields overlap (raw)
    input_overlap = input_fields1.intersection(input_fields2)
    input_unique1 = input_fields1 - input_fields2
    input_unique2 = input_fields2 - input_fields1
    input_total = input_fields1.union(input_fields2)
    
    input_score = len(input_overlap) / len(input_total) if input_total else 0
    
    print(f"\n1. INPUT FIELDS SIMILARITY (Raw):")
    print(f"   Overlap: {len(input_overlap)} fields ({input_score:.2%})")
    print(f"   Unique to {file1_name}: {len(input_unique1)} fields")
    print(f"   Unique to {file2_name}: {len(input_unique2)} fields")
    print(f"   Total unique fields: {len(input_total)}")
    
    print(f"   \n   Overlapping fields ({len(input_overlap)}):")
    if input_overlap:
        for field in sorted(input_overlap):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    print(f"   \n   Unique to {file1_name} ({len(input_unique1)}):")
    if input_unique1:
        for field in sorted(input_unique1):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    print(f"   \n   Unique to {file2_name} ({len(input_unique2)}):")
    if input_unique2:
        for field in sorted(input_unique2):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    # Score 2: Output fields overlap (raw)
    output_overlap = output_fields1.intersection(output_fields2)
    output_unique1 = output_fields1 - output_fields2
    output_unique2 = output_fields2 - output_fields1
    output_total = output_fields1.union(output_fields2)
    
    output_score = len(output_overlap) / len(output_total) if output_total else 0
    
    print(f"\n2. OUTPUT FIELDS SIMILARITY (Raw):")
    print(f"   Overlap: {len(output_overlap)} fields ({output_score:.2%})")
    print(f"   Unique to {file1_name}: {len(output_unique1)} fields")
    print(f"   Unique to {file2_name}: {len(output_unique2)} fields")
    print(f"   Total unique fields: {len(output_total)}")
    
    print(f"   \n   Overlapping fields ({len(output_overlap)}):")
    if output_overlap:
        for field in sorted(output_overlap):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    print(f"   \n   Unique to {file1_name} ({len(output_unique1)}):")
    if output_unique1:
        for field in sorted(output_unique1):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    print(f"   \n   Unique to {file2_name} ({len(output_unique2)}):")
    if output_unique2:
        for field in sorted(output_unique2):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    # Score 3: Input fields overlap (post-mapping)
    mapped_input1 = apply_mappings_to_fields(engine, input_fields1, ds_ta1, include_table_prefix=True)
    mapped_input2 = apply_mappings_to_fields(engine, input_fields2, ds_ta2, include_table_prefix=True)
    
    mapped_input_overlap = mapped_input1.intersection(mapped_input2)
    mapped_input_unique1 = mapped_input1 - mapped_input2
    mapped_input_unique2 = mapped_input2 - mapped_input1
    mapped_input_total = mapped_input1.union(mapped_input2)
    
    mapped_input_score = len(mapped_input_overlap) / len(mapped_input_total) if mapped_input_total else 0
    
    print(f"\n3. INPUT FIELDS SIMILARITY (Post-Mapping):")
    print(f"   Overlap: {len(mapped_input_overlap)} fields ({mapped_input_score:.2%})")
    print(f"   Unique to {file1_name}: {len(mapped_input_unique1)} fields")
    print(f"   Unique to {file2_name}: {len(mapped_input_unique2)} fields")
    print(f"   Total unique fields: {len(mapped_input_total)}")
    
    print(f"   \n   Overlapping fields ({len(mapped_input_overlap)}):")
    if mapped_input_overlap:
        for field in sorted(mapped_input_overlap):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    print(f"   \n   Unique to {file1_name} ({len(mapped_input_unique1)}):")
    if mapped_input_unique1:
        for field in sorted(mapped_input_unique1):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    print(f"   \n   Unique to {file2_name} ({len(mapped_input_unique2)}):")
    if mapped_input_unique2:
        for field in sorted(mapped_input_unique2):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    # Score 4: Output fields overlap (post-mapping)
    mapped_output1 = apply_mappings_to_fields(engine, output_fields1, ds_ta1)
    mapped_output2 = apply_mappings_to_fields(engine, output_fields2, ds_ta2)
    
    mapped_output_overlap = mapped_output1.intersection(mapped_output2)
    mapped_output_unique1 = mapped_output1 - mapped_output2
    mapped_output_unique2 = mapped_output2 - mapped_output1
    mapped_output_total = mapped_output1.union(mapped_output2)
    
    mapped_output_score = len(mapped_output_overlap) / len(mapped_output_total) if mapped_output_total else 0
    
    print(f"\n4. OUTPUT FIELDS SIMILARITY (Post-Mapping):")
    print(f"   Overlap: {len(mapped_output_overlap)} fields ({mapped_output_score:.2%})")
    print(f"   Unique to {file1_name}: {len(mapped_output_unique1)} fields")
    print(f"   Unique to {file2_name}: {len(mapped_output_unique2)} fields")
    print(f"   Total unique fields: {len(mapped_output_total)}")
    
    print(f"   \n   Overlapping fields ({len(mapped_output_overlap)}):")
    if mapped_output_overlap:
        for field in sorted(mapped_output_overlap):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    print(f"   \n   Unique to {file1_name} ({len(mapped_output_unique1)}):")
    if mapped_output_unique1:
        for field in sorted(mapped_output_unique1):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    print(f"   \n   Unique to {file2_name} ({len(mapped_output_unique2)}):")
    if mapped_output_unique2:
        for field in sorted(mapped_output_unique2):
            print(f"     • {field}")
    else:
        print("     (none)")
    
    # Summary
    print(f"\nOVERALL SIMILARITY SUMMARY:")
    print(f"   Raw Input Fields:        {input_score:.1%}")
    print(f"   Raw Output Fields:       {output_score:.1%}")
    print(f"   Post-Mapping Input:      {mapped_input_score:.1%}")
    print(f"   Post-Mapping Output:     {mapped_output_score:.1%}")


def resolve_recursive_data_sources(engine: 'RemediationMappingEngine', view_path: str, visited: set = None) -> set:
    """Recursively resolve all table data sources from a calculation view, including through other CVs"""
    if visited is None:
        visited = set()

    # Prevent infinite recursion
    view_name = os.path.basename(view_path)
    if view_name in visited:
        return set()
    visited.add(view_name)

    all_table_sources = set()

    try:
        # Parse this view
        _, table_sources, calc_view_sources = engine.parse_calculation_view(view_path, False)

        # Add direct table sources
        all_table_sources.update(table_sources)

        # Recursively resolve calculation view sources
        for cv_source in calc_view_sources:
            # Try to find the calculation view file
            view_dir = os.path.dirname(view_path)
            parent_dir = os.path.dirname(view_dir)

            # Look for the calculation view in the same directory structure
            potential_paths = [
                os.path.join(parent_dir, cv_source, f"{cv_source}.calculationview"),
                os.path.join(view_dir, f"{cv_source}.calculationview"),
                os.path.join(os.path.dirname(parent_dir), "inputs", "cv", cv_source, f"{cv_source}.calculationview")
            ]

            for potential_path in potential_paths:
                if os.path.exists(potential_path):
                    recursive_sources = resolve_recursive_data_sources(engine, potential_path, visited.copy())
                    all_table_sources.update(recursive_sources)
                    break

    except Exception as e:
        # If we can't parse a view, just return what we have
        pass

    return all_table_sources


def compare_calculation_views(engine: 'RemediationMappingEngine', original_view: str, remediated_view: str, debug: bool = False):
    """Compare output columns between original and remediated calculation views based on field mappings"""

    if debug:
        print(f"\nComparing calculation views:")
        print(f"  Original: {os.path.basename(original_view)}")
        print(f"  Remediated: {os.path.basename(remediated_view)}")

    # Extract output columns from both views
    orig_attributes, orig_measures = engine.extract_output_columns(original_view)
    remed_attributes, remed_measures = engine.extract_output_columns(remediated_view)

    # Extract data sources from remediated view - now recursively resolve them
    _, direct_remed_data_sources, _ = engine.parse_calculation_view(remediated_view, debug)
    remed_data_sources = resolve_recursive_data_sources(engine, remediated_view)

    if debug:
        print(f"Direct remediated data sources: {sorted(direct_remed_data_sources)}")
        print(f"Recursive remediated data sources: {sorted(remed_data_sources)}")
    
    if debug:
        print(f"\nOriginal view has {len(orig_attributes)} attributes and {len(orig_measures)} measures")
        print(f"Remediated view has {len(remed_attributes)} attributes and {len(remed_measures)} measures")
        print(f"Remediated view has {len(remed_data_sources)} data sources")
    
    print("\n" + "="*80)
    print("CALCULATION VIEW COMPARISON REPORT")
    print("="*80)
    print(f"Original: {os.path.basename(original_view)}")
    print(f"Remediated: {os.path.basename(remediated_view)}")
    print("="*80)
    
    # Compare attributes
    print("\nATTRIBUTE COMPARISON:")
    print("-" * 40)

    attribute_issues = []
    for attr in orig_attributes:
        # Check if this attribute has any mappings
        mappings_found = []
        for key, mapping_list in engine.field_mappings.items():
            for mapping in mapping_list:
                if mapping.ecc_field == attr:
                    # Filter out mappings where target table doesn't exist in remediated view
                    if mapping.s4_table in remed_data_sources:
                        mappings_found.append(mapping)

        if not mappings_found:
            # No direct mappings found for this field name - check lineage for original source field
            if attr in remed_attributes:
                print(f"✓ {attr}")
            else:
                # Trace lineage to find original source field
                lineage = trace_field_lineage(original_view, attr, debug)

                # Check if any source fields in the lineage have mappings
                source_mappings_found = []
                for lineage_entry in lineage:
                    if lineage_entry.is_original_source:
                        # Use source_field if available, otherwise use field_name for original source
                        original_field_name = lineage_entry.source_field or lineage_entry.field_name
                        # Check for mappings using the original source field name
                        for key, mapping_list in engine.field_mappings.items():
                            for mapping in mapping_list:
                                if (mapping.ecc_field == original_field_name and
                                    mapping.s4_table in remed_data_sources):
                                    source_mappings_found.append(mapping)

                if source_mappings_found:
                    # Check if any of the mapped target fields exist in remediated view
                    target_fields_found = []
                    for mapping in source_mappings_found:
                        if mapping.s4_field in remed_attributes:
                            target_fields_found.append(mapping)

                    if target_fields_found:
                        print(f"⚠ {attr} (mapping exists for source field, only renaming needed)")
                        for mapping in target_fields_found:
                            print(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✓] (needs renaming from {attr})")
                    else:
                        print(f"✗ {attr} (mapping exists for source field but target field missing from remediated view)")
                        for mapping in source_mappings_found:
                            print(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✗]")
                        attribute_issues.append(f"Mapped attribute {attr} missing and target fields missing from remediated view")
                else:
                    print(f"✗ {attr} (no mapping defined, field missing from remediated view)")
                    attribute_issues.append(f"Unmapped attribute {attr} missing from remediated view")

                # Always show lineage for unmapped fields
                lineage_output = format_field_lineage(attr, lineage)
                print(lineage_output)
        else:
            # Has mappings - check if any mapped fields exist in remediated view
            mapping_statuses = []
            any_found = False

            for mapping in mappings_found:
                if mapping.s4_field in remed_attributes:
                    mapping_statuses.append(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✓]")
                    any_found = True
                else:
                    mapping_statuses.append(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✗]")

            if any_found:
                # At least one mapping found
                if attr in remed_attributes:
                    print(f"? {attr} (original field present, but has mappings)")
                    for status in mapping_statuses:
                        print(status)
                else:
                    print(f"✓ {attr} (properly mapped)")
                    for status in mapping_statuses:
                        print(status)
            else:
                # No mapped fields found in remediated view
                print(f"✗ {attr}")
                for status in mapping_statuses:
                    print(status)
                if attr in remed_attributes:
                    attribute_issues.append(f"Mapped attribute {attr} present but none of its mapped outputs exist")
                else:
                    attribute_issues.append(f"Mapped attribute {attr} missing and none of its mapped outputs exist")
    
    # Compare measures
    print("\nMEASURE COMPARISON:")
    print("-" * 40)

    measure_issues = []
    for measure in orig_measures:
        # Check if this measure has any mappings
        mappings_found = []
        for key, mapping_list in engine.field_mappings.items():
            for mapping in mapping_list:
                if mapping.ecc_field == measure:
                    # Filter out mappings where target table doesn't exist in remediated view
                    if mapping.s4_table in remed_data_sources:
                        mappings_found.append(mapping)

        if not mappings_found:
            # No direct mappings found for this field name - check lineage for original source field
            if measure in remed_measures:
                print(f"✓ {measure}")
            else:
                # Trace lineage to find original source field
                lineage = trace_field_lineage(original_view, measure, debug)

                # Check if any source fields in the lineage have mappings
                source_mappings_found = []
                for lineage_entry in lineage:
                    if lineage_entry.is_original_source:
                        # Use source_field if available, otherwise use field_name for original source
                        original_field_name = lineage_entry.source_field or lineage_entry.field_name
                        # Check for mappings using the original source field name
                        for key, mapping_list in engine.field_mappings.items():
                            for mapping in mapping_list:
                                if (mapping.ecc_field == original_field_name and
                                    mapping.s4_table in remed_data_sources):
                                    source_mappings_found.append(mapping)

                if source_mappings_found:
                    # Check if any of the mapped target fields exist in remediated view
                    target_fields_found = []
                    for mapping in source_mappings_found:
                        if mapping.s4_field in remed_measures:
                            target_fields_found.append(mapping)

                    if target_fields_found:
                        print(f"⚠ {measure} (mapping exists for source field, only renaming needed)")
                        for mapping in target_fields_found:
                            print(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✓] (needs renaming from {measure})")
                    else:
                        print(f"✗ {measure} (mapping exists for source field but target field missing from remediated view)")
                        for mapping in source_mappings_found:
                            print(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✗]")
                        measure_issues.append(f"Mapped measure {measure} missing and target fields missing from remediated view")
                else:
                    print(f"✗ {measure} (no mapping defined, field missing from remediated view)")
                    measure_issues.append(f"Unmapped measure {measure} missing from remediated view")

                # Always show lineage for unmapped fields
                lineage_output = format_field_lineage(measure, lineage)
                print(lineage_output)
        else:
            # Has mappings - check if any mapped fields exist in remediated view
            mapping_statuses = []
            any_found = False

            for mapping in mappings_found:
                if mapping.s4_field in remed_measures:
                    mapping_statuses.append(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✓]")
                    any_found = True
                else:
                    mapping_statuses.append(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✗]")

            if any_found:
                # At least one mapping found
                if measure in remed_measures:
                    print(f"? {measure} (original field present, but has mappings)")
                    for status in mapping_statuses:
                        print(status)
                else:
                    print(f"✓ {measure} (properly mapped)")
                    for status in mapping_statuses:
                        print(status)
            else:
                # No mapped fields found in remediated view
                print(f"✗ {measure}")
                for status in mapping_statuses:
                    print(status)
                if measure in remed_measures:
                    measure_issues.append(f"Mapped measure {measure} present but none of its mapped outputs exist")
                else:
                    measure_issues.append(f"Mapped measure {measure} missing and none of its mapped outputs exist")
    
    # Check for surplus attributes in remediated view
    print("\nSURPLUS ATTRIBUTES IN REMEDIATED VIEW:")
    print("-" * 40)

    surplus_attributes = []
    for attr in remed_attributes:
        # Check if this attribute exists in original view
        if attr in orig_attributes:
            continue  # Not surplus - exists in original

        # Check if this attribute is a mapping target from any original field
        is_mapping_target = False
        for orig_attr in orig_attributes:
            for key, mapping_list in engine.field_mappings.items():
                for mapping in mapping_list:
                    if mapping.ecc_field == orig_attr and mapping.s4_field == attr:
                        if mapping.s4_table in remed_data_sources:
                            is_mapping_target = True
                            break
                if is_mapping_target:
                    break
            if is_mapping_target:
                break

        if not is_mapping_target:
            surplus_attributes.append(attr)
            print(f"⚠ {attr} (no relation to original attributes)")
            # Trace lineage for surplus attribute
            lineage = trace_field_lineage(remediated_view, attr, debug)
            lineage_output = format_field_lineage(attr, lineage)
            print(lineage_output)

    if not surplus_attributes:
        print("None")

    # Check for surplus measures in remediated view
    print("\nSURPLUS MEASURES IN REMEDIATED VIEW:")
    print("-" * 40)

    surplus_measures = []
    for measure in remed_measures:
        # Check if this measure exists in original view
        if measure in orig_measures:
            continue  # Not surplus - exists in original

        # Check if this measure is a mapping target from any original field
        is_mapping_target = False
        for orig_measure in orig_measures:
            for key, mapping_list in engine.field_mappings.items():
                for mapping in mapping_list:
                    if mapping.ecc_field == orig_measure and mapping.s4_field == measure:
                        if mapping.s4_table in remed_data_sources:
                            is_mapping_target = True
                            break
                if is_mapping_target:
                    break
            if is_mapping_target:
                break

        if not is_mapping_target:
            surplus_measures.append(measure)
            print(f"⚠ {measure} (no relation to original measures)")
            # Trace lineage for surplus measure
            lineage = trace_field_lineage(remediated_view, measure, debug)
            lineage_output = format_field_lineage(measure, lineage)
            print(lineage_output)

    if not surplus_measures:
        print("None")

    # Summary
    total_issues = len(attribute_issues) + len(measure_issues)
    total_warnings = len(surplus_attributes) + len(surplus_measures)

    # Count fields present in both views
    attrs_present_in_both = len(orig_attributes.intersection(remed_attributes))
    measures_present_in_both = len(orig_measures.intersection(remed_measures))
    
    print(f"\nSUMMARY:")
    print("-" * 40)
    print(f"Total attributes in original: {len(orig_attributes)}")
    print(f"Total measures in original: {len(orig_measures)}")
    print(f"Total attributes in remediated: {len(remed_attributes)}")
    print(f"Total measures in remediated: {len(remed_measures)}")
    print(f"Attributes present in both views: {attrs_present_in_both}")
    print(f"Measures present in both views: {measures_present_in_both}")
    print(f"Issues found: {total_issues}")
    print(f"Warnings (surplus fields): {total_warnings}")

    if total_issues > 0:
        print(f"\nISSUES DETECTED:")
        print("-" * 40)
        for issue in attribute_issues + measure_issues:
            print(f"  • {issue}")

    if total_warnings > 0:
        print(f"\nWARNINGS DETECTED:")
        print("-" * 40)
        for attr in surplus_attributes:
            print(f"  • Surplus attribute: {attr}")
        for measure in surplus_measures:
            print(f"  • Surplus measure: {measure}")


def multi_input_compare(engine: 'RemediationMappingEngine', num_inputs: int, debug: bool = False, output_file: str = None, use_pager: bool = False):
    """Compare multiple input views collectively against one remediated view"""

    # Find calculation views in both directories
    script_dir = Path(__file__).parent
    cv_input_dir = script_dir / "inputs" / "cv"
    cv_remediated_dir = script_dir / "inputs" / "cv_remediated"

    calc_views = sorted(find_calculation_views(str(cv_input_dir)))
    remediated_views = sorted(find_calculation_views(str(cv_remediated_dir)))

    if not calc_views:
        print("No calculation view files found in inputs/cv.")
        sys.exit(1)

    if not remediated_views:
        print("No calculation view files found in inputs/cv_remediated.")
        sys.exit(1)

    # Select multiple input views
    print(f"\nSelect {num_inputs} original calculation views to compare:")
    print("\nAvailable Original Calculation Views:")
    for i, view_path in enumerate(calc_views, 1):
        view_name = os.path.basename(view_path)
        relative_path = os.path.relpath(view_path, str(cv_input_dir))
        print(f"{i}. {view_name} (inputs/cv/{relative_path})")

    selected_input_views = []
    i = 0
    while i < num_inputs:
        try:
            choice = int(input(f"\nSelect input view {i+1} (number): ")) - 1
            if choice < 0 or choice >= len(calc_views):
                print("Invalid selection.")
                sys.exit(1)
            if calc_views[choice] in selected_input_views:
                print("View already selected. Please choose a different view.")
                continue  # Retry this selection
            selected_input_views.append(calc_views[choice])
            i += 1  # Only increment when successfully added
        except ValueError:
            print("Invalid input. Please enter a number.")
            sys.exit(1)

    # Select one remediated view
    print("\nAvailable Remediated Calculation Views:")
    for j, view_path in enumerate(remediated_views, 1):
        view_name = os.path.basename(view_path)
        relative_path = os.path.relpath(view_path, str(cv_remediated_dir))
        print(f"{j}. {view_name} (inputs/cv_remediated/{relative_path})")

    try:
        choice = int(input("\nSelect a remediated calculation view to compare (number): ")) - 1
        if choice < 0 or choice >= len(remediated_views):
            print("Invalid selection.")
            sys.exit(1)
    except ValueError:
        print("Invalid input. Please enter a number.")
        sys.exit(1)

    selected_remediated = remediated_views[choice]

    # Perform multi-input comparison
    if output_file:
        # Redirect output to file
        original_stdout = sys.stdout
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                sys.stdout = f
                compare_multi_input_views(engine, selected_input_views, selected_remediated, debug)
        finally:
            sys.stdout = original_stdout
            print(f"Multi-input comparison report saved to: {output_file}")
    elif use_pager:
        with pager_output():
            compare_multi_input_views(engine, selected_input_views, selected_remediated, debug)
    else:
        compare_multi_input_views(engine, selected_input_views, selected_remediated, debug)


def compare_multi_input_views(engine: 'RemediationMappingEngine', input_views: List[str], remediated_view: str, debug: bool = False):
    """Core logic for comparing multiple input views against one remediated view"""

    if debug:
        print(f"\nComparing multiple input calculation views:")
        for i, view in enumerate(input_views, 1):
            print(f"  Input {i}: {os.path.basename(view)}")
        print(f"  Remediated: {os.path.basename(remediated_view)}")

    # Extract output columns from all input views
    input_view_data = []
    all_input_attributes = set()
    all_input_measures = set()

    for i, view in enumerate(input_views, 1):
        attributes, measures = engine.extract_output_columns(view)
        view_data = {
            'view_path': view,
            'view_name': os.path.basename(view),
            'view_number': i,
            'attributes': attributes,
            'measures': measures
        }
        input_view_data.append(view_data)

        # Add to aggregated sets with view suffixes
        for attr in attributes:
            all_input_attributes.add(f"{attr}_{i}")

        for measure in measures:
            all_input_measures.add(f"{measure}_{i}")

    # Extract output columns from remediated view
    remed_attributes, remed_measures = engine.extract_output_columns(remediated_view)

    # Extract data sources from remediated view
    remed_data_sources = resolve_recursive_data_sources(engine, remediated_view)

    if debug:
        print(f"\nInput views contain {len(all_input_attributes)} total suffixed attributes and {len(all_input_measures)} total suffixed measures")
        print(f"Remediated view has {len(remed_attributes)} attributes and {len(remed_measures)} measures")
        print(f"Remediated view has {len(remed_data_sources)} data sources")

    print("\n" + "="*80)
    print("MULTI-INPUT CALCULATION VIEW COMPARISON REPORT")
    print("="*80)
    for i, view_data in enumerate(input_view_data, 1):
        print(f"Input {i}: {view_data['view_name']}")
    print(f"Remediated: {os.path.basename(remediated_view)}")
    print("="*80)

    # Compare attributes
    print("\nATTRIBUTE COMPARISON:")
    print("-" * 40)

    for view_data in input_view_data:
        print(f"\nView {view_data['view_number']}: {view_data['view_name']}")
        print("-" * 30)

        for attr in view_data['attributes']:
            # Check if this attribute has any mappings
            mappings_found = []
            for key, mapping_list in engine.field_mappings.items():
                for mapping in mapping_list:
                    if mapping.ecc_field == attr:
                        if mapping.s4_table in remed_data_sources:
                            mappings_found.append(mapping)

            if not mappings_found:
                if attr in remed_attributes:
                    print(f"✓ {attr}")
                else:
                    # Trace lineage to find original source field
                    lineage = trace_field_lineage(view_data['view_path'], attr, debug)

                    # Check if any source fields in the lineage have mappings
                    source_mappings_found = []
                    for lineage_entry in lineage:
                        if lineage_entry.is_original_source:
                            original_field_name = lineage_entry.source_field or lineage_entry.field_name
                            for key, mapping_list in engine.field_mappings.items():
                                for mapping in mapping_list:
                                    if (mapping.ecc_field == original_field_name and
                                        mapping.s4_table in remed_data_sources):
                                        source_mappings_found.append(mapping)

                    if source_mappings_found:
                        target_fields_found = []
                        for mapping in source_mappings_found:
                            if mapping.s4_field in remed_attributes:
                                target_fields_found.append(mapping)

                        if target_fields_found:
                            print(f"⚠ {attr} (mapping exists for source field, only renaming needed)")
                            for mapping in target_fields_found:
                                print(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✓] (needs renaming from {attr})")
                        else:
                            print(f"✗ {attr} (mapping exists for source field but target field missing from remediated view)")
                            for mapping in source_mappings_found:
                                print(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✗]")

                            # Show lineage from input view
                            lineage_output = format_field_lineage(f"{attr} (Input View {view_data['view_number']})", lineage)
                            print(lineage_output)

                            # Also check if this field exists in remediated view and show its lineage
                            if attr in remed_attributes:
                                print(f"    NOTE: Field '{attr}' exists in remediated view - showing its lineage:")
                                remed_lineage = trace_field_lineage(remediated_view, attr, debug)
                                remed_lineage_output = format_field_lineage(f"{attr} (Remediated View)", remed_lineage)
                                print(remed_lineage_output)
                    else:
                        print(f"✗ {attr} (no mapping defined, field missing from remediated view)")

                    # Show lineage from input view
                    lineage_output = format_field_lineage(f"{attr} (Input View {view_data['view_number']})", lineage)
                    print(lineage_output)

                    # Also check if this field exists in remediated view and show its lineage
                    if attr in remed_attributes:
                        print(f"    NOTE: Field '{attr}' exists in remediated view - showing its lineage:")
                        remed_lineage = trace_field_lineage(remediated_view, attr, debug)
                        remed_lineage_output = format_field_lineage(f"{attr} (Remediated View)", remed_lineage)
                        print(remed_lineage_output)
            else:
                # Has mappings - check if any mapped fields exist in remediated view
                mapping_statuses = []
                any_found = False

                for mapping in mappings_found:
                    if mapping.s4_field in remed_attributes:
                        mapping_statuses.append(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✓]")
                        any_found = True
                    else:
                        mapping_statuses.append(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✗]")

                if any_found:
                    if attr in remed_attributes:
                        print(f"? {attr} (original field present, but has mappings)")
                        for status in mapping_statuses:
                            print(status)
                    else:
                        print(f"✓ {attr} (properly mapped)")
                        for status in mapping_statuses:
                            print(status)
                else:
                    print(f"✗ {attr}")
                    for status in mapping_statuses:
                        print(status)

                    # Show lineage from input view for fields with mappings but no mapped outputs
                    lineage = trace_field_lineage(view_data['view_path'], attr, debug)
                    lineage_output = format_field_lineage(f"{attr} (Input View {view_data['view_number']})", lineage)
                    print(lineage_output)

                    # Also check if this field exists in remediated view and show its lineage
                    if attr in remed_attributes:
                        print(f"    NOTE: Field '{attr}' exists in remediated view - showing its lineage:")
                        remed_lineage = trace_field_lineage(remediated_view, attr, debug)
                        remed_lineage_output = format_field_lineage(f"{attr} (Remediated View)", remed_lineage)
                        print(remed_lineage_output)

    # Compare measures
    print("\nMEASURE COMPARISON:")
    print("-" * 40)

    for view_data in input_view_data:
        print(f"\nView {view_data['view_number']}: {view_data['view_name']}")
        print("-" * 30)

        for measure in view_data['measures']:
            # Check if this measure has any mappings
            mappings_found = []
            for key, mapping_list in engine.field_mappings.items():
                for mapping in mapping_list:
                    if mapping.ecc_field == measure:
                        if mapping.s4_table in remed_data_sources:
                            mappings_found.append(mapping)

            if not mappings_found:
                if measure in remed_measures:
                    print(f"✓ {measure}")
                else:
                    # Trace lineage to find original source field
                    lineage = trace_field_lineage(view_data['view_path'], measure, debug)

                    # Check if any source fields in the lineage have mappings
                    source_mappings_found = []
                    for lineage_entry in lineage:
                        if lineage_entry.is_original_source:
                            original_field_name = lineage_entry.source_field or lineage_entry.field_name
                            for key, mapping_list in engine.field_mappings.items():
                                for mapping in mapping_list:
                                    if (mapping.ecc_field == original_field_name and
                                        mapping.s4_table in remed_data_sources):
                                        source_mappings_found.append(mapping)

                    if source_mappings_found:
                        target_fields_found = []
                        for mapping in source_mappings_found:
                            if mapping.s4_field in remed_measures:
                                target_fields_found.append(mapping)

                        if target_fields_found:
                            print(f"⚠ {measure} (mapping exists for source field, only renaming needed)")
                            for mapping in target_fields_found:
                                print(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✓] (needs renaming from {measure})")
                        else:
                            print(f"✗ {measure} (mapping exists for source field but target field missing from remediated view)")
                            for mapping in source_mappings_found:
                                print(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✗]")

                            # Show lineage from input view
                            lineage_output = format_field_lineage(f"{measure} (Input View {view_data['view_number']})", lineage)
                            print(lineage_output)

                            # Also check if this measure exists in remediated view and show its lineage
                            if measure in remed_measures:
                                print(f"    NOTE: Measure '{measure}' exists in remediated view - showing its lineage:")
                                remed_lineage = trace_field_lineage(remediated_view, measure, debug)
                                remed_lineage_output = format_field_lineage(f"{measure} (Remediated View)", remed_lineage)
                                print(remed_lineage_output)
                    else:
                        print(f"✗ {measure} (no mapping defined, field missing from remediated view)")

                    # Show lineage from input view
                    lineage_output = format_field_lineage(f"{measure} (Input View {view_data['view_number']})", lineage)
                    print(lineage_output)

                    # Also check if this measure exists in remediated view and show its lineage
                    if measure in remed_measures:
                        print(f"    NOTE: Measure '{measure}' exists in remediated view - showing its lineage:")
                        remed_lineage = trace_field_lineage(remediated_view, measure, debug)
                        remed_lineage_output = format_field_lineage(f"{measure} (Remediated View)", remed_lineage)
                        print(remed_lineage_output)
            else:
                # Has mappings - check if any mapped fields exist in remediated view
                mapping_statuses = []
                any_found = False

                for mapping in mappings_found:
                    if mapping.s4_field in remed_measures:
                        mapping_statuses.append(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✓]")
                        any_found = True
                    else:
                        mapping_statuses.append(f"    {mapping.ecc_table}.{mapping.ecc_field} → {mapping.s4_table}.{mapping.s4_field} [✗]")

                if any_found:
                    if measure in remed_measures:
                        print(f"? {measure} (original field present, but has mappings)")
                        for status in mapping_statuses:
                            print(status)
                    else:
                        print(f"✓ {measure} (properly mapped)")
                        for status in mapping_statuses:
                            print(status)
                else:
                    print(f"✗ {measure}")
                    for status in mapping_statuses:
                        print(status)

                    # Show lineage from input view for measures with mappings but no mapped outputs
                    lineage = trace_field_lineage(view_data['view_path'], measure, debug)
                    lineage_output = format_field_lineage(f"{measure} (Input View {view_data['view_number']})", lineage)
                    print(lineage_output)

                    # Also check if this measure exists in remediated view and show its lineage
                    if measure in remed_measures:
                        print(f"    NOTE: Measure '{measure}' exists in remediated view - showing its lineage:")
                        remed_lineage = trace_field_lineage(remediated_view, measure, debug)
                        remed_lineage_output = format_field_lineage(f"{measure} (Remediated View)", remed_lineage)
                        print(remed_lineage_output)

    # Check for missing and surplus fields using union logic
    print("\nUNION-BASED MISSING/SURPLUS ANALYSIS:")
    print("-" * 40)

    # Create union sets (without suffixes for comparison)
    union_attributes = set()
    union_measures = set()

    for view_data in input_view_data:
        union_attributes.update(view_data['attributes'])
        union_measures.update(view_data['measures'])

    # Check for missing attributes in remediated view
    print("\nMISSING ATTRIBUTES IN REMEDIATED VIEW (based on union):")
    print("-" * 50)

    missing_attributes = []
    for attr in union_attributes:
        if attr not in remed_attributes:
            # Check if it has a proper mapping
            has_valid_mapping = False
            for key, mapping_list in engine.field_mappings.items():
                for mapping in mapping_list:
                    if mapping.ecc_field == attr and mapping.s4_table in remed_data_sources:
                        if mapping.s4_field in remed_attributes:
                            has_valid_mapping = True
                            break
                if has_valid_mapping:
                    break

            if not has_valid_mapping:
                missing_attributes.append(attr)
                # Show which input views contain this attribute
                containing_views = []
                containing_view_data = []
                for view_data in input_view_data:
                    if attr in view_data['attributes']:
                        containing_views.append(f"View {view_data['view_number']}")
                        containing_view_data.append(view_data)
                print(f"✗ {attr} (in: {', '.join(containing_views)})")

                # Show lineage from each input view that contains this attribute
                for view_data in containing_view_data:
                    lineage = trace_field_lineage(view_data['view_path'], attr, debug)
                    lineage_output = format_field_lineage(f"{attr} (Input View {view_data['view_number']})", lineage)
                    print(lineage_output)

                # Also check if this attribute exists in remediated view and show its lineage
                if attr in remed_attributes:
                    print(f"    NOTE: Attribute '{attr}' exists in remediated view - showing its lineage:")
                    remed_lineage = trace_field_lineage(remediated_view, attr, debug)
                    remed_lineage_output = format_field_lineage(f"{attr} (Remediated View)", remed_lineage)
                    print(remed_lineage_output)
                else:
                    print(f"    Checking if '{attr}' exists anywhere in remediated view lineage...")
                    # Try to find this field in the remediated view's lineage even if it's not in final output
                    try:
                        remed_lineage = trace_field_lineage(remediated_view, attr, debug)
                        if remed_lineage:
                            print(f"    Found '{attr}' in remediated view lineage:")
                            remed_lineage_output = format_field_lineage(f"{attr} (Remediated View Internal)", remed_lineage)
                            print(remed_lineage_output)
                        else:
                            print(f"    '{attr}' not found anywhere in remediated view")
                    except:
                        print(f"    '{attr}' not found anywhere in remediated view")

    if not missing_attributes:
        print("None")

    # Check for missing measures in remediated view
    print("\nMISSING MEASURES IN REMEDIATED VIEW (based on union):")
    print("-" * 50)

    missing_measures = []
    for measure in union_measures:
        if measure not in remed_measures:
            # Check if it has a proper mapping
            has_valid_mapping = False
            for key, mapping_list in engine.field_mappings.items():
                for mapping in mapping_list:
                    if mapping.ecc_field == measure and mapping.s4_table in remed_data_sources:
                        if mapping.s4_field in remed_measures:
                            has_valid_mapping = True
                            break
                if has_valid_mapping:
                    break

            if not has_valid_mapping:
                missing_measures.append(measure)
                # Show which input views contain this measure
                containing_views = []
                containing_view_data = []
                for view_data in input_view_data:
                    if measure in view_data['measures']:
                        containing_views.append(f"View {view_data['view_number']}")
                        containing_view_data.append(view_data)
                print(f"✗ {measure} (in: {', '.join(containing_views)})")

                # Show lineage from each input view that contains this measure
                for view_data in containing_view_data:
                    lineage = trace_field_lineage(view_data['view_path'], measure, debug)
                    lineage_output = format_field_lineage(f"{measure} (Input View {view_data['view_number']})", lineage)
                    print(lineage_output)

                # Also check if this measure exists in remediated view and show its lineage
                if measure in remed_measures:
                    print(f"    NOTE: Measure '{measure}' exists in remediated view - showing its lineage:")
                    remed_lineage = trace_field_lineage(remediated_view, measure, debug)
                    remed_lineage_output = format_field_lineage(f"{measure} (Remediated View)", remed_lineage)
                    print(remed_lineage_output)
                else:
                    print(f"    Checking if '{measure}' exists anywhere in remediated view lineage...")
                    # Try to find this measure in the remediated view's lineage even if it's not in final output
                    try:
                        remed_lineage = trace_field_lineage(remediated_view, measure, debug)
                        if remed_lineage:
                            print(f"    Found '{measure}' in remediated view lineage:")
                            remed_lineage_output = format_field_lineage(f"{measure} (Remediated View Internal)", remed_lineage)
                            print(remed_lineage_output)
                        else:
                            print(f"    '{measure}' not found anywhere in remediated view")
                    except:
                        print(f"    '{measure}' not found anywhere in remediated view")

    if not missing_measures:
        print("None")

    # Check for surplus attributes in remediated view
    print("\nSURPLUS ATTRIBUTES IN REMEDIATED VIEW (not in any input view):")
    print("-" * 60)

    surplus_attributes = []
    for attr in remed_attributes:
        if attr not in union_attributes:
            # Check if this attribute is a mapping target from any union field
            is_mapping_target = False
            for union_attr in union_attributes:
                for key, mapping_list in engine.field_mappings.items():
                    for mapping in mapping_list:
                        if mapping.ecc_field == union_attr and mapping.s4_field == attr:
                            if mapping.s4_table in remed_data_sources:
                                is_mapping_target = True
                                break
                    if is_mapping_target:
                        break
                if is_mapping_target:
                    break

            if not is_mapping_target:
                surplus_attributes.append(attr)
                print(f"⚠ {attr} (no relation to any input view attributes)")
                # Trace lineage for surplus attribute
                lineage = trace_field_lineage(remediated_view, attr, debug)
                lineage_output = format_field_lineage(attr, lineage)
                print(lineage_output)

    if not surplus_attributes:
        print("None")

    # Check for surplus measures in remediated view
    print("\nSURPLUS MEASURES IN REMEDIATED VIEW (not in any input view):")
    print("-" * 60)

    surplus_measures = []
    for measure in remed_measures:
        if measure not in union_measures:
            # Check if this measure is a mapping target from any union field
            is_mapping_target = False
            for union_measure in union_measures:
                for key, mapping_list in engine.field_mappings.items():
                    for mapping in mapping_list:
                        if mapping.ecc_field == union_measure and mapping.s4_field == measure:
                            if mapping.s4_table in remed_data_sources:
                                is_mapping_target = True
                                break
                    if is_mapping_target:
                        break
                if is_mapping_target:
                    break

            if not is_mapping_target:
                surplus_measures.append(measure)
                print(f"⚠ {measure} (no relation to any input view measures)")
                # Trace lineage for surplus measure
                lineage = trace_field_lineage(remediated_view, measure, debug)
                lineage_output = format_field_lineage(measure, lineage)
                print(lineage_output)

    if not surplus_measures:
        print("None")

    # Summary
    print("\nSUMMARY:")
    print("-" * 40)
    print(f"Input views analyzed: {len(input_view_data)}")
    print(f"Total unique attributes in union: {len(union_attributes)}")
    print(f"Total unique measures in union: {len(union_measures)}")
    print(f"Remediated view attributes: {len(remed_attributes)}")
    print(f"Remediated view measures: {len(remed_measures)}")
    print(f"Missing attributes: {len(missing_attributes)}")
    print(f"Missing measures: {len(missing_measures)}")
    print(f"Surplus attributes: {len(surplus_attributes)}")
    print(f"Surplus measures: {len(surplus_measures)}")

    if missing_attributes or missing_measures or surplus_attributes or surplus_measures:
        print("\nISSUES FOUND:")
        for attr in missing_attributes:
            print(f"  • Missing attribute: {attr}")
        for measure in missing_measures:
            print(f"  • Missing measure: {measure}")
        for attr in surplus_attributes:
            print(f"  • Surplus attribute: {attr}")
        for measure in surplus_measures:
            print(f"  • Surplus measure: {measure}")


@dataclass
class FieldLineage:
    """Represents the lineage of a field through calculation view nodes"""
    field_name: str
    node_id: str
    source_field: str = None  # None if same as field_name
    source_node: str = None   # Node where this field comes from
    is_original_source: bool = False  # True if this is from a DataSource


def trace_field_lineage(xml_file_path: str, field_name: str, debug: bool = False) -> List[FieldLineage]:
    """
    Trace the lineage of a field through the calculation view hierarchy.
    Returns a list of FieldLineage objects showing the path from original source to final field.
    """
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
                    # Check for keyMapping to find the source field and node
                    key_mapping = attr.find('.//keyMapping')
                    if key_mapping is not None:
                        logical_model_source_field = key_mapping.get('columnName')
                        logical_model_source_node = key_mapping.get('columnObjectName')
                    break
            # Check measures
            if not field_found_in_output:
                for measure in logical_model.findall('.//baseMeasures/measure'):
                    if measure.get('id') == field_name:
                        field_found_in_output = True
                        # Check for keyMapping to find the source field and node
                        key_mapping = measure.find('.//keyMapping')
                        if key_mapping is not None:
                            logical_model_source_field = key_mapping.get('columnName')
                            logical_model_source_node = key_mapping.get('columnObjectName')
                        break

        if not field_found_in_output:
            if debug:
                print(f"Field {field_name} not found in logical model output")
            return lineage

        # If there's a logical model mapping, add it to lineage first
        if logical_model_source_field and logical_model_source_node:
            if logical_model_source_field != field_name:
                # There's a renaming in the logical model
                logical_model_entry = FieldLineage(
                    field_name=field_name,
                    node_id="LogicalModel",
                    source_field=logical_model_source_field,
                    source_node=logical_model_source_node,
                    is_original_source=False
                )
                lineage.append(logical_model_entry)
                if debug:
                    print(f"Found logical model renaming: {logical_model_source_field} -> {field_name} from {logical_model_source_node}")
                # Continue tracing from the source field
                current_field = logical_model_source_field
            else:
                # No renaming, but we know the source node
                current_field = field_name
        else:
            # No logical model mapping found, start with the field name
            current_field = field_name

        # Start tracing from the determined current field and work backwards
        traced_nodes = set()  # Prevent infinite loops

        def trace_backwards(field, current_node_id=None):
            """Recursively trace field backwards through nodes"""
            if debug:
                print(f"Tracing field '{field}' in node '{current_node_id}'")

            # Find calculation views
            calc_views = []
            calc_views.extend(root.findall('.//calc:calculationView', ns))
            if not calc_views:
                calc_views.extend(root.findall('.//calculationView'))

            # If current_node_id is None, start from the last calculation view
            if current_node_id is None:
                if calc_views:
                    # Typically the last calculation view is the final aggregation
                    current_node_id = calc_views[-1].get('id')

            if current_node_id in traced_nodes:
                return  # Prevent infinite loops
            traced_nodes.add(current_node_id)

            # Find the calculation view with this ID
            target_calc_view = None
            for calc_view in calc_views:
                if calc_view.get('id') == current_node_id:
                    target_calc_view = calc_view
                    break

            if target_calc_view is None:
                if debug:
                    print(f"Could not find calculation view with ID: {current_node_id}")
                return

            # Check if this field is defined in viewAttributes of this node
            view_attrs = target_calc_view.find('.//viewAttributes')
            field_in_node = False
            if view_attrs is not None:
                for attr in view_attrs.findall('.//viewAttribute'):
                    if attr.get('id') == field:
                        field_in_node = True
                        break

            if not field_in_node:
                if debug:
                    print(f"Field '{field}' not found in node '{current_node_id}' viewAttributes")
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

                        if debug and is_renamed:
                            print(f"Found renaming: {source_field} -> {target_field} in {current_node_id} from {source_node_name}")

                        # Check if source is a DataSource (original table)
                        is_datasource = False
                        datasources = []
                        datasources.extend(root.findall('.//calc:DataSource', ns))
                        if not datasources:
                            datasources.extend(root.findall('.//DataSource'))

                        for ds in datasources:
                            if ds.get('id') == source_node_name:
                                is_datasource = True
                                # Mark as original source
                                lineage_entry.is_original_source = True
                                if debug:
                                    print(f"Reached original source: {source_node_name}.{source_field}")
                                break

                        # If not a datasource, continue tracing backwards
                        if not is_datasource:
                            trace_backwards(source_field, source_node_name)
                        break

                if field_mapped:
                    break

        # Start the backwards trace
        if logical_model_source_node:
            # Start tracing from the known source node
            trace_backwards(current_field, logical_model_source_node)
        else:
            # Start tracing from the last calculation view (default behavior)
            trace_backwards(current_field)

        # Reverse the lineage to show from source to target
        lineage.reverse()

        if debug:
            print(f"Completed lineage trace for {field_name}: {len(lineage)} steps")

        return lineage

    except Exception as e:
        if debug:
            print(f"Error tracing lineage for field {field_name}: {e}")
        return []


def format_field_lineage(field_name: str, lineage: List[FieldLineage]) -> str:
    """Format field lineage for display"""
    if not lineage:
        return f"    - No lineage found for {field_name}"

    result = []
    for entry in lineage:
        if entry.is_original_source:
            source_display = f"{entry.source_node}.{entry.source_field or entry.field_name}"
            result.append(f"    - Source: {source_display}")
        else:
            if entry.source_field and entry.source_field != entry.field_name:
                # Field was renamed
                result.append(f"    - Renamed: {entry.node_id} from {entry.source_field} -> {entry.field_name}")
            else:
                # Field passed through without renaming
                result.append(f"    - Passed through: {entry.node_id}")

    return "\n".join(result)


def extract_field_hidden_status(xml_file_path: str, debug: bool = False) -> dict:
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


def extract_field_descriptions(xml_file_path: str, debug: bool = False) -> dict:
    """Extract field descriptions from the logicalModel section of a calculation view"""
    descriptions = {}

    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Find logicalModel (search without namespace)
        for logical_model in root.findall('.//logicalModel'):
            # Extract attribute descriptions
            for attribute in logical_model.findall('.//attribute'):
                attr_id = attribute.get('id')
                if attr_id:
                    desc_elem = attribute.find('.//descriptions')
                    if desc_elem is not None:
                        default_desc = desc_elem.get('defaultDescription', '')
                        descriptions[attr_id] = default_desc
                    else:
                        descriptions[attr_id] = ''

            # Extract measure descriptions
            for measure in logical_model.findall('.//measure'):
                measure_id = measure.get('id')
                if measure_id:
                    desc_elem = measure.find('.//descriptions')
                    if desc_elem is not None:
                        default_desc = desc_elem.get('defaultDescription', '')
                        descriptions[measure_id] = default_desc
                    else:
                        descriptions[measure_id] = ''

    except Exception as e:
        if debug:
            print(f"Warning: Could not extract descriptions from {xml_file_path}: {e}")

    return descriptions


def generate_detailed_view_comparison(engine: 'RemediationMappingEngine', source_view_data: dict, remediated_view_path: str, remed_attributes: set, remed_measures: set, semantic_renamings: dict = None, debug: bool = False) -> list:
    """Generate detailed comparison data for a specific source view"""

    # Get data sources from remediated view
    remed_data_sources = resolve_recursive_data_sources(engine, remediated_view_path)

    # Extract descriptions and hidden status from both views
    source_descriptions = extract_field_descriptions(source_view_data['view_path'], debug)
    remed_descriptions = extract_field_descriptions(remediated_view_path, debug)
    source_hidden_status = extract_field_hidden_status(source_view_data['view_path'], debug)
    remed_hidden_status = extract_field_hidden_status(remediated_view_path, debug)

    detailed_rows = []

    # Process attributes
    for attr in source_view_data['attributes']:
        row_data = process_field_comparison(engine, attr, 'ATTRIBUTE', source_descriptions, remed_descriptions,
                                          remed_attributes, remed_measures, remed_data_sources, source_hidden_status, remed_hidden_status, semantic_renamings, debug)
        detailed_rows.append(row_data)

    # Process measures
    for measure in source_view_data['measures']:
        row_data = process_field_comparison(engine, measure, 'MEASURE', source_descriptions, remed_descriptions,
                                          remed_attributes, remed_measures, remed_data_sources, source_hidden_status, remed_hidden_status, semantic_renamings, debug)
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
                surplus_row = ['', '', '', 'N/A', attr, remed_descriptions.get(attr, ''), 'ATTRIBUTE', target_hidden, 'N/A', 'N/A', 'N/A', 'N/A']
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
                surplus_row = ['', '', '', 'N/A', measure, remed_descriptions.get(measure, ''), 'MEASURE', target_hidden, 'N/A', 'N/A', 'N/A', 'N/A']
                detailed_rows.append(surplus_row)

    return detailed_rows


def process_field_comparison(engine: 'RemediationMappingEngine', field_name: str, field_type: str, source_descriptions: dict, remed_descriptions: dict,
                           remed_attributes: set, remed_measures: set, remed_data_sources: set, source_hidden_status: dict = None, remed_hidden_status: dict = None, semantic_renamings: dict = None, debug: bool = False) -> list:
    """Process a single field comparison and return row data"""

    # Initialize row with source field data
    source_column_name = field_name
    source_description = source_descriptions.get(field_name, '')
    source_type = field_type

    # Initialize target fields (will be populated if mapping found)
    target_column_name = ''
    target_description = ''
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
        target_type = field_type
        rename_flag = 'N'
        matching_type = 'Y'
        # Check if descriptions match
        if source_description and target_description:
            matching_description = 'Y' if source_description == target_description else 'N'
        else:
            matching_description = 'N/A'
    elif field_name in opposite_fields:
        # Field exists but with different type (type mismatch)
        target_column_name = field_name
        target_description = remed_descriptions.get(field_name, '')
        target_type = 'MEASURE' if field_type == 'ATTRIBUTE' else 'ATTRIBUTE'
        rename_flag = 'N'
        matching_type = 'TYPE_MISMATCH'
        # Check if descriptions match
        if source_description and target_description:
            matching_description = 'Y' if source_description == target_description else 'N'
        else:
            matching_description = 'N/A'
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
            if mapping_type_mismatch:
                target_type = 'MEASURE' if field_type == 'ATTRIBUTE' else 'ATTRIBUTE'
                matching_type = 'TYPE_MISMATCH'
            else:
                target_type = field_type
                matching_type = 'Y'
            rename_flag = 'Y' if field_name != mapping_found.s4_field else 'N'
            # Check if descriptions match
            if source_description and target_description:
                matching_description = 'Y' if source_description == target_description else 'N'
            else:
                matching_description = 'N/A'
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
                        if semantic_type_mismatch:
                            target_type = 'MEASURE' if field_type == 'ATTRIBUTE' else 'ATTRIBUTE'
                            matching_type = 'TYPE_MISMATCH'
                        else:
                            target_type = field_type
                            matching_type = 'Y'
                        rename_flag = 'Y'
                        # Check if descriptions match
                        if source_description and target_description:
                            matching_description = 'Y' if source_description == target_description else 'N'
                        else:
                            matching_description = 'N/A'

            if not semantic_renaming_found:
                # No mapping or semantic renaming found - field is missing
                target_column_name = ''
                target_description = ''
                target_type = ''
                rename_flag = 'N/A'
                matching_type = 'N/A'

    # Get hidden status
    source_hidden = 'Y' if source_hidden_status and source_hidden_status.get(field_name, False) else 'N'
    target_hidden = 'Y' if remed_hidden_status and target_column_name and remed_hidden_status.get(target_column_name, False) else ('N' if target_column_name else 'N/A')

    # Check if hidden status matches
    matching_hidden = 'N/A' if target_hidden == 'N/A' else ('Y' if source_hidden == target_hidden else 'N')

    # Create row data (added HIDDEN columns and check)
    row_data = [
        source_column_name, source_description, source_type, source_hidden,
        target_column_name, target_description, target_type, target_hidden,
        rename_flag, matching_description, matching_type, matching_hidden
    ]

    return row_data


def generate_union_view_comparison(engine: 'RemediationMappingEngine', input_view_data: list, remediated_view_path: str, remed_attributes: set, remed_measures: set, semantic_renamings: dict = None, debug: bool = False) -> list:
    """Generate union comparison data for all source views combined"""

    # Get data sources from remediated view
    remed_data_sources = resolve_recursive_data_sources(engine, remediated_view_path)

    # Extract descriptions and hidden status from remediated view
    remed_descriptions = extract_field_descriptions(remediated_view_path, debug)
    remed_hidden_status = extract_field_hidden_status(remediated_view_path, debug)

    # Create union of all source fields
    union_attributes = set()
    union_measures = set()
    union_descriptions = {}
    union_hidden_status = {}

    for view_data in input_view_data:
        union_attributes.update(view_data['attributes'])
        union_measures.update(view_data['measures'])

        # Extract descriptions and hidden status from this source view
        source_descriptions = extract_field_descriptions(view_data['view_path'], debug)
        source_hidden_status = extract_field_hidden_status(view_data['view_path'], debug)
        union_descriptions.update(source_descriptions)
        union_hidden_status.update(source_hidden_status)

    detailed_rows = []

    # Process union attributes
    for attr in sorted(union_attributes):
        row_data = process_field_comparison(engine, attr, 'ATTRIBUTE', union_descriptions, remed_descriptions,
                                          remed_attributes, remed_measures, remed_data_sources, union_hidden_status, remed_hidden_status, semantic_renamings, debug)
        detailed_rows.append(row_data)

    # Process union measures
    for measure in sorted(union_measures):
        row_data = process_field_comparison(engine, measure, 'MEASURE', union_descriptions, remed_descriptions,
                                          remed_attributes, remed_measures, remed_data_sources, union_hidden_status, remed_hidden_status, semantic_renamings, debug)
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


def generate_remediation_report(engine: 'RemediationMappingEngine', num_inputs: int, output_file: str, semantic_renaming_file: str = None, debug: bool = False):
    """Generate a comprehensive remediation report"""
    import pandas as pd

    # Find calculation views in both directories
    script_dir = Path(__file__).parent
    cv_input_dir = script_dir / "inputs" / "cv"
    cv_remediated_dir = script_dir / "inputs" / "cv_remediated"
    reports_dir = script_dir / "reports"

    # Create reports directory if it doesn't exist
    reports_dir.mkdir(exist_ok=True)

    # Update output file path to use reports directory
    output_filename = os.path.basename(output_file)
    output_file = str(reports_dir / output_filename)

    calc_views = sorted(find_calculation_views(str(cv_input_dir)))
    remediated_views = sorted(find_calculation_views(str(cv_remediated_dir)))

    if not calc_views:
        print("No calculation view files found in inputs/cv.")
        sys.exit(1)

    if not remediated_views:
        print("No calculation view files found in inputs/cv_remediated.")
        sys.exit(1)

    # Select input view(s)
    if num_inputs == 1:
        # Single input view selection
        print("\nAvailable Original Calculation Views:")
        for i, view_path in enumerate(calc_views, 1):
            view_name = os.path.basename(view_path)
            relative_path = os.path.relpath(view_path, str(cv_input_dir))
            print(f"{i}. {view_name} (inputs/cv/{relative_path})")

        try:
            choice = int(input("\nSelect an original calculation view (number): ")) - 1
            if choice < 0 or choice >= len(calc_views):
                print("Invalid selection.")
                sys.exit(1)
        except ValueError:
            print("Invalid input. Please enter a number.")
            sys.exit(1)

        selected_input_views = [calc_views[choice]]
    else:
        # Multi-input view selection
        print(f"\nSelect {num_inputs} original calculation views:")
        print("\nAvailable Original Calculation Views:")
        for i, view_path in enumerate(calc_views, 1):
            view_name = os.path.basename(view_path)
            relative_path = os.path.relpath(view_path, str(cv_input_dir))
            print(f"{i}. {view_name} (inputs/cv/{relative_path})")

        selected_input_views = []
        i = 0
        while i < num_inputs:
            try:
                choice = int(input(f"\nSelect input view {i+1} (number): ")) - 1
                if choice < 0 or choice >= len(calc_views):
                    print("Invalid selection.")
                    sys.exit(1)
                if calc_views[choice] in selected_input_views:
                    print("View already selected. Please choose a different view.")
                    continue
                selected_input_views.append(calc_views[choice])
                i += 1
            except ValueError:
                print("Invalid input. Please enter a number.")
                sys.exit(1)

    # Select remediated view
    print("\nAvailable Remediated Calculation Views:")
    for j, view_path in enumerate(remediated_views, 1):
        view_name = os.path.basename(view_path)
        relative_path = os.path.relpath(view_path, str(cv_remediated_dir))
        print(f"{j}. {view_name} (inputs/cv_remediated/{relative_path})")

    try:
        choice = int(input("\nSelect a remediated calculation view (number): ")) - 1
        if choice < 0 or choice >= len(remediated_views):
            print("Invalid selection.")
            sys.exit(1)
    except ValueError:
        print("Invalid input. Please enter a number.")
        sys.exit(1)

    selected_remediated = remediated_views[choice]

    # Load semantic renaming if provided
    semantic_renamings = {}
    if semantic_renaming_file:
        if debug:
            print(f"Loading renamings from: {semantic_renaming_file}")

        # Read the CSV file and create mapping dictionary
        try:
            with open(semantic_renaming_file, 'r', encoding='utf-8') as f:
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

                # Create the header structure (added HIDDEN columns)
                # Source: cols 0-3, Target: cols 4-7, CHECK: cols 8-11
                header_row_1 = [source_view_name, '', '', '', target_view_name, '', '', '', 'CHECK', '', '', '']
                header_row_2 = [
                    'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
                    'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
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
                union_header_row_1 = ['All Source Views (Union)', '', '', '', os.path.basename(selected_remediated).replace('.calculationview', ''), '', '', '', 'CHECK', '', '', '']
                union_header_row_2 = [
                    'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
                    'COLUMN_NAME', 'COLUMN_DESCRIPTION', 'COLUMN_TYPE', 'HIDDEN? (Y/N)',
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


def main():
    """Main function with menu for calculation view selection"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Remediation Mapping Engine for ECC to S/4HANA calculation view migration')
    parser.add_argument('--hide-remapped', action='store_true', 
                       help='Hide the remapped adjacency list from output')
    parser.add_argument('--remediated', '-r', action='store_true',
                       help='Only output exhaustive list of 1:1 field mappings, skip table-to-itself mappings, and skip printing adjacency lists and summary')
    parser.add_argument('--list-measures', '-m', action='store_true',
                       help='List all non-attribute fields (measures) with their aggregation types and mappings for each node')
    parser.add_argument('--show-adjacency', '-a', action='store_true',
                       help='Show original and remapped adjacency lists in output')
    parser.add_argument('--pager', '-p', action='store_true',
                       help='[DEPRECATED] Display output in a pager (less/more) for better readability')
    parser.add_argument('--debug', '-d', action='store_true',
                       help='Show verbose debug output during processing')
    parser.add_argument('--compare', '-c', action='store_true',
                       help='Compare output columns between original and remediated calculation views based on field mappings')
    parser.add_argument('--similarity', '-s', action='store_true',
                       help='Calculate similarity scores between two calculation views with optional directory selection')
    parser.add_argument('--output', '-o', type=str,
                       help='Save output to specified file instead of printing to console')
    parser.add_argument('--sources', action='store_true',
                       help='List all schema.table data sources for calculation views in a selected folder')
    parser.add_argument('--inputs', '-i', type=int,
                       help='Number of input views to select for multi-input comparison (use with --compare)')
    parser.add_argument('--report', action='store_true',
                       help='Generate a comprehensive CSV report of the remediation (requires --output with .csv suffix)')
    parser.add_argument('--renamings', type=str,
                       help='CSV file containing additional semantic renamings for report generation')
    args = parser.parse_args()

    # Validate report flag requirements
    if args.report:
        if not args.output:
            print("Error: --report requires --output (-o) flag with a .csv file specified")
            sys.exit(1)
        if not (args.output.endswith('.csv') or args.output.endswith('.xlsx')):
            print("Error: --report requires output file to have .csv or .xlsx suffix")
            sys.exit(1)
        if args.renamings:
            # Check if renamings file exists and is in the correct directory
            script_dir = Path(__file__).parent
            renamings_dir = script_dir / "inputs/renamings"

            # If relative path given, assume it's in inputs/renamings
            if not os.path.isabs(args.renamings):
                full_renamings_path = renamings_dir / args.renamings
            else:
                full_renamings_path = Path(args.renamings)

            # Check if file exists
            if not full_renamings_path.exists():
                print(f"Error: Renamings file not found: {full_renamings_path}")
                if not os.path.isabs(args.renamings):
                    print(f"Expected location: {renamings_dir}")
                sys.exit(1)

            # Update args.renamings to the full path for later use
            args.renamings = str(full_renamings_path)

    # File paths for inputs
    script_dir = Path(__file__).parent
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
        # Default to single input if --inputs not specified
        num_inputs = args.inputs if args.inputs else 1
        generate_remediation_report(engine, num_inputs, args.output, args.renamings, args.debug)
        return

    # Handle similarity mode
    if args.similarity:
        # Directory selection for both files
        print("\nSelect directory for File 1:")
        print("1. inputs/cv (original)")
        print("2. inputs/cv_remediated (remediated)")
        
        try:
            dir1_choice = int(input("Select directory for File 1 (1 or 2): "))
            if dir1_choice not in [1, 2]:
                print("Invalid selection.")
                sys.exit(1)
        except ValueError:
            print("Invalid input. Please enter 1 or 2.")
            sys.exit(1)
        
        dir1 = script_dir / "inputs" / ("cv" if dir1_choice == 1 else "cv_remediated")
        
        print("\nSelect directory for File 2:")
        print("1. inputs/cv (original)")
        print("2. inputs/cv_remediated (remediated)")
        
        try:
            dir2_choice = int(input("Select directory for File 2 (1 or 2): "))
            if dir2_choice not in [1, 2]:
                print("Invalid selection.")
                sys.exit(1)
        except ValueError:
            print("Invalid input. Please enter 1 or 2.")
            sys.exit(1)
        
        dir2 = script_dir / "inputs" / ("cv" if dir2_choice == 1 else "cv_remediated")
        
        # Find calculation views in selected directories
        calc_views1 = find_calculation_views(str(dir1))
        calc_views2 = find_calculation_views(str(dir2))
        
        if not calc_views1:
            print(f"No calculation view files found in {dir1}.")
            sys.exit(1)
        
        if not calc_views2:
            print(f"No calculation view files found in {dir2}.")
            sys.exit(1)
        
        # Present menu for file 1
        print(f"\nAvailable Calculation Views in {dir1.name}:")
        for i, view_path in enumerate(calc_views1, 1):
            view_name = os.path.basename(view_path)
            relative_path = os.path.relpath(view_path, str(dir1))
            print(f"{i}. {view_name} ({dir1.name}/{relative_path})")
        
        # Get user selection for file 1
        try:
            choice = int(input("\nSelect File 1 (number): ")) - 1
            if choice < 0 or choice >= len(calc_views1):
                print("Invalid selection.")
                sys.exit(1)
        except ValueError:
            print("Invalid input. Please enter a number.")
            sys.exit(1)
        
        selected_file1 = calc_views1[choice]
        
        # Present menu for file 2
        print(f"\nAvailable Calculation Views in {dir2.name}:")
        for i, view_path in enumerate(calc_views2, 1):
            view_name = os.path.basename(view_path)
            relative_path = os.path.relpath(view_path, str(dir2))
            print(f"{i}. {view_name} ({dir2.name}/{relative_path})")
        
        # Get user selection for file 2
        try:
            choice = int(input("\nSelect File 2 (number): ")) - 1
            if choice < 0 or choice >= len(calc_views2):
                print("Invalid selection.")
                sys.exit(1)
        except ValueError:
            print("Invalid input. Please enter a number.")
            sys.exit(1)
        
        selected_file2 = calc_views2[choice]
        
        # Perform similarity analysis
        if args.pager:
            with pager_output():
                calculate_similarity_scores(engine, selected_file1, selected_file2, args.debug)
        else:
            calculate_similarity_scores(engine, selected_file1, selected_file2, args.debug)
        
        return

    # Handle compare mode
    if args.sources:
        # Sources listing mode
        print("Select folder to analyze:")
        print("1. Original calculation views (inputs/cv)")
        print("2. Remediated calculation views (inputs/cv_remediated)")

        try:
            choice = int(input("\nSelect folder (1 or 2): "))
            if choice == 1:
                cv_dir = script_dir / "inputs" / "cv"
                folder_name = "Original (inputs/cv)"
            elif choice == 2:
                cv_dir = script_dir / "inputs" / "cv_remediated"
                folder_name = "Remediated (inputs/cv_remediated)"
            else:
                print("Invalid selection.")
                sys.exit(1)
        except ValueError:
            print("Invalid input. Please enter 1 or 2.")
            sys.exit(1)

        calc_views = sorted(find_calculation_views(str(cv_dir)))
        if not calc_views:
            print(f"No calculation view files found in {cv_dir}.")
            sys.exit(1)

        print(f"\n{'='*80}")
        print(f"DATA SOURCES ANALYSIS - {folder_name}")
        print(f"{'='*80}")

        all_sources = set()

        for view_path in calc_views:
            view_name = os.path.basename(view_path)
            print(f"\n{view_name}:")
            print("-" * len(view_name + ":"))

            try:
                # Get all recursive data sources for this view
                recursive_sources = resolve_recursive_data_sources(engine, view_path)

                if recursive_sources:

                    # Extract schema.table pairs
                    view_sources = set()

                    # Parse the XML to get schema information
                    tree = ET.parse(view_path)
                    root = tree.getroot()

                    # Find all DataSource elements
                    datasources = []
                    datasources.extend(root.findall('.//DataSource'))

                    for ds in datasources:
                        table_name = ds.get('id')
                        if table_name in recursive_sources:
                            # Find the columnObject to get schema
                            col_obj = ds.find('.//columnObject')
                            if col_obj is not None:
                                schema_name = col_obj.get('schemaName')
                                column_obj_name = col_obj.get('columnObjectName')
                                if schema_name and column_obj_name:
                                    source_entry = f"{schema_name}.{column_obj_name}"
                                    view_sources.add(source_entry)
                                    all_sources.add(source_entry)

                    # Sort and display sources for this view
                    sorted_sources = sorted(view_sources)
                    for source in sorted_sources:
                        print(f"  {source}")

                    if not sorted_sources:
                        print("  No schema.table sources found")

                else:
                    print("  No data sources found")

            except Exception as e:
                print(f"  Error analyzing {view_name}: {e}")

        # Summary
        print(f"\n{'='*80}")
        print("SUMMARY - ALL UNIQUE SCHEMA.TABLE SOURCES:")
        print(f"{'='*80}")

        if all_sources:
            sorted_all_sources = sorted(all_sources)
            for source in sorted_all_sources:
                print(f"  {source}")
            print(f"\nTotal unique sources: {len(sorted_all_sources)}")
        else:
            print("No schema.table sources found")

        return

    if args.compare:
        # Check if multi-input comparison is requested
        if args.inputs and args.inputs > 1:
            # Multi-input comparison mode
            multi_input_compare(engine, args.inputs, args.debug, args.output, args.pager)
            return

        # Single input comparison mode (existing logic)
        # Find calculation views in both directories
        cv_input_dir = script_dir / "inputs" / "cv"
        cv_remediated_dir = script_dir / "inputs" / "cv_remediated"

        calc_views = sorted(find_calculation_views(str(cv_input_dir)))
        remediated_views = sorted(find_calculation_views(str(cv_remediated_dir)))
        
        if not calc_views:
            print("No calculation view files found in inputs/cv.")
            sys.exit(1)
        
        if not remediated_views:
            print("No calculation view files found in inputs/cv_remediated.")
            sys.exit(1)
        
        # Present menu for original calculation view
        print("\nAvailable Original Calculation Views:")
        for i, view_path in enumerate(calc_views, 1):
            view_name = os.path.basename(view_path)
            relative_path = os.path.relpath(view_path, str(cv_input_dir))
            print(f"{i}. {view_name} (inputs/cv/{relative_path})")
        
        # Get user selection for original view
        try:
            choice = int(input("\nSelect an original calculation view to compare (number): ")) - 1
            if choice < 0 or choice >= len(calc_views):
                print("Invalid selection.")
                sys.exit(1)
        except ValueError:
            print("Invalid input. Please enter a number.")
            sys.exit(1)
        
        selected_original = calc_views[choice]
        
        # Present menu for remediated calculation view
        print("\nAvailable Remediated Calculation Views:")
        for i, view_path in enumerate(remediated_views, 1):
            view_name = os.path.basename(view_path)
            relative_path = os.path.relpath(view_path, str(cv_remediated_dir))
            print(f"{i}. {view_name} (inputs/cv_remediated/{relative_path})")
        
        # Get user selection for remediated view
        try:
            choice = int(input("\nSelect a remediated calculation view to compare (number): ")) - 1
            if choice < 0 or choice >= len(remediated_views):
                print("Invalid selection.")
                sys.exit(1)
        except ValueError:
            print("Invalid input. Please enter a number.")
            sys.exit(1)
        
        selected_remediated = remediated_views[choice]
        
        # Perform comparison
        if args.output:
            # Redirect output to file
            original_stdout = sys.stdout
            try:
                with open(args.output, 'w', encoding='utf-8') as f:
                    sys.stdout = f
                    compare_calculation_views(engine, selected_original, selected_remediated, args.debug)
            finally:
                sys.stdout = original_stdout
                print(f"Comparison report saved to: {args.output}")
        elif args.pager:
            with pager_output():
                compare_calculation_views(engine, selected_original, selected_remediated, args.debug)
        else:
            compare_calculation_views(engine, selected_original, selected_remediated, args.debug)
        
        return
    
    # Normal mode - find calculation views in the inputs/cv directory
    cv_input_dir = script_dir / "inputs" / "cv"
    calc_views = find_calculation_views(str(cv_input_dir))
    
    if not calc_views:
        print("No calculation view files found.")
        sys.exit(1)
    
    # Present menu
    print("\nAvailable Calculation Views:")
    for i, view_path in enumerate(calc_views, 1):
        view_name = os.path.basename(view_path)
        relative_path = os.path.relpath(view_path, str(cv_input_dir))
        print(f"{i}. {view_name} (inputs/cv/{relative_path})")
    
    # Get user selection
    try:
        choice = int(input("\nSelect a calculation view to analyze (number): ")) - 1
        if choice < 0 or choice >= len(calc_views):
            print("Invalid selection.")
            sys.exit(1)
    except ValueError:
        print("Invalid input. Please enter a number.")
        sys.exit(1)
    
    selected_view = calc_views[choice]
    if args.debug:
        print(f"\nSelected: {os.path.basename(selected_view)}")
    
    # Process the calculation view
    try:
        # Parse calculation view and extract data sources
        if args.debug:
            print(f"\nParsing calculation view: {selected_view}")
        ds, ds_ta, calc_view_sources = engine.parse_calculation_view(selected_view, args.debug)
        
        # Validate DS-TA
        if args.debug:
            print(f"\nValidating DS-TA...")
        if not engine.validate_ds_ta(ds_ta, args.debug):
            print("Validation failed. Stopping execution.")
            sys.exit(1)
        
        # Extract field usage from calculation view
        if args.debug:
            print(f"\nExtracting field usage from calculation view...")
        table_field_usage = engine.extract_field_usage(selected_view, ds_ta)
        
        # Create original adjacency list
        if args.debug:
            print(f"\nCreating original adjacency list...")
        engine.create_original_adjacency_list(selected_view, ds, ds_ta, args.debug)
        
        # Analyze table mappings
        if args.debug:
            print(f"\nAnalyzing table mappings...")
        mapping_results = engine.analyze_table_mappings(ds_ta, table_field_usage)
        
        # Add calculation view sources to summary
        if calc_view_sources:
            if args.debug:
                print(f"\nFound {len(calc_view_sources)} calculation view data sources")
            for cv_source in calc_view_sources:
                engine.summary.append(f"UNCHANGED: {cv_source} (calculation view)")
        
        # Create remapped adjacency list
        if args.debug:
            print(f"\nCreating remapped adjacency list...")
        engine.create_remapped_adjacency_list(mapping_results)
        
        # Collect flagged fields
        engine.collect_flagged_fields(mapping_results)
        
        # Use pager if requested, otherwise normal output
        if args.pager:
            with pager_output():
                # Print field extraction summary (basic output - always show unless using remediated flag)
                if not args.remediated:
                    engine.print_field_extraction_summary(table_field_usage)
                
                # Print measures if requested
                if args.list_measures:
                    engine.print_measures_with_mappings(selected_view, mapping_results)
                
                # Print pretty mappings 
                engine.print_pretty_mappings(mapping_results, table_field_usage, args.remediated)
                
                # Print results (skip if using remediated flag)
                if not args.remediated:
                    engine.print_results(mapping_results, args.hide_remapped, args.show_adjacency)
        else:
            # Print field extraction summary (basic output - always show unless using remediated flag)
            if not args.remediated:
                engine.print_field_extraction_summary(table_field_usage)
            
            # Print measures if requested
            if args.list_measures:
                engine.print_measures_with_mappings(selected_view, mapping_results)
            
            # Print pretty mappings 
            engine.print_pretty_mappings(mapping_results, table_field_usage, args.remediated)
            
            # Print results (skip if using remediated flag)
            if not args.remediated:
                engine.print_results(mapping_results, args.hide_remapped, args.show_adjacency)
        
        # Save results to files
        engine.save_results_to_files(selected_view, mapping_results)
        
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()