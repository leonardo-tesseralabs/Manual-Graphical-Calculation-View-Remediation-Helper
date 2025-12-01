#!/usr/bin/env python3
"""
Business-Logic Calculation View Generator

AI-friendly YAML specification that generates complete SAP HANA calculation views.
Supports node replacements and hierarchical changes for ECC to S/4 remediation.
"""

import yaml
import json
import copy
import argparse
from typing import Dict, Any, List
from cv_parser import CalculationViewParser
from complete_cv_generator import CompleteCalculationViewGenerator
from field_mapping_engine import FieldMappingEngine, FieldMapping


class BusinessCalculationViewGenerator:
    def __init__(self):
        self.parser = CalculationViewParser()
        self.generator = CompleteCalculationViewGenerator()
        self.field_mapping_engine = FieldMappingEngine()
    
    def generate_from_yaml(self, yaml_path: str) -> str:
        """Generate XML from AI-friendly YAML specification"""
        
        # 1. Load YAML business specification
        with open(yaml_path, 'r') as f:
            business_spec = yaml.safe_load(f)
        
        # 2. Parse base template
        base_template = business_spec['BASE_TEMPLATE']
        base_parsed = self.parser.parse_file(base_template)
        base_data = self._dataclass_to_dict(base_parsed)
        
        # 3. Apply business changes with comprehensive field mapping
        modified_data = self._apply_changes_with_field_engine(base_data, business_spec)
        
        # 4. Generate identical XML
        return self.generator.generate_from_parsed_data(modified_data)
    
    def _dataclass_to_dict(self, obj) -> Dict[str, Any]:
        """Convert dataclass to dictionary recursively"""
        if hasattr(obj, '__dataclass_fields__'):
            return {k: self._dataclass_to_dict(v) for k, v in obj.__dict__.items()}
        elif isinstance(obj, list):
            return [self._dataclass_to_dict(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self._dataclass_to_dict(v) for k, v in obj.items()}
        else:
            return obj
    
    def _apply_changes_with_field_engine(self, base_data: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
        """Apply business changes using comprehensive field mapping engine"""
        
        # Work on copy to avoid modifying original
        modified_data = copy.deepcopy(base_data)
        
        # Update view metadata
        modified_data['id'] = spec['VIEW_ID']
        modified_data['descriptions'] = spec.get('DESCRIPTION', spec['VIEW_ID'])
        modified_data['logical_descriptions'] = spec.get('DESCRIPTION', spec['VIEW_ID'])
        
        # Convert YAML mappings to FieldMapping objects
        field_mappings = self._create_field_mappings_from_spec(spec)
        
        # Apply explicit DELETE/ADD operations first
        if 'DELETE_NODES' in spec:
            self._delete_nodes(modified_data, spec['DELETE_NODES'])
            
        if 'ADD_NODES' in spec:
            self._add_nodes(modified_data, spec['ADD_NODES'])
            
        if 'REBUILD_NODES' in spec:
            self._rebuild_nodes(modified_data, spec['REBUILD_NODES'])
            
        if 'ADD_JOINS' in spec:
            self._add_joins(modified_data, spec['ADD_JOINS'])
            # After adding joins, redirect calculation flow to use the final join
            self._integrate_joins_into_flow(modified_data, spec['ADD_JOINS'])
        
        # Fallback: support old NODE_REPLACEMENTS and NEW_NODES format for compatibility
        if 'NODE_REPLACEMENTS' in spec:
            self._apply_node_replacements(modified_data, spec['NODE_REPLACEMENTS'])
        
        if 'NEW_NODES' in spec:
            self._add_new_nodes(modified_data, spec['NEW_NODES'])
        
        # Then apply field mappings to update references
        if field_mappings:
            # Use a simpler field mapping approach since nodes are already handled
            self._apply_simple_field_mappings(modified_data, spec.get('FIELD_MAPPINGS', []))
        
        # Update logical model to use S/4HANA field names
        if 'FIELD_MAPPINGS' in spec:
            self._update_logical_model_field_names(modified_data, spec['FIELD_MAPPINGS'])
            
        return modified_data

    def _create_field_mappings_from_spec(self, spec: Dict[str, Any]) -> List[FieldMapping]:
        """Convert YAML spec to FieldMapping objects"""
        
        field_mappings = []
        
        # Extract field mappings from NODE_REPLACEMENTS
        for replacement in spec.get('NODE_REPLACEMENTS', []):
            from_node = replacement['node_id']
            new_node = replacement['new_node']
            to_node = new_node['id']
            
            # Extract field mappings from the field specifications
            for field_spec in new_node.get('fields', []):
                if isinstance(field_spec, str) and ' as ' in field_spec.lower():
                    # Parse "RBUKRS as BUKRS" format
                    parts = field_spec.split(' as ')
                    source_expr = parts[0].strip()
                    target_field = parts[1].strip()
                    
                    # Handle aggregations like "SUM(HSL) as DMBTR"
                    if source_expr.startswith('SUM(') and source_expr.endswith(')'):
                        source_field = source_expr[4:-1]  # Extract field from SUM(field)
                    else:
                        source_field = source_expr
                    
                    field_mappings.append(FieldMapping(
                        from_field=target_field,   # What template expects (BUKRS)
                        to_field=source_field,     # What ACDOCA provides (RBUKRS)
                        from_node=from_node,       # Original template node (BSEG)
                        to_node=to_node           # New data source (ACDOCA)
                    ))
        
        # Add explicit field mappings
        for fm in spec.get('FIELD_MAPPINGS', []):
            field_mappings.append(FieldMapping(
                from_field=fm['from_field'],
                to_field=fm['to_field'], 
                from_node=fm.get('from_node', 'BSEG'),
                to_node=fm.get('to_node', 'ACDOCA')
            ))
            
        return field_mappings
    
    def _apply_changes(self, base_data: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
        """Apply all business logic changes to base data"""
        
        # Work on copy to avoid modifying original
        modified_data = copy.deepcopy(base_data)
        
        # Update view metadata
        modified_data['id'] = spec['VIEW_ID']
        modified_data['descriptions'] = spec.get('DESCRIPTION', spec['VIEW_ID'])
        modified_data['logical_descriptions'] = spec.get('DESCRIPTION', spec['VIEW_ID'])
        
        # Apply node replacements
        if 'NODE_REPLACEMENTS' in spec:
            self._apply_node_replacements(modified_data, spec['NODE_REPLACEMENTS'])
        
        # Add new nodes
        if 'NEW_NODES' in spec:
            self._add_new_nodes(modified_data, spec['NEW_NODES'])
        
        # Apply simple field/table mappings
        if 'FIELD_MAPPINGS' in spec:
            self._apply_field_mappings(modified_data, spec['FIELD_MAPPINGS'])
            
        if 'TABLE_MAPPINGS' in spec:
            self._apply_table_mappings(modified_data, spec['TABLE_MAPPINGS'])
        
        return modified_data
    
    def _apply_node_replacements(self, data: Dict[str, Any], replacements: list):
        """Replace entire nodes with new structure"""
        
        for replacement in replacements:
            node_id = replacement['node_id']
            new_node = replacement['new_node']
            
            # Replace data source
            for i, ds in enumerate(data.get('data_sources', [])):
                if ds['id'] == node_id:
                    data['data_sources'][i] = {
                        'id': new_node['id'],
                        'type': new_node.get('type', 'DATA_BASE_TABLE'),
                        'schema_name': new_node.get('schema', ds['schema_name']),
                        'column_object_name': new_node.get('table', new_node['id']),
                        'view_attributes_all': True
                    }
                    # Update references to this data source
                    self._update_node_references(data, node_id, new_node['id'])
                    break
            
            # Replace calculation view
            for i, cv in enumerate(data.get('calculation_views', [])):
                if cv['id'] == node_id:
                    data['calculation_views'][i] = self._build_calculation_view(new_node)
                    break
    
    def _build_calculation_view(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Build calculation view from specification"""
        
        cv = {
            'id': spec['id'],
            'type': spec['type'].title() + 'View',
            'descriptions': spec['id'],
            'view_attributes': [],
            'calculated_view_attributes': [],
            'inputs': []
        }
        
        # Add optional properties
        if 'join_type' in spec:
            cv['join_type'] = spec['join_type']
        if 'cardinality' in spec:
            cv['cardinality'] = spec['cardinality']
        if 'join_order' in spec:
            cv['join_order'] = spec['join_order']
        
        # Handle different node types
        if spec.get('type') == 'join':
            return self._build_join_view(spec)
        elif spec.get('type') == 'aggregation':
            return self._build_aggregation_view(spec)
        else:
            # Generic calculation view
            return self._build_generic_view(spec)
    
    def _build_join_view(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Build join calculation view with proper field mappings and conditions"""
        
        cv = {
            'id': spec['id'],
            'type': 'JoinView',
            'descriptions': spec.get('id', ''),
            'view_attributes': [],
            'calculated_view_attributes': [],
            'inputs': [],
            'join_type': spec.get('join_type', 'inner'),
            'join_order': 'OUTSIDE_IN',
            'cardinality': 'CN_1'
        }
        
        # For joins with "all" fields, we need to determine fields from sources
        # For now, create a comprehensive set of expected S/4HANA fields
        if spec.get('fields') == 'all':
            s4_fields = [
                'RCLNT', 'RBUKRS', 'GJAHR', 'BELNR', 'BUZEI', 'RACCT', 'RCNTR',
                'HSL', 'WSL', 'TSL', 'MSL', 'DRCRK', 'RBUSA', 'PS_POSID',
                'RGRANT_NBR', 'RFAREA', 'RBUDGET_PD', 'RHCUR', 'RWCUR', 'RTCUR',
                'ROCUR', 'RUNIT', 'BLDAT', 'BUDAT', 'POPER', 'BLART', 'PRCTR',
                'MATNR', 'WERKS', 'LIFNR', 'KUNNR', 'AUFNR', 'SEGMENT', 'KOKRS',
                'ANLN1', 'ANLN2', 'EBELN', 'EBELP', 'ZEKKN', 'PERNR', 'FISTL',
                'HBKID', 'BSCHL', 'KTOSL', 'MWSKZ', 'AUGDT', 'AUGGJ', 'AUGBL',
                'VALUT', 'NETDT', 'UMSKZ', 'SGTXT', 'ZUONR', 'MEASURE', '_DATAAGING'
            ]
            
            for field in s4_fields:
                cv['view_attributes'].append({'id': field})
        
        # Build inputs with proper mappings
        if 'sources' in spec:
            for source in spec['sources']:
                input_data = {'node': source, 'mappings': []}
                
                # Add field mappings for each source
                for field in cv['view_attributes']:
                    field_id = field['id']
                    input_data['mappings'].append({
                        'type': 'Calculation:AttributeMapping',
                        'target': field_id,
                        'source': field_id
                    })
                
                cv['inputs'].append(input_data)
        
        # Add join conditions if specified
        if 'join_conditions' in spec:
            cv['join_attributes'] = []
            for condition in spec['join_conditions']:
                # Extract field name from condition like "Aggregation_1.MANDT = BSEG_REMAINING.MANDT"
                if '=' in condition:
                    parts = condition.split('=')
                    left_field = parts[0].strip().split('.')[-1]  # Get field name after dot
                    cv['join_attributes'].append(left_field)
        
        return cv
    
    def _build_aggregation_view(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Build aggregation calculation view"""
        
        cv = {
            'id': spec['id'],
            'type': 'AggregationView',
            'descriptions': spec['id'],
            'view_attributes': [],
            'calculated_view_attributes': [],
            'inputs': []
        }
        
        # Build view attributes from fields
        for field in spec.get('fields', []):
            if field == 'all':
                continue
            
            va = {'id': self._extract_field_name(field)}
            
            # Add aggregation if present
            if 'SUM(' in field and spec.get('type') == 'aggregation':
                va['aggregation_type'] = 'sum'
            elif 'COUNT(' in field and spec.get('type') == 'aggregation':
                va['aggregation_type'] = 'count'
            
            cv['view_attributes'].append(va)
        
        # Build inputs
        if 'source' in spec:
            cv['inputs'] = [{'node': spec['source'], 'mappings': []}]
        elif 'sources' in spec:
            cv['inputs'] = []
            for source in spec['sources']:
                cv['inputs'].append({'node': source, 'mappings': []})
        
        # Add filters
        if 'filters' in spec:
            cv['filter_expression'] = ' AND '.join(spec['filters'])
        
        return cv
    
    def _build_generic_view(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Build generic calculation view"""
        
        cv = {
            'id': spec['id'],
            'type': spec['type'].title() + 'View',
            'descriptions': spec['id'],
            'view_attributes': [],
            'calculated_view_attributes': [],
            'inputs': []
        }
        
        # Build view attributes from fields
        for field in spec.get('fields', []):
            if field == 'all':
                continue
            
            va = {'id': self._extract_field_name(field)}
            cv['view_attributes'].append(va)
        
        # Build inputs
        if 'source' in spec:
            cv['inputs'] = [{'node': spec['source'], 'mappings': []}]
        elif 'sources' in spec:
            cv['inputs'] = []
            for source in spec['sources']:
                cv['inputs'].append({'node': source, 'mappings': []})
        
        return cv
    
    def _extract_field_name(self, field_spec: str) -> str:
        """Extract final field name from specification like 'SUM(WSL) as WRBTR'"""
        if ' as ' in field_spec.lower():
            return field_spec.split(' as ')[-1].strip()
        elif '(' in field_spec and ')' in field_spec:
            # Extract from SUM(FIELD) format
            return field_spec.split('(')[1].split(')')[0].strip()
        else:
            return field_spec.strip()
    
    def _update_node_references(self, data: Dict[str, Any], old_id: str, new_id: str):
        """Update all references to a node ID"""
        
        # Update calculation view inputs
        for cv in data.get('calculation_views', []):
            for input_data in cv.get('inputs', []):
                if input_data['node'] == old_id:
                    input_data['node'] = new_id
    
    def _add_new_nodes(self, data: Dict[str, Any], new_nodes: list):
        """Add completely new nodes"""
        
        for node_spec in new_nodes:
            if node_spec.get('type') == 'DATA_BASE_TABLE':
                # Add new data source
                new_ds = {
                    'id': node_spec['id'],
                    'type': 'DATA_BASE_TABLE',
                    'schema_name': node_spec.get('schema', 'SLT_DR0'),
                    'column_object_name': node_spec.get('table', node_spec['id']),
                    'view_attributes_all': True
                }
                data.setdefault('data_sources', []).append(new_ds)
            else:
                # Add new calculation view
                new_cv = self._build_calculation_view(node_spec)
                data.setdefault('calculation_views', []).append(new_cv)
    
    def _apply_field_mappings(self, data: Dict[str, Any], mappings: list):
        """Apply simple field name changes"""
        
        for mapping in mappings:
            from_field = mapping['from_field']
            to_field = mapping['to_field']
            
            # Update in calculation views
            for cv in data.get('calculation_views', []):
                for va in cv.get('view_attributes', []):
                    if va['id'] == from_field:
                        va['id'] = to_field
                
                # Update mappings
                for input_data in cv.get('inputs', []):
                    for map_data in input_data.get('mappings', []):
                        if map_data.get('target') == from_field:
                            map_data['target'] = to_field
                        if map_data.get('source') == from_field:
                            map_data['source'] = to_field
            
            # Update logical model
            for attr in data.get('logical_attributes', []):
                if attr['column_name'] == from_field:
                    attr['column_name'] = to_field
                    attr['id'] = to_field
            
            for measure in data.get('logical_measures', []):
                if measure['column_name'] == from_field:
                    measure['column_name'] = to_field
                    measure['id'] = to_field
    
    def _apply_table_mappings(self, data: Dict[str, Any], mappings: list):
        """Apply table name changes"""
        
        for mapping in mappings:
            from_table = mapping['from_table']
            to_table = mapping['to_table']
            to_schema = mapping.get('to_schema')
            
            # Update data sources
            for ds in data.get('data_sources', []):
                if ds['column_object_name'] == from_table:
                    ds['column_object_name'] = to_table
                    ds['id'] = to_table
                    if to_schema:
                        ds['schema_name'] = to_schema
            
            # Update node references
            self._update_node_references(data, from_table, to_table)
    
    def _delete_nodes(self, data: Dict[str, Any], delete_nodes: list):
        """Delete nodes explicitly"""
        for node_id in delete_nodes:
            # Remove from data sources
            data['data_sources'] = [ds for ds in data.get('data_sources', []) if ds['id'] != node_id]
            
            # Remove from calculation views
            data['calculation_views'] = [cv for cv in data.get('calculation_views', []) if cv['id'] != node_id]
            
            print(f"🗑️  Deleted node: {node_id}")
    
    def _add_nodes(self, data: Dict[str, Any], add_nodes: list):
        """Add nodes explicitly"""
        for node_spec in add_nodes:
            if node_spec['type'] == 'DATA_BASE_TABLE':
                new_ds = {
                    'id': node_spec['id'],
                    'type': 'DATA_BASE_TABLE',
                    'schema_name': node_spec.get('schema', 'SLT_DR0'),
                    'column_object_name': node_spec.get('table', node_spec['id']),
                    'view_attributes_all': True
                }
                data.setdefault('data_sources', []).append(new_ds)
                print(f"➕ Added data source: {node_spec['id']}")
    
    def _rebuild_nodes(self, data: Dict[str, Any], rebuild_nodes: list):
        """Rebuild calculation view nodes with new structure"""
        for replacement in rebuild_nodes:
            node_id = replacement['node_id']
            new_node = replacement['new_node']
            
            # Find and replace the calculation view
            for i, cv in enumerate(data.get('calculation_views', [])):
                if cv['id'] == node_id:
                    data['calculation_views'][i] = self._build_calculation_view(new_node)
                    print(f"🔄 Rebuilt calculation view: {node_id}")
                    break
    
    def _add_joins(self, data: Dict[str, Any], add_joins: list):
        """Add new join calculation views"""
        for join_spec in add_joins:
            join_cv = self._build_calculation_view(join_spec)
            data.setdefault('calculation_views', []).append(join_cv)
            print(f"🔗 Added join: {join_spec['id']}")
    
    def _integrate_joins_into_flow(self, data: Dict[str, Any], add_joins: list):
        """Integrate join nodes into the main calculation flow"""
        
        # Find the final join (last in the sequence)
        if not add_joins:
            return
            
        final_join_id = add_joins[-1]['id']  # FINAL_BSEG_ADD_Join
        original_source = "Aggregation_1"  # What needs to be replaced
        
        # Update all calculation views that reference the original source
        for cv in data.get('calculation_views', []):
            for input_data in cv.get('inputs', []):
                if input_data['node'] == f"#{original_source}":
                    input_data['node'] = f"#{final_join_id}"
                    print(f"🔄 Redirected {cv['id']} from {original_source} to {final_join_id}")
        
        # Update logical model if it references the original source
        logical_model = data.get('logical_model')
        if logical_model and logical_model.get('id') == original_source:
            logical_model['id'] = final_join_id
            print(f"🔄 Updated logical model to reference {final_join_id}")
        
        print(f"✅ Integrated joins into calculation flow")

    def _apply_simple_field_mappings(self, data: Dict[str, Any], mappings: list):
        """Apply simple field name changes (fallback method)"""
        
        for mapping in mappings:
            from_field = mapping['from_field']
            to_field = mapping['to_field']
            
            # Update in calculation views
            for cv in data.get('calculation_views', []):
                for va in cv.get('view_attributes', []):
                    if va['id'] == from_field:
                        va['id'] = to_field
                
                # Update mappings
                for input_data in cv.get('inputs', []):
                    for map_data in input_data.get('mappings', []):
                        if map_data.get('target') == from_field:
                            map_data['target'] = to_field
                        if map_data.get('source') == from_field:
                            map_data['source'] = to_field
    
    def _update_logical_model_field_names(self, data: Dict[str, Any], field_mappings: list):
        """Update logical model to use S/4HANA field names"""
        
        # Create ECC → S/4HANA field mapping dictionary
        ecc_to_s4_fields = {}
        for mapping in field_mappings:
            if mapping.get('from_node') == 'BSEG' and mapping.get('to_node') == 'ACDOCA':
                ecc_to_s4_fields[mapping['from_field']] = mapping['to_field']
        
        # Update logical attributes
        for attr in data.get('logical_attributes', []):
            old_field = attr.get('column_name')
            if old_field in ecc_to_s4_fields:
                new_field = ecc_to_s4_fields[old_field]
                attr['column_name'] = new_field
                attr['id'] = new_field
                print(f"🔄 Updated logical attribute: {old_field} → {new_field}")
        
        # Update logical measures
        for measure in data.get('logical_measures', []):
            old_field = measure.get('column_name')
            if old_field in ecc_to_s4_fields:
                new_field = ecc_to_s4_fields[old_field]
                measure['column_name'] = new_field  
                measure['id'] = new_field
                print(f"🔄 Updated logical measure: {old_field} → {new_field}")
        
        print(f"✅ Updated logical model field names to S/4HANA")


def main():
    parser = argparse.ArgumentParser(description='Generate SAP HANA calculation view from business YAML')
    parser.add_argument('yaml_spec', help='Path to YAML business specification')
    parser.add_argument('output_xml', help='Path to output XML file')
    
    args = parser.parse_args()
    
    # Generate XML
    generator = BusinessCalculationViewGenerator()
    xml_output = generator.generate_from_yaml(args.yaml_spec)
    
    # Write to file
    with open(args.output_xml, 'w', encoding='utf-8') as f:
        f.write(xml_output)
    
    print(f"Generated calculation view: {args.output_xml}")


if __name__ == '__main__':
    main()