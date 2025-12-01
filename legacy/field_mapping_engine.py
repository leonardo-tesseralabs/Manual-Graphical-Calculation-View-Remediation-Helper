#!/usr/bin/env python3
"""
Field Mapping Engine

Surgical fix for ECC→S/4HANA field remapping that propagates changes through
the entire calculation view dependency hierarchy.
"""

import copy
from typing import Dict, List, Set, Tuple, Any
from dataclasses import dataclass


@dataclass
class FieldMapping:
    from_field: str
    to_field: str
    from_node: str
    to_node: str


@dataclass 
class NodeDependency:
    node_id: str
    inputs: List[str]  # Nodes this depends on
    outputs: List[str]  # Fields this node outputs
    field_mappings: Dict[str, str]  # source → target mappings


class FieldMappingEngine:
    def __init__(self):
        self.dependency_graph: Dict[str, NodeDependency] = {}
        self.field_flows: Dict[str, List[str]] = {}  # field → [nodes that consume it]
        
    def apply_comprehensive_field_mapping(self, parsed_data: Dict[str, Any], field_mappings: List[FieldMapping]) -> Dict[str, Any]:
        """Apply field mappings with explicit DELETE/ADD operations"""
        
        # Work on copy
        modified_data = copy.deepcopy(parsed_data)
        
        # 1. Build complete dependency graph
        self._build_dependency_graph(modified_data)
        
        # 2. Apply explicit DELETE/ADD operations based on field mappings
        self._apply_node_operations(modified_data, field_mappings)
        
        # 3. Rebuild input mappings for consistency
        self._rebuild_all_input_mappings(modified_data)
        
        return modified_data
    
    def _build_dependency_graph(self, data: Dict[str, Any]):
        """Build complete node dependency graph"""
        self.dependency_graph.clear()
        
        # Add data sources
        for ds in data.get('data_sources', []):
            self.dependency_graph[ds['id']] = NodeDependency(
                node_id=ds['id'],
                inputs=[],  # Data sources have no inputs
                outputs=[],  # Will be populated from first consuming node
                field_mappings={}
            )
        
        # Add calculation views
        for cv in data.get('calculation_views', []):
            inputs = []
            field_mappings = {}
            
            # Extract input nodes and mappings
            for input_data in cv.get('inputs', []):
                inputs.append(input_data['node'])
                
                # Extract field mappings from input
                for mapping in input_data.get('mappings', []):
                    field_mappings[mapping['source']] = mapping['target']
            
            # Extract output fields
            outputs = []
            for va in cv.get('view_attributes', []):
                outputs.append(va['id'])
            
            self.dependency_graph[cv['id']] = NodeDependency(
                node_id=cv['id'],
                inputs=inputs,
                outputs=outputs,
                field_mappings=field_mappings
            )
    
    def _apply_node_operations(self, data: Dict[str, Any], field_mappings: List[FieldMapping]):
        """Apply explicit DELETE/ADD operations based on field mappings"""
        
        # Analyze field mappings to determine operations
        node_operations = self._determine_node_operations(field_mappings)
        
        print(f"🔄 Node operations to apply: {node_operations}")
        
        # Apply DELETE operations
        for node_id in node_operations.get('delete', []):
            self._delete_node(data, node_id)
            
        # Apply ADD operations  
        for node_spec in node_operations.get('add', []):
            self._add_node(data, node_spec)
            
        # Update dependency graph after operations
        self._rebuild_dependency_graph(data)
    
    def _determine_node_operations(self, field_mappings: List[FieldMapping]) -> Dict[str, Any]:
        """Determine what DELETE/ADD operations are needed from field mappings"""
        
        operations = {'delete': set(), 'add': []}
        
        # Group mappings by source node (to_node)
        mappings_by_source = {}
        for fm in field_mappings:
            if fm.to_node not in mappings_by_source:
                mappings_by_source[fm.to_node] = []
            mappings_by_source[fm.to_node].append(fm)
        
        for source_node, mappings in mappings_by_source.items():
            # If this source node has many mappings, it's likely a replacement
            if len(mappings) > 5:  # Threshold for major node replacement
                # Collect nodes being replaced
                replaced_nodes = {fm.from_node for fm in mappings}
                operations['delete'].update(replaced_nodes)
                
                # Determine node type - only tables get added as DATA_BASE_TABLE
                if source_node in ['ACDOCA', 'BSEG', 'BKPF', 'BSEG_ADD', 'BSEG_REMAINING']:
                    # This is a table replacement
                    operations['add'].append({
                        'id': source_node,
                        'type': 'DATA_BASE_TABLE',
                        'schema': 'SLT_DR0',
                        'table': source_node,
                        'field_mappings': {fm.from_field: fm.to_field for fm in mappings}
                    })
                else:
                    # This is a calculation view that was deleted and needs reconstruction
                    # We'll handle this differently - don't add it back as a data source
                    print(f"⚠️  Skipping recreation of calculation view: {source_node}")
        
        return {
            'delete': list(operations['delete']),
            'add': operations['add']
        }
    
    def _delete_node(self, data: Dict[str, Any], node_id: str):
        """Delete a node from the calculation view"""
        
        # Remove from data sources
        data['data_sources'] = [ds for ds in data.get('data_sources', []) if ds['id'] != node_id]
        
        # Remove from calculation views  
        data['calculation_views'] = [cv for cv in data.get('calculation_views', []) if cv['id'] != node_id]
        
        # Remove from dependency graph
        if node_id in self.dependency_graph:
            del self.dependency_graph[node_id]
            
        print(f"🗑️  Deleted node: {node_id}")
    
    def _add_node(self, data: Dict[str, Any], node_spec: Dict[str, Any]):
        """Add a new node to the calculation view"""
        
        if node_spec['type'] == 'DATA_BASE_TABLE':
            # Add new data source
            new_ds = {
                'id': node_spec['id'],
                'type': 'DATA_BASE_TABLE',
                'schema_name': node_spec['schema'],
                'column_object_name': node_spec['table'],
                'view_attributes_all': True
            }
            data.setdefault('data_sources', []).append(new_ds)
            
            # Add to dependency graph with field mappings
            self.dependency_graph[node_spec['id']] = NodeDependency(
                node_id=node_spec['id'],
                inputs=[],
                outputs=[],
                field_mappings=node_spec.get('field_mappings', {})
            )
            
        print(f"➕ Added node: {node_spec['id']} ({node_spec['type']})")
    
    def _rebuild_dependency_graph(self, data: Dict[str, Any]):
        """Rebuild dependency graph after DELETE/ADD operations"""
        # Simply rebuild from scratch with new structure
        self._build_dependency_graph(data)
        print(f"🔄 Rebuilt dependency graph with {len(self.dependency_graph)} nodes")
    
    def _apply_node_replacements(self, data: Dict[str, Any], field_mappings: List[FieldMapping]):
        """Apply data source node replacements based on majority mappings"""
        
        # Count mappings per node pair to determine primary replacements
        node_mapping_counts = {}  # (from_node, to_node) → count
        field_mappings_by_source = {}  # source_node → {old_field → new_field}
        
        for fm in field_mappings:
            # Count node mapping pairs
            if fm.from_node != fm.to_node:
                pair = (fm.from_node, fm.to_node)
                node_mapping_counts[pair] = node_mapping_counts.get(pair, 0) + 1
            
            # Track field mappings for each source node
            if fm.to_node not in field_mappings_by_source:
                field_mappings_by_source[fm.to_node] = {}
            field_mappings_by_source[fm.to_node][fm.from_field] = fm.to_field
        
        # Determine primary node replacements (highest count wins)
        node_replacements = {}  # old_node → new_node
        for (from_node, to_node), count in node_mapping_counts.items():
            if from_node not in node_replacements or count > node_mapping_counts.get((from_node, node_replacements[from_node]), 0):
                node_replacements[from_node] = to_node
        
        print(f"🔄 Node replacements: {node_replacements}")
        print(f"🔄 Field mappings by source: {field_mappings_by_source}")
        
        # Debug: print dependency graph after building
        print(f"🔍 Dependency graph nodes: {list(self.dependency_graph.keys())}")
        for node_id, dep in self.dependency_graph.items():
            if dep.field_mappings:
                print(f"🔍 {node_id} field mappings: {dep.field_mappings}")
        
        # Apply data source replacements
        for old_node, new_node in node_replacements.items():
            self._replace_data_source_and_references(data, old_node, new_node, field_mappings_by_source.get(new_node, {}))
            # Update dependency graph to reflect the replacement
            self._update_dependency_graph_after_replacement(old_node, new_node, field_mappings_by_source.get(new_node, {}))
        
        # Update node_field_changes to reflect the replacements
        for old_node, new_node in node_replacements.items():
            if old_node in field_mappings_by_source:
                field_mappings_by_source[new_node] = field_mappings_by_source[old_node]

    def _replace_data_source_and_references(self, data: Dict[str, Any], old_node: str, new_node: str, field_mappings: Dict[str, str]):
        """Replace data source and update all references with field mappings"""
        
        # Replace data source
        for i, ds in enumerate(data.get('data_sources', [])):
            if ds['id'] == old_node:
                data['data_sources'][i] = {
                    'id': new_node,
                    'type': ds['type'],
                    'schema_name': 'SLT_DR0' if new_node == 'ACDOCA' else ds['schema_name'],
                    'column_object_name': new_node,
                    'view_attributes_all': True
                }
                print(f"✅ Replaced data source: {old_node} → {new_node}")
                break
        
        # Update all calculation views that reference this node
        for cv in data.get('calculation_views', []):
            for input_data in cv.get('inputs', []):
                if input_data['node'] == old_node:
                    # Update node reference
                    input_data['node'] = new_node
                    print(f"📎 Updated input reference: {old_node} → {new_node} in {cv['id']}")
                    
                    # Update field mappings
                    for mapping in input_data.get('mappings', []):
                        old_source = mapping.get('source')
                        if old_source in field_mappings:
                            new_source = field_mappings[old_source]
                            mapping['source'] = new_source
                            print(f"  🔄 Updated field mapping: {old_source} → {new_source}")
                            
                    break
    
    def _update_dependency_graph_after_replacement(self, old_node: str, new_node: str, field_mappings: Dict[str, str]):
        """Update dependency graph after node replacement"""
        
        if old_node in self.dependency_graph:
            old_dep = self.dependency_graph[old_node]
            
            # Create new dependency with updated field mappings
            new_dep = NodeDependency(
                node_id=new_node,
                inputs=old_dep.inputs,
                outputs=old_dep.outputs,
                field_mappings=field_mappings  # Use the ACDOCA field mappings
            )
            
            # Replace the old node with new node in dependency graph
            self.dependency_graph[new_node] = new_dep
            del self.dependency_graph[old_node]
            
            # Update any references in other nodes
            for node_id, dep in self.dependency_graph.items():
                # Update inputs that referenced the old node
                dep.inputs = [new_node if inp == old_node else inp for inp in dep.inputs]
                # Update outputs that referenced the old node  
                dep.outputs = [new_node if out == old_node else out for out in dep.outputs]
            
            print(f"🔄 Updated dependency graph: {old_node} → {new_node}")
    
    def _replace_data_source_node(self, data: Dict[str, Any], old_node: str, change_info: Dict[str, Any]):
        """Replace a data source and update first-level consumers"""
        new_node = change_info['new_node']
        field_changes = change_info['field_changes']
        
        # Update data source
        for i, ds in enumerate(data.get('data_sources', [])):
            if ds['id'] == old_node:
                # Update to new table structure (e.g., BSEG → ACDOCA)
                data['data_sources'][i] = {
                    'id': new_node,
                    'type': ds['type'],
                    'schema_name': change_info.get('new_schema', ds['schema_name']),
                    'column_object_name': new_node,
                    'view_attributes_all': True
                }
                print(f"✅ Replaced data source: {old_node} → {new_node}")
                break
        
        # Update calculation views that directly consume this data source
        for cv in data.get('calculation_views', []):
            for input_data in cv.get('inputs', []):
                if input_data['node'] == old_node:
                    # Update node reference
                    input_data['node'] = new_node
                    
                    # Update field mappings within this input
                    for mapping in input_data.get('mappings', []):
                        old_source = mapping['source']
                        if old_source in field_changes:
                            mapping['source'] = field_changes[old_source]
            
            # Update view attributes if this CV directly consumes the changed node
            if any(inp['node'] == new_node for inp in cv.get('inputs', [])):
                self._update_view_attributes_for_source_change(cv, field_changes)
    
    def _update_view_attributes_for_source_change(self, cv: Dict[str, Any], field_changes: Dict[str, str]):
        """Update view attributes when upstream source changes fields"""
        
        for va in cv.get('view_attributes', []):
            # Check if this view attribute maps to a changed field
            field_id = va['id']
            
            # If this field was renamed at source, we need to trace through mappings
            # This is handled in the comprehensive propagation step
            pass
    
    def _propagate_field_changes(self, data: Dict[str, Any], field_mappings: List[FieldMapping]):
        """Propagate field changes through entire dependency hierarchy"""
        
        # Build comprehensive field change map by node
        node_field_changes = {}  # node_id → {old_field → new_field}
        
        for fm in field_mappings:
            if fm.from_node not in node_field_changes:
                node_field_changes[fm.from_node] = {}
            node_field_changes[fm.from_node][fm.from_field] = fm.to_field
        
        print(f"🔄 Propagating field changes for nodes: {list(node_field_changes.keys())}")
        
        # Process nodes in dependency order (topological sort)
        processed_nodes = set()
        
        # Start with data sources and work downstream
        while len(processed_nodes) < len(self.dependency_graph):
            # Find nodes whose dependencies are all processed
            ready_nodes = []
            for node_id, dep in self.dependency_graph.items():
                if node_id not in processed_nodes:
                    if all(inp in processed_nodes or inp not in self.dependency_graph for inp in dep.inputs):
                        ready_nodes.append(node_id)
            
            if not ready_nodes:
                # Handle circular dependencies by processing remaining nodes
                remaining = [n for n in self.dependency_graph.keys() if n not in processed_nodes]
                if remaining:
                    ready_nodes = [remaining[0]]
            
            for current_node in ready_nodes:
                processed_nodes.add(current_node)
                
                # Find calculation view for this node
                cv = self._find_calculation_view(data, current_node)
                if cv:
                    print(f"📝 Processing node: {current_node}")
                    
                    # Apply field changes to this node
                    changes_applied = False
                    
                    # If this node has direct field changes, apply them
                    if current_node in node_field_changes:
                        field_changes = node_field_changes[current_node]
                        changes_applied = self._apply_field_changes_to_node(cv, field_changes)
                        
                    # Update input mappings based on upstream changes
                    input_changes_applied = self._update_node_input_mappings_comprehensive(cv, node_field_changes)
                    
                    if changes_applied or input_changes_applied:
                        print(f"✅ Applied changes to node: {current_node}")
        
        print(f"🎯 Completed field propagation for {len(processed_nodes)} nodes")
    
    def _find_calculation_view(self, data: Dict[str, Any], node_id: str) -> Dict[str, Any]:
        """Find calculation view by node ID"""
        for cv in data.get('calculation_views', []):
            if cv['id'] == node_id:
                return cv
        return None
    
    def _update_node_input_mappings(self, cv: Dict[str, Any], field_renames: Dict[str, str], processed_nodes: Set[str]):
        """Update input mappings based on upstream field changes"""
        
        for input_data in cv.get('inputs', []):
            input_node = input_data['node']
            
            # Only update if upstream node was processed (changed)
            if input_node in processed_nodes:
                for mapping in input_data.get('mappings', []):
                    source_field = mapping['source']
                    
                    # Check if source field was renamed
                    if source_field in field_renames:
                        mapping['source'] = field_renames[source_field]
    
    def _update_node_output_fields(self, cv: Dict[str, Any], field_renames: Dict[str, str]):
        """Update output field names if they were renamed"""
        
        for va in cv.get('view_attributes', []):
            field_id = va['id']
            
            # If this output field corresponds to a renamed input field
            if field_id in field_renames:
                va['id'] = field_renames[field_id]
    
    def _rebuild_all_input_mappings(self, data: Dict[str, Any]):
        """Rebuild all input mappings to ensure consistency"""
        
        for cv in data.get('calculation_views', []):
            for input_data in cv.get('inputs', []):
                input_node_id = input_data['node']
                
                # Get available fields from input node
                input_fields = self._get_node_output_fields(data, input_node_id)
                
                # Rebuild mappings to match available fields
                existing_mappings = {m['target']: m['source'] for m in input_data.get('mappings', [])}
                new_mappings = []
                
                # For each view attribute, ensure proper mapping
                for va in cv.get('view_attributes', []):
                    target_field = va['id']
                    
                    if target_field in existing_mappings:
                        source_field = existing_mappings[target_field]
                        # Verify source field exists in input
                        if source_field in input_fields:
                            new_mappings.append({
                                'type': 'Calculation:AttributeMapping',
                                'target': target_field,
                                'source': source_field
                            })
                        else:
                            # Field doesn't exist in input - this could be a problem
                            print(f"⚠️  Warning: Field '{source_field}' not found in node '{input_node_id}', skipping mapping for '{target_field}'")
                
                input_data['mappings'] = new_mappings
    
    def _get_node_output_fields(self, data: Dict[str, Any], node_id: str) -> List[str]:
        """Get list of fields output by a node"""
        
        # Check data sources - use dependency graph for mapped fields
        for ds in data.get('data_sources', []):
            if ds['id'] == node_id:
                if node_id in self.dependency_graph:
                    # Return the mapped field names (what the data source actually provides)
                    node_dep = self.dependency_graph[node_id]
                    mapped_fields = list(node_dep.field_mappings.values())  # The "to_field" values
                    return mapped_fields if mapped_fields else []
                return []  # Data sources without mappings
        
        # Check calculation views
        for cv in data.get('calculation_views', []):
            if cv['id'] == node_id:
                return [va['id'] for va in cv.get('view_attributes', [])]
        
        return []

    def _apply_field_changes_to_node(self, cv: Dict[str, Any], field_changes: Dict[str, str]) -> bool:
        """Apply field changes directly to a calculation view node"""
        changes_applied = False
        
        # Update view attributes - DON'T rename output fields, they stay as expected by downstream
        # The field mapping happens at the INPUT level, not output level
        # Example: Aggregation_1 still outputs "DMBTR" but sources it from "HSL"
        
        # Only update input mappings, not view attribute IDs
        # View attributes should keep their expected names for downstream nodes
        
        # Update input mappings
        for input_data in cv.get('inputs', []):
            for mapping in input_data.get('mappings', []):
                # Update target field names
                if mapping.get('target') in field_changes:
                    old_target = mapping['target']
                    new_target = field_changes[old_target]
                    mapping['target'] = new_target
                    print(f"  🎯 Updated mapping target: {old_target} → {new_target}")
                    changes_applied = True
                    
                # Update source field names based on node replacement
                # If this input comes from a replaced node, update source field names
                if mapping.get('source') in field_changes:
                    old_source = mapping['source']
                    new_source = field_changes[old_source]
                    mapping['source'] = new_source
                    print(f"  📥 Updated mapping source: {old_source} → {new_source}")
                    changes_applied = True
        
        return changes_applied
    
    def _update_node_input_mappings_comprehensive(self, cv: Dict[str, Any], node_field_changes: Dict[str, Dict[str, str]]) -> bool:
        """Update input mappings based on upstream field changes"""
        changes_applied = False
        
        for input_data in cv.get('inputs', []):
            input_node = input_data['node']
            
            # Check if the input node has field changes
            if input_node in node_field_changes:
                input_changes = node_field_changes[input_node]
                
                for mapping in input_data.get('mappings', []):
                    source_field = mapping.get('source')
                    
                    # If the source field was renamed in the input node
                    if source_field in input_changes:
                        old_source = source_field
                        new_source = input_changes[source_field]
                        mapping['source'] = new_source
                        print(f"  🔗 Updated upstream mapping: {input_node}.{old_source} → {input_node}.{new_source}")
                        changes_applied = True
        
        return changes_applied


def create_ecc_s4_field_mappings() -> List[FieldMapping]:
    """Create comprehensive ECC→S/4HANA field mappings from the provided list"""
    
    mappings = []
    
    # BSEG → ACDOCA mappings (major structural change)
    bseg_acdoca_mappings = [
        ("MANDT", "RCLNT"), ("BUKRS", "RBUKRS"), ("HKONT", "RACCT"),
        ("KOSTL", "RCNTR"), ("DMBTR", "HSL"), ("WRBTR", "WSL"),
        ("PSWBT", "TSL"), ("PSWSL", "RTCUR"), ("WAERS", "RWCUR"),
        ("GJAHR", "GJAHR"), ("BELNR", "BELNR"), ("BUZEI", "BUZEI"),
        ("SHKZG", "DRCRK"), ("GSBER", "RBUSA"), ("SEGMENT", "SEGMENT"),
        ("AUFNR", "AUFNR"), ("PRCTR", "PRCTR"), ("ANLN1", "ANLN1"),
        ("ANLN2", "ANLN2"), ("MATNR", "MATNR"), ("WERKS", "WERKS"),
        ("LIFNR", "LIFNR"), ("KUNNR", "KUNNR"), ("MWSKZ", "MWSKZ"),
        ("PROJK", "PS_POSID"), ("EBELN", "EBELN"), ("EBELP", "EBELP"),
        ("ZEKKN", "ZEKKN"), ("PERNR", "PERNR"), ("GRANT_NBR", "RGRANT_NBR"),
        ("FISTL", "FISTL"), ("FKBER", "RFAREA"), ("BUDGET_PD", "RBUDGET_PD"),
        ("HBKID", "HBKID"), ("BSCHL", "BSCHL"), ("KTOSL", "KTOSL"),
        ("AUGDT", "AUGDT"), ("AUGGJ", "AUGGJ"), ("AUGBL", "AUGBL"),
        ("UMSKZ", "UMSKZ"), ("SGTXT", "SGTXT"), ("ZUONR", "ZUONR"),
        ("MEASURE", "MEASURE"), ("_DATAAGING", "_DATAAGING")
    ]
    
    for from_field, to_field in bseg_acdoca_mappings:
        mappings.append(FieldMapping(
            from_field=from_field,
            to_field=to_field,
            from_node="BSEG",
            to_node="ACDOCA"
        ))
    
    # BKPF mappings (some fields move from BSEG to BKPF)
    bkpf_mappings = [
        ("AWKEY", "AWKEY"),  # BSEG.AWKEY → BKPF.AWKEY
    ]
    
    for from_field, to_field in bkpf_mappings:
        mappings.append(FieldMapping(
            from_field=from_field,
            to_field=to_field,
            from_node="BSEG",
            to_node="BKPF"
        ))
    
    return mappings


if __name__ == '__main__':
    # Test the field mapping engine
    engine = FieldMappingEngine()
    mappings = create_ecc_s4_field_mappings()
    
    print(f"Created {len(mappings)} ECC→S/4HANA field mappings")
    print("Key mappings:")
    for fm in mappings[:10]:
        print(f"  {fm.from_node}.{fm.from_field} → {fm.to_node}.{fm.to_field}")