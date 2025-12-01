#!/usr/bin/env python3
"""
Clean Graph Transformation Engine

Applies YAML transformation specifications to dependency graphs.
Focuses on correctness and maintainability over complexity.
"""

import yaml
import copy
from typing import Dict, List, Set, Any, Optional, Tuple
from dependency_graph import (
    DependencyGraph, DependencyGraphBuilder, GraphNode, GraphEdge,
    CalculatedColumn, FilterExpression, InputParameter
)
from cv_parser import CalculationViewParser


class FieldLineage:
    """Track field origins and transformations across the graph"""
    
    def __init__(self):
        self.field_origins: Dict[str, Dict[str, str]] = {}  # node_id -> {field_name -> source_node}
        self.transformed_sources: Set[str] = set()  # nodes with semantic transformations
        self.field_transformations: Dict[str, Dict[str, str]] = {}  # source_node -> {old_field -> new_field}
    
    def track_field_origin(self, node_id: str, field_name: str, source_node: str):
        """Track where a field originates from"""
        if node_id not in self.field_origins:
            self.field_origins[node_id] = {}
        self.field_origins[node_id][field_name] = source_node
    
    def get_field_origin(self, node_id: str, field_name: str) -> Optional[str]:
        """Get the origin node of a field"""
        return self.field_origins.get(node_id, {}).get(field_name)
    
    def add_transformed_source(self, node_id: str, transformations: Dict[str, str]):
        """Mark a node as having field transformations"""
        self.transformed_sources.add(node_id)
        self.field_transformations[node_id] = transformations
    
    def get_field_transformation(self, source_node: str, field_name: str) -> str:
        """Get transformed field name for a source"""
        return self.field_transformations.get(source_node, {}).get(field_name, field_name)


class GraphTransformer:
    """Clean, focused graph transformer"""
    
    def __init__(self):
        self.original_graph: Optional[DependencyGraph] = None
        self.transformed_graph: Optional[DependencyGraph] = None
        self.yaml_spec: Dict[str, Any] = {}
        self.node_id_mappings: Dict[str, str] = {}  # Track node renames
        self.field_lineage = FieldLineage()  # Track field origins and transformations
        
    def load_and_transform(self, yaml_path: str) -> DependencyGraph:
        """Load YAML and apply transformations"""
        
        # Load YAML specification
        with open(yaml_path, 'r') as f:
            self.yaml_spec = yaml.safe_load(f)
        
        # Build original graph
        parser = CalculationViewParser()
        builder = DependencyGraphBuilder()
        base_template = self.yaml_spec['BASE_TEMPLATE']
        parsed_cv = parser.parse_file(base_template)
        self.original_graph = builder.build_from_parsed_cv(parsed_cv)
        
        # Create working copy
        self.transformed_graph = self._deep_copy_graph(self.original_graph)
        
        # Apply transformations in sequence
        self._apply_transformations()
        
        return self.transformed_graph
    
    def _deep_copy_graph(self, original: DependencyGraph) -> DependencyGraph:
        """Create deep copy of graph"""
        new_graph = DependencyGraph()
        new_graph.input_parameters = copy.deepcopy(original.input_parameters)
        
        # Copy nodes
        for node_id, node in original.nodes.items():
            new_node = GraphNode(
                id=node.id,
                node_type=node.node_type,
                fields=node.fields.copy(),
                metadata=copy.deepcopy(node.metadata),
                dependencies=node.dependencies.copy(),
                dependents=node.dependents.copy(),
                calculated_columns=copy.deepcopy(node.calculated_columns),
                filter_expressions=copy.deepcopy(node.filter_expressions)
            )
            new_graph.add_node(new_node)
        
        # Copy edges
        for edge in original.edges:
            new_edge = GraphEdge(
                source_node=edge.source_node,
                target_node=edge.target_node,
                field_mappings=edge.field_mappings.copy(),
                edge_type=edge.edge_type
            )
            new_graph.add_edge(new_edge)
        
        return new_graph
    
    def _apply_transformations(self):
        """Apply all transformations in correct order"""
        print("Applying transformations...")
        
        # Phase 1: Structural changes
        self._delete_nodes()
        self._add_datasources()
        self._add_joins()
        self._rebuild_aggregations()
        
        # Phase 2: Reference updates
        self._update_node_references()
        
        # Phase 3: Field lineage and propagation
        self._build_field_lineage()
        self._extract_transformation_mappings()
        self._propagate_field_transformations_systematically()
        
        # Phase 4: Metadata updates
        self._update_existing_nodes()
        self._update_input_parameters()
        
        print("Transformations completed.")
    
    def _normalize_node_id(self, node_id: str) -> str:
        """Normalize node ID to handle # prefixes consistently"""
        if node_id.startswith('#'):
            return node_id
        return f"#{node_id}"
    
    def _find_node_id(self, search_id: str) -> Optional[str]:
        """Find actual node ID in graph (handles # prefix variations)"""
        candidates = [search_id, f"#{search_id}", search_id.lstrip('#')]
        for candidate in candidates:
            if candidate in self.transformed_graph.nodes:
                return candidate
        return None
    
    def _delete_nodes(self):
        """Delete specified nodes and clean up references"""
        if 'DELETE_NODES' not in self.yaml_spec:
            return
            
        for node_id in self.yaml_spec['DELETE_NODES']:
            actual_id = self._find_node_id(node_id)
            if not actual_id:
                print(f"Warning: Node {node_id} not found for deletion")
                continue
            
            # Remove edges
            self.transformed_graph.edges = [
                edge for edge in self.transformed_graph.edges 
                if edge.source_node != actual_id and edge.target_node != actual_id
            ]
            
            # Remove node
            del self.transformed_graph.nodes[actual_id]
            
            # Clean up adjacency lists
            self._clean_adjacency_lists(actual_id)
            
            print(f"Deleted node: {actual_id}")
    
    def _add_datasources(self):
        """Add new datasource nodes"""
        if 'ADD_NODES' not in self.yaml_spec:
            return
            
        for node_spec in self.yaml_spec['ADD_NODES']:
            if node_spec['type'] != 'datasource':
                continue
                
            node_id = node_spec['node_id']
            fields = set(node_spec.get('field_sources', {}).keys())
            
            new_node = GraphNode(
                id=node_id,
                node_type='datasource',
                fields=fields,
                metadata={
                    'schema_name': node_spec.get('schema_name', ''),
                    'table_name': node_spec.get('table_name', ''),
                    'description': node_spec.get('description', ''),
                    'type': 'datasource'
                }
            )
            
            self.transformed_graph.add_node(new_node)
            print(f"Added datasource: {node_id}")
    
    def _add_joins(self):
        """Add join nodes and edges"""
        if 'ADD_JOINS' not in self.yaml_spec:
            return
            
        for join_spec in self.yaml_spec['ADD_JOINS']:
            join_id = join_spec['join_id']
            left_node = self._find_node_id(join_spec['left_node'])
            right_node = self._find_node_id(join_spec['right_node'])
            
            if not left_node or not right_node:
                print(f"Warning: Could not find nodes for join {join_id}")
                continue
            
            # Create join node
            join_node_id = self._normalize_node_id(join_id)
            join_node = GraphNode(
                id=join_node_id,
                node_type='join',
                fields=set(),  # Will be populated by edges
                metadata={
                    'description': f"{join_spec['left_node']} {join_spec['type']} {join_spec['right_node']}",
                    'join_type': join_spec['type'],
                    'left_node': join_spec['left_node'],
                    'right_node': join_spec['right_node']
                }
            )
            
            self.transformed_graph.add_node(join_node)
            
            # Create edges
            join_conditions = join_spec.get('join_conditions', [])
            left_mappings, right_mappings = self._parse_join_conditions(join_conditions)
            
            left_edge = GraphEdge(
                source_node=left_node,
                target_node=join_node_id,
                field_mappings=left_mappings,
                edge_type="join"
            )
            
            right_edge = GraphEdge(
                source_node=right_node,
                target_node=join_node_id,
                field_mappings=right_mappings,
                edge_type="join"
            )
            
            self.transformed_graph.add_edge(left_edge)
            self.transformed_graph.add_edge(right_edge)
            
            print(f"Added join: {join_id}")
    
    def _parse_join_conditions(self, conditions: List[Dict]) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Parse join conditions into field mappings"""
        left_mappings = {}
        right_mappings = {}
        
        for condition in conditions:
            field_mapping = condition.get('field_mapping', '')
            if '=' in field_mapping:
                left_expr, right_expr = field_mapping.split('=')
                left_field = left_expr.strip().split('.')[-1]
                right_field = right_expr.strip().split('.')[-1]
                
                # Map both sides
                left_mappings[left_field] = left_field
                right_mappings[right_field] = left_field  # Join key consistency
        
        return left_mappings, right_mappings
    
    def _rebuild_aggregations(self):
        """Rebuild aggregation nodes with new dependencies"""
        if 'REBUILD_NODES' not in self.yaml_spec:
            return
            
        for rebuild_spec in self.yaml_spec['REBUILD_NODES']:
            original_node_id = rebuild_spec['original_node']
            new_node_id = rebuild_spec['new_node']
            
            # Track the mapping for reference updates
            self.node_id_mappings[self._normalize_node_id(original_node_id)] = self._normalize_node_id(new_node_id)
            
            # Create new aggregation node
            input_mappings = rebuild_spec.get('input_mappings', {})
            fields = set()
            dependencies = []
            
            # Process input mappings to determine fields and dependencies
            for source_node, field_mappings in input_mappings.items():
                source_node_id = self._find_node_id(source_node)
                if source_node_id:
                    dependencies.append(source_node_id)
                    # Extract actual field names from table.field mappings
                    for target_field, source_field in field_mappings.items():
                        if '.' in source_field:
                            # Extract the actual field name after the dot (e.g., "ACDOCA.RACCT" -> "RACCT")
                            actual_field = source_field.split('.')[-1]
                            fields.add(actual_field)
                        else:
                            # If no table prefix, use the source field as-is
                            fields.add(source_field)
            
            new_node = GraphNode(
                id=self._normalize_node_id(new_node_id),
                node_type=rebuild_spec.get('type', 'aggregation'),
                fields=fields,
                metadata={
                    'description': rebuild_spec.get('description', ''),
                    'type': rebuild_spec.get('type', 'aggregation')
                },
                dependencies=dependencies
            )
            
            self.transformed_graph.add_node(new_node)
            
            # Create input edges based on input_mappings
            self._create_input_edges(new_node.id, input_mappings)
            
            # Delete the original node
            original_node_actual_id = self._find_node_id(original_node_id)
            if original_node_actual_id and original_node_actual_id in self.transformed_graph.nodes:
                # Remove edges involving the original node (except those already updated)
                self.transformed_graph.edges = [
                    edge for edge in self.transformed_graph.edges 
                    if edge.source_node != original_node_actual_id and edge.target_node != original_node_actual_id
                ]
                # Remove the node
                del self.transformed_graph.nodes[original_node_actual_id]
                self._clean_adjacency_lists(original_node_actual_id)
                print(f"Deleted original node: {original_node_actual_id}")
            
            print(f"Rebuilt node: {original_node_id} -> {new_node_id}")
    
    def _create_input_edges(self, target_node_id: str, input_mappings: Dict[str, Dict[str, str]]):
        """Create input edges for rebuilt nodes"""
        for source_node, field_mappings in input_mappings.items():
            source_node_id = self._find_node_id(source_node)
            if source_node_id:
                edge = GraphEdge(
                    source_node=source_node_id,
                    target_node=target_node_id,
                    field_mappings=field_mappings,
                    edge_type="input"
                )
                self.transformed_graph.add_edge(edge)
    
    def _update_node_references(self):
        """Update all references to renamed nodes"""
        if not self.node_id_mappings:
            return
            
        print("Updating node references...")
        
        # Update dependencies and dependents in all nodes
        for node in self.transformed_graph.nodes.values():
            # Update dependencies
            updated_deps = []
            for dep in node.dependencies:
                if dep in self.node_id_mappings:
                    updated_deps.append(self.node_id_mappings[dep])
                    print(f"Updated dependency: {dep} -> {self.node_id_mappings[dep]} in {node.id}")
                else:
                    updated_deps.append(dep)
            node.dependencies = updated_deps
            
            # Update dependents
            updated_dependents = []
            for dependent in node.dependents:
                if dependent in self.node_id_mappings:
                    updated_dependents.append(self.node_id_mappings[dependent])
                else:
                    updated_dependents.append(dependent)
            node.dependents = updated_dependents
        
        # Update edges
        for edge in self.transformed_graph.edges:
            if edge.source_node in self.node_id_mappings:
                edge.source_node = self.node_id_mappings[edge.source_node]
                print(f"Updated edge source: {edge.source_node}")
            
            if edge.target_node in self.node_id_mappings:
                edge.target_node = self.node_id_mappings[edge.target_node]
                print(f"Updated edge target: {edge.target_node}")
    
    def _update_existing_nodes(self):
        """Update existing nodes with new fields/metadata"""
        if 'UPDATE_NODES' not in self.yaml_spec:
            return
            
        for update_spec in self.yaml_spec['UPDATE_NODES']:
            node_id = update_spec['node_id']
            actual_id = self._find_node_id(node_id)
            
            if not actual_id:
                print(f"Warning: Node {node_id} not found for updating")
                continue
                
            node = self.transformed_graph.nodes[actual_id]
            
            # Add new fields
            if 'add_field_mappings' in update_spec:
                for field_name in update_spec['add_field_mappings'].keys():
                    node.fields.add(field_name)
                    
            print(f"Updated node: {node_id}")
    
    def _update_input_parameters(self):
        """Update input parameters"""
        if 'INPUT_PARAMETERS' not in self.yaml_spec:
            return
            
        self.transformed_graph.input_parameters = []
        
        for param_spec in self.yaml_spec['INPUT_PARAMETERS']:
            input_param = InputParameter(
                id=param_spec['parameter_id'],
                parameter=param_spec['parameter_name'],
                description=param_spec['description'],
                datatype=param_spec['datatype'],
                length=param_spec['length'],
                mandatory=str(param_spec['mandatory']).lower(),
                value_domain_type=param_spec['value_domain_type'],
                selection_multiline=param_spec.get('selection_multiline', ''),
                selection_type=param_spec['selection_type']
            )
            self.transformed_graph.input_parameters.append(input_param)
    
    def _clean_adjacency_lists(self, node_id: str):
        """Clean up adjacency lists after node deletion"""
        if hasattr(self.transformed_graph, '_adjacency_list'):
            if node_id in self.transformed_graph._adjacency_list:
                del self.transformed_graph._adjacency_list[node_id]
        
        if hasattr(self.transformed_graph, '_reverse_adjacency_list'):
            if node_id in self.transformed_graph._reverse_adjacency_list:
                del self.transformed_graph._reverse_adjacency_list[node_id]
        
        # Remove from other nodes' dependency lists
        for node in self.transformed_graph.nodes.values():
            if node_id in node.dependencies:
                node.dependencies.remove(node_id)
            if node_id in node.dependents:
                node.dependents.remove(node_id)
    
    def _build_field_lineage(self):
        """Build field lineage map by traversing edges"""
        print("Building field lineage map...")
        
        for edge in self.transformed_graph.edges:
            target_node = edge.target_node
            source_node = edge.source_node
            
            # Track field origins from edge mappings
            for source_field, target_field in edge.field_mappings.items():
                self.field_lineage.track_field_origin(target_node, target_field, source_node)
        
        print("Field lineage map built")
    
    def _extract_transformation_mappings(self):
        """Extract field transformations from YAML and mark transformed sources"""
        print("Extracting transformation mappings...")
        
        # Extract from ADD_NODES field_sources - these define the canonical transformations
        if 'ADD_NODES' in self.yaml_spec:
            for node_spec in self.yaml_spec['ADD_NODES']:
                if node_spec['type'] == 'datasource':
                    node_id = node_spec['node_id']
                    field_sources = node_spec.get('field_sources', {})
                    transformations = {}
                    
                    for target_field, source_field in field_sources.items():
                        if '.' in source_field:
                            # Parse "BSEG.HKONT" patterns, ignore multi-source "|" for now
                            source_parts = source_field.split('|')[0]
                            original_field = source_parts.split('.')[-1]
                            
                            # If target field is different, it's a transformation
                            if target_field != original_field:
                                transformations[original_field] = target_field
                                print(f"Field transformation for {node_id}: {original_field} -> {target_field}")
                    
                    if transformations:
                        self.field_lineage.add_transformed_source(node_id, transformations)
        
        # Mark rebuilt nodes as transformed sources too
        if 'REBUILD_NODES' in self.yaml_spec:
            for rebuild_spec in self.yaml_spec['REBUILD_NODES']:
                new_node_id = self._normalize_node_id(rebuild_spec['new_node'])
                
                # Get transformations from input mappings
                input_mappings = rebuild_spec.get('input_mappings', {})
                transformations = {}
                
                for source_node, field_mappings in input_mappings.items():
                    for target_field, source_field in field_mappings.items():
                        if '.' in source_field:
                            original_field = source_field.split('.')[-1]
                            if target_field != original_field:
                                transformations[original_field] = target_field
                
                if transformations:
                    self.field_lineage.add_transformed_source(new_node_id, transformations)
                    print(f"Marked {new_node_id} as transformed source with {len(transformations)} mappings")
    
    def _propagate_field_transformations_systematically(self):
        """Systematically propagate field transformations using topological order"""
        if not self.field_lineage.transformed_sources:
            print("No transformed sources to propagate")
            return
        
        print(f"Systematically propagating transformations from {len(self.field_lineage.transformed_sources)} sources...")
        
        # Get nodes in topological order
        topo_order = self.transformed_graph.topological_sort()
        affected_nodes = self._identify_transformation_scope()
        
        print(f"Found {len(affected_nodes)} affected nodes")
        
        # Process nodes in dependency order
        for node_id in topo_order:
            if node_id in affected_nodes:
                self._update_node_fields_with_lineage(node_id)
                self._update_outgoing_edges_with_lineage(node_id)
        
        print("Systematic field transformation propagation completed")
    
    def _identify_transformation_scope(self) -> Set[str]:
        """Find all nodes affected by field transformations"""
        affected_nodes = set(self.field_lineage.transformed_sources)
        
        # Find all downstream dependencies using BFS
        to_visit = list(affected_nodes)
        visited = set()
        
        while to_visit:
            current = to_visit.pop(0)
            if current in visited:
                continue
            visited.add(current)
            
            # Add dependents
            if current in self.transformed_graph.nodes:
                for dependent in self.transformed_graph.nodes[current].dependents:
                    if dependent not in affected_nodes:
                        affected_nodes.add(dependent)
                        to_visit.append(dependent)
        
        return affected_nodes
    
    def _update_node_fields_with_lineage(self, node_id: str):
        """Update node fields based on their origins and transformations"""
        if node_id not in self.transformed_graph.nodes:
            return
            
        node = self.transformed_graph.nodes[node_id]
        new_fields = set()
        
        for field in node.fields:
            # Check if this field originated from a transformed source
            origin = self.field_lineage.get_field_origin(node_id, field)
            
            if origin and origin in self.field_lineage.transformed_sources:
                # Apply transformation
                transformed_field = self.field_lineage.get_field_transformation(origin, field)
                new_fields.add(transformed_field)
                print(f"  Transformed field in {node_id}: {field} -> {transformed_field} (from {origin})")
            else:
                # Keep original
                new_fields.add(field)
        
        node.fields = new_fields
    
    def _update_outgoing_edges_with_lineage(self, node_id: str):
        """Update edges originating from this node to use transformed field names"""
        for edge in self.transformed_graph.edges:
            if edge.source_node == node_id:
                new_mappings = {}
                
                for source_field, target_field in edge.field_mappings.items():
                    # If this is a transformed source, use transformed field names
                    if node_id in self.field_lineage.transformed_sources:
                        transformed_source = self.field_lineage.get_field_transformation(node_id, source_field)
                        new_mappings[transformed_source] = transformed_source  # Propagate transformed names
                        print(f"  Updated edge {node_id} -> {edge.target_node}: {source_field} -> {transformed_source}")
                    else:
                        new_mappings[source_field] = target_field
                
                edge.field_mappings = new_mappings

    def print_transformation_summary(self):
        """Print transformation summary"""
        print("\n=== TRANSFORMATION SUMMARY ===")
        print(f"Original nodes: {len(self.original_graph.nodes)}")
        print(f"Transformed nodes: {len(self.transformed_graph.nodes)}")
        print(f"Original edges: {len(self.original_graph.edges)}")
        print(f"Transformed edges: {len(self.transformed_graph.edges)}")
        print(f"Input parameters: {len(self.transformed_graph.input_parameters)}")
        
        if self.node_id_mappings:
            print(f"\nNode mappings applied: {len(self.node_id_mappings)}")
            for old_id, new_id in self.node_id_mappings.items():
                print(f"  {old_id} -> {new_id}")


def main():
    """Test the graph transformer"""
    import sys
    if len(sys.argv) != 2:
        print("Usage: python graph_transformer.py <yaml_specification_file>")
        sys.exit(1)
    
    transformer = GraphTransformer()
    transformed_graph = transformer.load_and_transform(sys.argv[1])
    
    transformer.print_transformation_summary()
    
    print("\n=== TRANSFORMED DEPENDENCY GRAPH ===")
    transformed_graph.print_graph()


if __name__ == '__main__':
    main()