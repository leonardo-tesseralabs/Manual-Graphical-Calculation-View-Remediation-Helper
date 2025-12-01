#!/usr/bin/env python3
"""
Calculation View Dependency Graph Builder

Constructs an in-memory dependency graph from parsed XML calculation view data.
Tracks nodes (data sources, calculation views) and their dependencies.
"""

from typing import Dict, List, Set, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
import json
from cv_parser import ParsedCalculationView, CalculationView, DataSource


@dataclass
class CalculatedColumn:
    """Represents a calculated column in a calculation view"""
    id: str
    datatype: str
    length: str
    expression_language: str
    formula: str


@dataclass
class FilterExpression:
    """Represents a filter expression in a calculation view"""
    expression: str
    language: str = "SQL"


@dataclass
class InputParameter:
    """Represents an input parameter/variable in the calculation view"""
    id: str
    parameter: str
    description: str
    datatype: str
    length: str
    mandatory: str
    value_domain_type: str
    selection_multiline: str
    selection_type: str


@dataclass
class GraphNode:
    """Represents a node in the dependency graph"""
    id: str
    node_type: str  # 'datasource', 'projection', 'join', 'aggregation', etc.
    fields: Set[str] = field(default_factory=set)  # Available fields from this node
    metadata: Dict[str, Any] = field(default_factory=dict)  # Additional node data
    dependencies: List[str] = field(default_factory=list)  # Node IDs this depends on
    dependents: List[str] = field(default_factory=list)  # Node IDs that depend on this
    # Node-level elements
    calculated_columns: List[CalculatedColumn] = field(default_factory=list)  # Calculated columns for this node
    filter_expressions: List[FilterExpression] = field(default_factory=list)  # Filter expressions for this node


@dataclass 
class GraphEdge:
    """Represents a dependency relationship between nodes"""
    source_node: str
    target_node: str
    field_mappings: Dict[str, str] = field(default_factory=dict)  # source_field -> target_field
    edge_type: str = "input"  # 'input', 'join', 'lookup', etc.


class DependencyGraph:
    """In-memory dependency graph for calculation view"""
    
    def __init__(self):
        self.nodes: Dict[str, GraphNode] = {}
        self.edges: List[GraphEdge] = []
        self._adjacency_list: Dict[str, List[str]] = defaultdict(list)
        self._reverse_adjacency_list: Dict[str, List[str]] = defaultdict(list)
        # View-level elements
        self.input_parameters: List[InputParameter] = []  # Global input parameters/variables
    
    def add_node(self, node: GraphNode):
        """Add a node to the graph"""
        self.nodes[node.id] = node
    
    def add_edge(self, edge: GraphEdge):
        """Add an edge to the graph"""
        self.edges.append(edge)
        self._adjacency_list[edge.source_node].append(edge.target_node)
        self._reverse_adjacency_list[edge.target_node].append(edge.source_node)
        
        # Update node dependency lists
        if edge.source_node in self.nodes and edge.target_node not in self.nodes[edge.source_node].dependents:
            self.nodes[edge.source_node].dependents.append(edge.target_node)
        if edge.target_node in self.nodes and edge.source_node not in self.nodes[edge.target_node].dependencies:
            self.nodes[edge.target_node].dependencies.append(edge.source_node)
    
    def get_dependencies(self, node_id: str) -> List[str]:
        """Get direct dependencies of a node"""
        return self._reverse_adjacency_list.get(node_id, [])
    
    def get_dependents(self, node_id: str) -> List[str]:
        """Get direct dependents of a node"""
        return self._adjacency_list.get(node_id, [])
    
    def topological_sort(self) -> List[str]:
        """Return nodes in topological order (dependencies before dependents)"""
        in_degree = defaultdict(int)
        
        # Calculate in-degree for each node
        for node_id in self.nodes:
            in_degree[node_id] = len(self._reverse_adjacency_list[node_id])
        
        queue = deque([node_id for node_id in self.nodes if in_degree[node_id] == 0])
        result = []
        
        while queue:
            current = queue.popleft()
            result.append(current)
            
            for dependent in self._adjacency_list[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        return result
    
    def print_graph(self):
        """Print the dependency graph structure"""
        print("=== CALCULATION VIEW DEPENDENCY GRAPH ===\n")
        
        # Print view-level input parameters
        if self.input_parameters:
            print("INPUT PARAMETERS (View-Level):")
            print("-" * 50)
            for param in self.input_parameters:
                print(f"Parameter: {param.id}")
                print(f"  Type: {param.datatype} ({param.length})")
                print(f"  Description: {param.description}")
                print(f"  Mandatory: {param.mandatory}")
                if param.value_domain_type:
                    print(f"  Value Domain: {param.value_domain_type}")
                if param.selection_type:
                    print(f"  Selection: {param.selection_type}")
                print()
        
        print("NODES:")
        print("-" * 50)
        for node_id, node in self.nodes.items():
            print(f"[{node.node_type.upper()}] {node_id}")
            if node.fields:
                print(f"  Fields: {sorted(node.fields)}")
            if node.dependencies:
                print(f"  Dependencies: {node.dependencies}")
            if node.dependents:
                print(f"  Dependents: {node.dependents}")
            if node.calculated_columns:
                print(f"  Calculated Columns:")
                for calc_col in node.calculated_columns:
                    print(f"    {calc_col.id}: {calc_col.formula} ({calc_col.datatype})")
            if node.filter_expressions:
                print(f"  Filter Expressions:")
                for filter_expr in node.filter_expressions:
                    print(f"    {filter_expr.expression}")
            if node.metadata:
                print(f"  Metadata: {node.metadata}")
            print()
        
        print("\nEDGES:")
        print("-" * 50)
        for edge in self.edges:
            print(f"{edge.source_node} --[{edge.edge_type}]--> {edge.target_node}")
            if edge.field_mappings:
                for src_field, tgt_field in edge.field_mappings.items():
                    print(f"    {src_field} -> {tgt_field}")
            print()
        
        print("\nTOPOLOGICAL ORDER:")
        print("-" * 50)
        topo_order = self.topological_sort()
        for i, node_id in enumerate(topo_order):
            node_type = self.nodes[node_id].node_type
            print(f"{i+1}. [{node_type.upper()}] {node_id}")


class DependencyGraphBuilder:
    """Builds dependency graph from parsed calculation view data"""
    
    def __init__(self):
        self.graph = DependencyGraph()
    
    def build_from_parsed_cv(self, parsed_cv: ParsedCalculationView) -> DependencyGraph:
        """Build dependency graph from parsed calculation view"""
        
        # 1. Extract view-level input parameters
        self._extract_input_parameters(parsed_cv.variables)
        
        # 2. Add data source nodes
        self._add_data_source_nodes(parsed_cv.data_sources)
        
        # 3. Add calculation view nodes with calculated columns and filters
        self._add_calculation_view_nodes(parsed_cv.calculation_views)
        
        # 4. Add edges for dependencies
        self._add_calculation_view_dependencies(parsed_cv.calculation_views)
        
        # 5. Infer field propagation through the graph
        self._propagate_fields()
        
        return self.graph
    
    def _extract_input_parameters(self, variables):
        """Extract view-level input parameters from parsed variables"""
        for var in variables:
            input_param = InputParameter(
                id=var.id,
                parameter=var.parameter,
                description=var.description,
                datatype=var.datatype,
                length=var.length,
                mandatory=var.mandatory,
                value_domain_type=var.value_domain_type,
                selection_multiline=var.selection_multiline,
                selection_type=var.selection_type
            )
            self.graph.input_parameters.append(input_param)
    
    def _add_data_source_nodes(self, data_sources: List[DataSource]):
        """Add data source nodes to graph"""
        for ds in data_sources:
            # Extract fields - for now we'll need to infer or get from schema
            fields = set()  # TODO: Get actual fields from schema/metadata
            
            node = GraphNode(
                id=ds.id,
                node_type="datasource", 
                fields=fields,
                metadata={
                    "schema_name": ds.schema_name,
                    "table_name": ds.column_object_name,
                    "type": ds.type
                }
            )
            self.graph.add_node(node)
    
    def _add_calculation_view_nodes(self, calc_views: List[CalculationView]):
        """Add calculation view nodes to graph"""
        for cv in calc_views:
            # Extract output fields from view attributes
            fields = {attr.id for attr in cv.view_attributes}
            fields.update({attr.id for attr in cv.calculated_view_attributes})
            
            # Extract calculated columns
            calculated_columns = []
            for calc_attr in cv.calculated_view_attributes:
                calc_col = CalculatedColumn(
                    id=calc_attr.id,
                    datatype=calc_attr.datatype,
                    length=calc_attr.length,
                    expression_language=calc_attr.expression_language,
                    formula=calc_attr.formula
                )
                calculated_columns.append(calc_col)
            
            # Extract filter expressions
            filter_expressions = []
            if cv.filter_expression:
                filter_expr = FilterExpression(
                    expression=cv.filter_expression,
                    language=cv.filter_expression_language or "SQL"
                )
                filter_expressions.append(filter_expr)
            
            node = GraphNode(
                id=cv.id,
                node_type=cv.type.lower() if cv.type else "calculationview",
                fields=fields,
                calculated_columns=calculated_columns,
                filter_expressions=filter_expressions,
                metadata={
                    "description": cv.descriptions,
                    "join_type": cv.join_type,
                    "cardinality": cv.cardinality
                }
            )
            self.graph.add_node(node)
    
    def _add_calculation_view_dependencies(self, calc_views: List[CalculationView]):
        """Add edges for calculation view dependencies"""
        for cv in calc_views:
            for input_node in cv.inputs:
                # Create field mappings from input mappings
                field_mappings = {}
                for mapping in input_node.mappings:
                    field_mappings[mapping.source] = mapping.target
                
                edge = GraphEdge(
                    source_node=input_node.node,
                    target_node=cv.id,
                    field_mappings=field_mappings,
                    edge_type="input"
                )
                self.graph.add_edge(edge)
    
    def _propagate_fields(self):
        """Propagate field information through the dependency graph"""
        # Process nodes in topological order to ensure dependencies are processed first
        topo_order = self.graph.topological_sort()
        
        for node_id in topo_order:
            node = self.graph.nodes[node_id]
            
            # For calculation views, merge fields from dependencies
            if node.node_type != "datasource":
                inherited_fields = set()
                
                # Get fields from all dependencies
                for dep_id in node.dependencies:
                    dep_node = self.graph.nodes[dep_id]
                    inherited_fields.update(dep_node.fields)
                
                # Apply field mappings from edges
                for edge in self.graph.edges:
                    if edge.target_node == node_id:
                        # Map source fields to target fields
                        for src_field, tgt_field in edge.field_mappings.items():
                            if src_field in inherited_fields:
                                inherited_fields.remove(src_field)
                                inherited_fields.add(tgt_field)
                
                # Combine with explicitly defined fields
                node.fields.update(inherited_fields)


def main():
    """Test the dependency graph builder"""
    import sys
    if len(sys.argv) != 2:
        print("Usage: python dependency_graph.py <calculation_view_file.xml>")
        sys.exit(1)
    
    from cv_parser import CalculationViewParser
    
    # Parse the calculation view
    parser = CalculationViewParser()
    parsed_cv = parser.parse_file(sys.argv[1])
    
    # Build dependency graph
    builder = DependencyGraphBuilder()
    graph = builder.build_from_parsed_cv(parsed_cv)
    
    # Print the graph
    graph.print_graph()


if __name__ == '__main__':
    main()