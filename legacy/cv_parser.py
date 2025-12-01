#!/usr/bin/env python3
"""
Complete Calculation View Parser

This script parses a SAP HANA calculation view XML file and extracts ALL structural
information needed to reconstruct it programmatically with 100% fidelity.
"""

import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import json

@dataclass
class Variable:
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
class DataSource:
    id: str
    type: str
    schema_name: str
    column_object_name: str
    view_attributes_all: bool = True

@dataclass
class ViewAttribute:
    id: str
    aggregation_type: Optional[str] = None
    order: Optional[str] = None
    semantic_type: Optional[str] = None
    attribute_hierarchy_active: Optional[str] = None
    display_attribute: Optional[str] = None
    measure_type: Optional[str] = None
    filter_config: Optional[Dict[str, Any]] = None

@dataclass
class Mapping:
    type: str
    target: str
    source: str

@dataclass
class Input:
    node: str
    mappings: List[Mapping] = field(default_factory=list)

@dataclass
class CalculatedViewAttribute:
    id: str
    datatype: str
    length: str
    expression_language: str
    formula: str

@dataclass
class CalculationView:
    id: str
    type: str
    descriptions: str = ""
    view_attributes: List[ViewAttribute] = field(default_factory=list)
    calculated_view_attributes: List[CalculatedViewAttribute] = field(default_factory=list)
    inputs: List[Input] = field(default_factory=list)
    join_attributes: List[str] = field(default_factory=list)
    filter_expression: Optional[str] = None
    # Additional attributes for joins/projections
    cardinality: Optional[str] = None
    join_order: Optional[str] = None
    join_type: Optional[str] = None
    dynamic: Optional[str] = None
    filter_expression_language: Optional[str] = None

@dataclass
class LogicalAttribute:
    id: str
    order: str
    semantic_type: str
    attribute_hierarchy_active: str
    display_attribute: str
    description: str
    column_object_name: str
    column_name: str

@dataclass
class LogicalMeasure:
    id: str
    order: str
    aggregation_type: str
    measure_type: str
    description: str
    column_object_name: str
    column_name: str

@dataclass
class Shape:
    model_object_name: str
    model_object_namespace: str
    expanded: str
    upper_left_x: str
    upper_left_y: str
    rectangle_height: str
    rectangle_width: str

@dataclass
class ParsedCalculationView:
    # Root attributes
    schema_version: str = ""
    id: str = ""
    apply_privilege_type: str = ""
    check_analytic_privileges: str = ""
    default_client: str = ""
    default_language: str = ""
    hierarchies_sql_enabled: str = ""
    translation_relevant: str = ""
    visibility: str = ""
    calculation_scenario_type: str = ""
    data_category: str = ""
    enforce_sql_execution: str = ""
    execution_semantic: str = ""
    output_view_type: str = ""
    
    # Content
    descriptions: str = ""
    activated_at: str = ""
    changed_at: str = ""
    variables: List[Variable] = field(default_factory=list)
    data_sources: List[DataSource] = field(default_factory=list)
    calculation_views: List[CalculationView] = field(default_factory=list)
    logical_model_id: str = ""
    logical_descriptions: str = ""
    logical_attributes: List[LogicalAttribute] = field(default_factory=list)
    logical_measures: List[LogicalMeasure] = field(default_factory=list)
    shapes: List[Shape] = field(default_factory=list)
    
    # Execution hints
    execution_hints_name: str = ""
    execution_hints_value: str = ""
    
    # Layout info
    relative_width_scenario: str = ""

class CalculationViewParser:
    def __init__(self):
        self.namespaces = {
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'AccessControl': 'http://www.sap.com/ndb/SQLCoreModelAccessControl.ecore',
            'Calculation': 'http://www.sap.com/ndb/BiModelCalculation.ecore'
        }

    def parse_file(self, xml_file_path: str) -> ParsedCalculationView:
        """Parse the calculation view XML file completely"""
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            # Parse root attributes
            result = ParsedCalculationView()
            result.schema_version = root.get(f"{{{self.namespaces['xsi']}}}schemaVersion", "")
            result.id = root.get("id", "")
            result.apply_privilege_type = root.get("applyPrivilegeType", "")
            result.check_analytic_privileges = root.get("checkAnalyticPrivileges", "")
            result.default_client = root.get("defaultClient", "")
            result.default_language = root.get("defaultLanguage", "")
            result.hierarchies_sql_enabled = root.get("hierarchiesSQLEnabled", "")
            result.translation_relevant = root.get("translationRelevant", "")
            result.visibility = root.get("visibility", "")
            result.calculation_scenario_type = root.get("calculationScenarioType", "")
            result.data_category = root.get("dataCategory", "")
            result.enforce_sql_execution = root.get("enforceSqlExecution", "")
            result.execution_semantic = root.get("executionSemantic", "")
            result.output_view_type = root.get("outputViewType", "")
            
            # Parse descriptions
            desc_elem = root.find("descriptions")
            if desc_elem is not None:
                result.descriptions = desc_elem.get("defaultDescription", "")
            
            # Parse metadata
            metadata_elem = root.find("metadata")
            if metadata_elem is not None:
                result.activated_at = metadata_elem.get("activatedAt", "")
                result.changed_at = metadata_elem.get("changedAt", "")
            
            # Parse execution hints
            hints_elem = root.find("executionHints")
            if hints_elem is not None:
                result.execution_hints_name = hints_elem.get("name", "")
                result.execution_hints_value = hints_elem.get("value", "")
            
            # Parse information model layout
            layout_elem = root.find("informationModelLayout")
            if layout_elem is not None:
                result.relative_width_scenario = layout_elem.get("relativeWidthScenario", "")
            
            # Parse variables
            result.variables = self._parse_variables(root)
            
            # Parse data sources
            result.data_sources = self._parse_data_sources(root)
            
            # Parse calculation views
            result.calculation_views = self._parse_calculation_views(root)
            
            # Parse logical model
            self._parse_logical_model(root, result)
            
            # Parse layout
            result.shapes = self._parse_layout(root)
            
            return result
            
        except ET.ParseError as e:
            print(f"Error parsing XML file: {e}")
            raise
        except FileNotFoundError:
            print(f"Error: XML file not found: {xml_file_path}")
            raise

    def _parse_variables(self, root: ET.Element) -> List[Variable]:
        """Parse local variables"""
        variables = []
        local_vars = root.find("localVariables")
        if local_vars is not None:
            for var_elem in local_vars.findall("variable"):
                desc_elem = var_elem.find("descriptions")
                props_elem = var_elem.find("variableProperties")
                domain_elem = props_elem.find("valueDomain") if props_elem is not None else None
                sel_elem = props_elem.find("selection") if props_elem is not None else None
                
                var = Variable(
                    id=var_elem.get("id", ""),
                    parameter=var_elem.get("parameter", ""),
                    description=desc_elem.get("defaultDescription", "") if desc_elem is not None else "",
                    datatype=props_elem.get("datatype", "") if props_elem is not None else "",
                    length=props_elem.get("length", "") if props_elem is not None else "",
                    mandatory=props_elem.get("mandatory", "") if props_elem is not None else "",
                    value_domain_type=domain_elem.get("type", "") if domain_elem is not None else "",
                    selection_multiline=sel_elem.get("multiLine", "") if sel_elem is not None else "",
                    selection_type=sel_elem.get("type", "") if sel_elem is not None else ""
                )
                variables.append(var)
        return variables

    def _parse_data_sources(self, root: ET.Element) -> List[DataSource]:
        """Parse data sources"""
        data_sources = []
        ds_container = root.find("dataSources")
        if ds_container is not None:
            for ds_elem in ds_container.findall("DataSource"):
                col_obj = ds_elem.find("columnObject")
                view_attrs = ds_elem.find("viewAttributes")
                
                ds = DataSource(
                    id=ds_elem.get("id", ""),
                    type=ds_elem.get("type", ""),
                    schema_name=col_obj.get("schemaName", "") if col_obj is not None else "",
                    column_object_name=col_obj.get("columnObjectName", "") if col_obj is not None else "",
                    view_attributes_all=view_attrs.get("allViewAttributes", "") == "true" if view_attrs is not None else True
                )
                data_sources.append(ds)
        return data_sources

    def _parse_calculation_views(self, root: ET.Element) -> List[CalculationView]:
        """Parse calculation views"""
        calculation_views = []
        cv_container = root.find("calculationViews")
        if cv_container is not None:
            for cv_elem in cv_container.findall("calculationView"):
                cv = CalculationView(
                    id=cv_elem.get("id", ""),
                    type=cv_elem.get(f"{{{self.namespaces['xsi']}}}type", "").replace("Calculation:", ""),
                    cardinality=cv_elem.get("cardinality"),
                    join_order=cv_elem.get("joinOrder"),
                    join_type=cv_elem.get("joinType"),
                    dynamic=cv_elem.get("dynamic"),
                    filter_expression_language=cv_elem.get("filterExpressionLanguage")
                )
                
                # Parse descriptions
                desc_elem = cv_elem.find("descriptions")
                cv.descriptions = desc_elem.get("defaultDescription", "") if desc_elem is not None else ""
                
                # Parse view attributes
                cv.view_attributes = self._parse_view_attributes(cv_elem)
                
                # Parse calculated view attributes
                cv.calculated_view_attributes = self._parse_calculated_view_attributes(cv_elem)
                
                # Parse inputs
                cv.inputs = self._parse_inputs(cv_elem)
                
                # Parse join attributes
                for ja_elem in cv_elem.findall("joinAttribute"):
                    cv.join_attributes.append(ja_elem.get("name", ""))
                
                # Parse filter
                filter_elem = cv_elem.find("filter")
                if filter_elem is not None:
                    cv.filter_expression = filter_elem.text
                
                calculation_views.append(cv)
        
        return calculation_views

    def _parse_view_attributes(self, cv_elem: ET.Element) -> List[ViewAttribute]:
        """Parse view attributes within a calculation view"""
        view_attrs = []
        va_container = cv_elem.find("viewAttributes")
        if va_container is not None:
            for va_elem in va_container.findall("viewAttribute"):
                # Parse filter if present
                filter_config = None
                filter_elem = va_elem.find("filter")
                if filter_elem is not None:
                    filter_config = {
                        'type': filter_elem.get(f"{{{self.namespaces['xsi']}}}type", ""),
                        'operator': filter_elem.get("operator"),
                        'including': filter_elem.get("including"),
                        'value': filter_elem.get("value")
                    }
                
                va = ViewAttribute(
                    id=va_elem.get("id", ""),
                    aggregation_type=va_elem.get("aggregationType"),  # This should capture aggregationType="sum"
                    order=va_elem.get("order"),
                    semantic_type=va_elem.get("semanticType"),
                    attribute_hierarchy_active=va_elem.get("attributeHierarchyActive"),
                    display_attribute=va_elem.get("displayAttribute"),
                    measure_type=va_elem.get("measureType"),
                    filter_config=filter_config
                )
                view_attrs.append(va)
        return view_attrs

    def _parse_calculated_view_attributes(self, cv_elem: ET.Element) -> List[CalculatedViewAttribute]:
        """Parse calculated view attributes"""
        calc_attrs = []
        cva_container = cv_elem.find("calculatedViewAttributes")
        if cva_container is not None:
            for cva_elem in cva_container.findall("calculatedViewAttribute"):
                formula_elem = cva_elem.find("formula")
                
                cva = CalculatedViewAttribute(
                    id=cva_elem.get("id", ""),
                    datatype=cva_elem.get("datatype", ""),
                    length=cva_elem.get("length", ""),
                    expression_language=cva_elem.get("expressionLanguage", ""),
                    formula=formula_elem.text if formula_elem is not None else ""
                )
                calc_attrs.append(cva)
        return calc_attrs

    def _parse_inputs(self, cv_elem: ET.Element) -> List[Input]:
        """Parse input elements"""
        inputs = []
        for input_elem in cv_elem.findall("input"):
            mappings = []
            for mapping_elem in input_elem.findall("mapping"):
                mapping = Mapping(
                    type=mapping_elem.get(f"{{{self.namespaces['xsi']}}}type", ""),
                    target=mapping_elem.get("target", ""),
                    source=mapping_elem.get("source", "")
                )
                mappings.append(mapping)
            
            input_obj = Input(
                node=input_elem.get("node", ""),
                mappings=mappings
            )
            inputs.append(input_obj)
        return inputs

    def _parse_logical_model(self, root: ET.Element, result: ParsedCalculationView):
        """Parse logical model section"""
        logical_model = root.find("logicalModel")
        if logical_model is not None:
            result.logical_model_id = logical_model.get("id", "")
            
            # Parse logical descriptions
            desc_elem = logical_model.find("descriptions")
            if desc_elem is not None:
                result.logical_descriptions = desc_elem.get("defaultDescription", "")
            
            # Parse attributes
            attrs_container = logical_model.find("attributes")
            if attrs_container is not None:
                for attr_elem in attrs_container.findall("attribute"):
                    desc_elem = attr_elem.find("descriptions")
                    key_mapping = attr_elem.find("keyMapping")
                    
                    attr = LogicalAttribute(
                        id=attr_elem.get("id", ""),
                        order=attr_elem.get("order", ""),
                        semantic_type=attr_elem.get("semanticType", ""),
                        attribute_hierarchy_active=attr_elem.get("attributeHierarchyActive", ""),
                        display_attribute=attr_elem.get("displayAttribute", ""),
                        description=desc_elem.get("defaultDescription", "") if desc_elem is not None else "",
                        column_object_name=key_mapping.get("columnObjectName", "") if key_mapping is not None else "",
                        column_name=key_mapping.get("columnName", "") if key_mapping is not None else ""
                    )
                    result.logical_attributes.append(attr)
            
            # Parse measures
            measures_container = logical_model.find("baseMeasures")
            if measures_container is not None:
                for measure_elem in measures_container.findall("measure"):
                    desc_elem = measure_elem.find("descriptions")
                    measure_mapping = measure_elem.find("measureMapping")
                    
                    measure = LogicalMeasure(
                        id=measure_elem.get("id", ""),
                        order=measure_elem.get("order", ""),
                        aggregation_type=measure_elem.get("aggregationType", ""),
                        measure_type=measure_elem.get("measureType", ""),
                        description=desc_elem.get("defaultDescription", "") if desc_elem is not None else "",
                        column_object_name=measure_mapping.get("columnObjectName", "") if measure_mapping is not None else "",
                        column_name=measure_mapping.get("columnName", "") if measure_mapping is not None else ""
                    )
                    result.logical_measures.append(measure)

    def _parse_layout(self, root: ET.Element) -> List[Shape]:
        """Parse layout shapes"""
        shapes = []
        layout_elem = root.find("layout")
        if layout_elem is not None:
            shapes_container = layout_elem.find("shapes")
            if shapes_container is not None:
                for shape_elem in shapes_container.findall("shape"):
                    corner_elem = shape_elem.find("upperLeftCorner")
                    size_elem = shape_elem.find("rectangleSize")
                    
                    shape = Shape(
                        model_object_name=shape_elem.get("modelObjectName", ""),
                        model_object_namespace=shape_elem.get("modelObjectNameSpace", ""),
                        expanded=shape_elem.get("expanded", ""),
                        upper_left_x=corner_elem.get("x", "") if corner_elem is not None else "",
                        upper_left_y=corner_elem.get("y", "") if corner_elem is not None else "",
                        rectangle_height=size_elem.get("height", "") if size_elem is not None else "",
                        rectangle_width=size_elem.get("width", "") if size_elem is not None else ""
                    )
                    shapes.append(shape)
        return shapes

def main():
    import sys
    if len(sys.argv) != 2:
        print("Usage: python cv_parser.py <calculation_view_file.xml>")
        sys.exit(1)
    
    parser = CalculationViewParser()
    result = parser.parse_file(sys.argv[1])
    
    # Convert to dict for JSON serialization
    import dataclasses
    result_dict = dataclasses.asdict(result)
    
    # Pretty print the parsed structure
    print(json.dumps(result_dict, indent=2, ensure_ascii=False))

if __name__ == '__main__':
    main()