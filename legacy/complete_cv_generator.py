#!/usr/bin/env python3
"""
Complete Calculation View Generator

This script generates a SAP HANA calculation view XML file with 100% fidelity
from a parsed calculation view structure.
"""

import xml.etree.ElementTree as ET
from xml.dom import minidom
import json
import argparse
from typing import Dict, List, Any, Optional

class CompleteCalculationViewGenerator:
    def __init__(self):
        self.namespaces = {
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'AccessControl': 'http://www.sap.com/ndb/SQLCoreModelAccessControl.ecore',
            'Calculation': 'http://www.sap.com/ndb/BiModelCalculation.ecore'
        }
        
        # Register namespaces
        for prefix, uri in self.namespaces.items():
            ET.register_namespace(prefix, uri)

    def generate_from_parsed_data(self, parsed_data: Dict[str, Any]) -> str:
        """Generate calculation view XML from parsed data dictionary"""
        
        # Create root element
        root = ET.Element(f"{{{self.namespaces['Calculation']}}}scenario")
        
        # Set all root attributes
        if parsed_data.get('schema_version'):
            root.set(f"{{{self.namespaces['xsi']}}}schemaVersion", parsed_data['schema_version'])
        else:
            root.set(f"{{{self.namespaces['xsi']}}}schemaVersion", "2.3")
            
        root.set("id", parsed_data['id'])
        root.set("applyPrivilegeType", parsed_data['apply_privilege_type'])
        root.set("checkAnalyticPrivileges", parsed_data['check_analytic_privileges'])
        root.set("defaultClient", parsed_data['default_client'])
        root.set("defaultLanguage", parsed_data['default_language'])
        root.set("hierarchiesSQLEnabled", parsed_data['hierarchies_sql_enabled'])
        root.set("translationRelevant", parsed_data['translation_relevant'])
        root.set("visibility", parsed_data['visibility'])
        root.set("calculationScenarioType", parsed_data['calculation_scenario_type'])
        root.set("dataCategory", parsed_data['data_category'])
        root.set("enforceSqlExecution", parsed_data['enforce_sql_execution'])
        root.set("executionSemantic", parsed_data['execution_semantic'])
        root.set("outputViewType", parsed_data['output_view_type'])
        
        # Add origin (empty)
        ET.SubElement(root, "origin")
        
        # Add descriptions
        descriptions = ET.SubElement(root, "descriptions")
        descriptions.set("defaultDescription", parsed_data['descriptions'])
        
        # Add metadata
        metadata = ET.SubElement(root, "metadata")
        metadata.set("activatedAt", parsed_data['activated_at'])
        metadata.set("changedAt", parsed_data['changed_at'])
        
        # Add local variables
        self._add_variables(root, parsed_data['variables'])
        
        # Add variable mappings (empty)
        ET.SubElement(root, "variableMappings")
        
        # Add information model layout
        layout = ET.SubElement(root, "informationModelLayout")
        layout.set("relativeWidthScenario", parsed_data['relative_width_scenario'])
        
        # Add execution hints
        if parsed_data['execution_hints_name'] or parsed_data['execution_hints_value']:
            hints = ET.SubElement(root, "executionHints")
            hints.set("name", parsed_data['execution_hints_name'])
            hints.set("value", parsed_data['execution_hints_value'])
        
        # Add data sources
        self._add_data_sources(root, parsed_data['data_sources'])
        
        # Add calculation views
        self._add_calculation_views(root, parsed_data['calculation_views'])
        
        # Add logical model
        self._add_logical_model(root, parsed_data)
        
        # Add layout
        self._add_layout(root, parsed_data['shapes'])
        
        return self._prettify_xml(root)

    def _add_variables(self, parent: ET.Element, variables: List[Dict[str, Any]]):
        """Add local variables"""
        if not variables:
            return
            
        local_vars = ET.SubElement(parent, "localVariables")
        for var in variables:
            variable = ET.SubElement(local_vars, "variable")
            variable.set("id", var['id'])
            if var['parameter']:
                variable.set("parameter", var['parameter'])
            
            # Add descriptions
            desc = ET.SubElement(variable, "descriptions")
            desc.set("defaultDescription", var['description'])
            
            # Add variable properties
            props = ET.SubElement(variable, "variableProperties")
            props.set("datatype", var['datatype'])
            props.set("length", var['length'])
            props.set("mandatory", var['mandatory'])
            
            # Add value domain
            domain = ET.SubElement(props, "valueDomain")
            domain.set("type", var['value_domain_type'])
            
            # Add selection
            selection = ET.SubElement(props, "selection")
            selection.set("multiLine", var['selection_multiline'])
            selection.set("type", var['selection_type'])

    def _add_data_sources(self, parent: ET.Element, data_sources: List[Dict[str, Any]]):
        """Add data sources"""
        if not data_sources:
            return
            
        ds_container = ET.SubElement(parent, "dataSources")
        for ds in data_sources:
            ds_elem = ET.SubElement(ds_container, "DataSource")
            ds_elem.set("id", ds['id'])
            ds_elem.set("type", ds['type'])
            
            # Add view attributes
            attrs = ET.SubElement(ds_elem, "viewAttributes")
            if ds.get('view_attributes_all', True):
                attrs.set("allViewAttributes", "true")
            
            # Add column object
            col_obj = ET.SubElement(ds_elem, "columnObject")
            col_obj.set("schemaName", ds['schema_name'])
            col_obj.set("columnObjectName", ds['column_object_name'])

    def _add_calculation_views(self, parent: ET.Element, calculation_views: List[Dict[str, Any]]):
        """Add calculation views"""
        if not calculation_views:
            return
            
        cv_container = ET.SubElement(parent, "calculationViews")
        for cv in calculation_views:
            cv_elem = ET.SubElement(cv_container, "calculationView")
            cv_elem.set(f"{{{self.namespaces['xsi']}}}type", f"Calculation:{cv['type']}")
            cv_elem.set("id", cv['id'])
            
            # Add optional attributes (map from parsed names to XML attribute names)
            cv_attr_mappings = {
                'cardinality': 'cardinality',
                'join_order': 'joinOrder',
                'join_type': 'joinType', 
                'dynamic': 'dynamic',
                'filter_expression_language': 'filterExpressionLanguage'
            }
            
            for parsed_attr, xml_attr in cv_attr_mappings.items():
                if cv.get(parsed_attr):
                    cv_elem.set(xml_attr, cv[parsed_attr])
            
            # Add descriptions
            desc = ET.SubElement(cv_elem, "descriptions")
            if cv.get('descriptions'):
                desc.set("defaultDescription", cv['descriptions'])
            
            # Add view attributes
            self._add_view_attributes(cv_elem, cv.get('view_attributes', []))
            
            # Add calculated view attributes
            self._add_calculated_view_attributes(cv_elem, cv.get('calculated_view_attributes', []))
            
            # Add inputs
            self._add_inputs(cv_elem, cv.get('inputs', []))
            
            # Add join attributes
            for join_attr in cv.get('join_attributes', []):
                ja_elem = ET.SubElement(cv_elem, "joinAttribute")
                ja_elem.set("name", join_attr)
            
            # Add filter
            if cv.get('filter_expression'):
                filter_elem = ET.SubElement(cv_elem, "filter")
                filter_elem.text = cv['filter_expression']

    def _add_view_attributes(self, parent: ET.Element, view_attributes: List[Dict[str, Any]]):
        """Add view attributes"""
        if not view_attributes:
            return
            
        va_container = ET.SubElement(parent, "viewAttributes")
        for va in view_attributes:
            va_elem = ET.SubElement(va_container, "viewAttribute")
            va_elem.set("id", va['id'])
            
            # Add optional attributes (map from parsed names to XML attribute names)
            attr_mappings = {
                'aggregation_type': 'aggregationType',
                'order': 'order',
                'semantic_type': 'semanticType', 
                'attribute_hierarchy_active': 'attributeHierarchyActive',
                'display_attribute': 'displayAttribute',
                'measure_type': 'measureType'
            }
            
            for parsed_attr, xml_attr in attr_mappings.items():
                if va.get(parsed_attr):
                    va_elem.set(xml_attr, va[parsed_attr])
            
            # Add filter if present
            if va.get('filter_config'):
                filter_elem = ET.SubElement(va_elem, "filter")
                fc = va['filter_config']
                if fc.get('type'):
                    filter_elem.set(f"{{{self.namespaces['xsi']}}}type", fc['type'])
                if fc.get('operator'):
                    filter_elem.set("operator", fc['operator'])
                if fc.get('including'):
                    filter_elem.set("including", fc['including'])
                if fc.get('value'):
                    filter_elem.set("value", fc['value'])

    def _add_calculated_view_attributes(self, parent: ET.Element, calc_attributes: List[Dict[str, Any]]):
        """Add calculated view attributes"""
        if not calc_attributes:
            calc_container = ET.SubElement(parent, "calculatedViewAttributes")
            return
            
        calc_container = ET.SubElement(parent, "calculatedViewAttributes")
        for cva in calc_attributes:
            cva_elem = ET.SubElement(calc_container, "calculatedViewAttribute")
            cva_elem.set("datatype", cva['datatype'])
            cva_elem.set("id", cva['id'])
            cva_elem.set("length", cva['length'])
            cva_elem.set("expressionLanguage", cva['expression_language'])
            
            # Add formula (preserve XML entities like &#xD; for exact fidelity)
            formula_elem = ET.SubElement(cva_elem, "formula")
            formula_elem.text = cva['formula']

    def _add_inputs(self, parent: ET.Element, inputs: List[Dict[str, Any]]):
        """Add inputs"""
        for input_data in inputs:
            input_elem = ET.SubElement(parent, "input")
            input_elem.set("node", input_data['node'])
            
            # Add mappings
            for mapping in input_data.get('mappings', []):
                mapping_elem = ET.SubElement(input_elem, "mapping")
                mapping_elem.set(f"{{{self.namespaces['xsi']}}}type", mapping['type'])
                mapping_elem.set("target", mapping['target'])
                mapping_elem.set("source", mapping['source'])

    def _add_logical_model(self, parent: ET.Element, parsed_data: Dict[str, Any]):
        """Add logical model"""
        logical_model = ET.SubElement(parent, "logicalModel")
        logical_model.set("id", parsed_data['logical_model_id'])
        
        # Add descriptions
        desc = ET.SubElement(logical_model, "descriptions")
        desc.set("defaultDescription", parsed_data['logical_descriptions'])
        
        # Add attributes
        attrs_container = ET.SubElement(logical_model, "attributes")
        for attr in parsed_data.get('logical_attributes', []):
            attr_elem = ET.SubElement(attrs_container, "attribute")
            attr_elem.set("id", attr['id'])
            attr_elem.set("order", attr['order'])
            if attr.get('semantic_type'):
                attr_elem.set("semanticType", attr['semantic_type'])
            attr_elem.set("attributeHierarchyActive", attr['attribute_hierarchy_active'])
            attr_elem.set("displayAttribute", attr['display_attribute'])
            
            # Add descriptions
            desc_elem = ET.SubElement(attr_elem, "descriptions")
            desc_elem.set("defaultDescription", attr['description'])
            
            # Add key mapping
            mapping_elem = ET.SubElement(attr_elem, "keyMapping")
            mapping_elem.set("columnObjectName", attr['column_object_name'])
            mapping_elem.set("columnName", attr['column_name'])
        
        # Add calculated attributes (empty)
        ET.SubElement(logical_model, "calculatedAttributes")
        
        # Add private data foundation
        pdf = ET.SubElement(logical_model, "privateDataFoundation")
        ET.SubElement(pdf, "tableProxies")
        ET.SubElement(pdf, "joins")
        layout = ET.SubElement(pdf, "layout")
        ET.SubElement(layout, "shapes")
        
        # Add base measures
        measures_container = ET.SubElement(logical_model, "baseMeasures")
        for measure in parsed_data.get('logical_measures', []):
            measure_elem = ET.SubElement(measures_container, "measure")
            measure_elem.set("id", measure['id'])
            measure_elem.set("order", measure['order'])
            measure_elem.set("aggregationType", measure['aggregation_type'])
            measure_elem.set("measureType", measure['measure_type'])
            
            # Add descriptions
            desc_elem = ET.SubElement(measure_elem, "descriptions")
            desc_elem.set("defaultDescription", measure['description'])
            
            # Add measure mapping
            mapping_elem = ET.SubElement(measure_elem, "measureMapping")
            mapping_elem.set("columnObjectName", measure['column_object_name'])
            mapping_elem.set("columnName", measure['column_name'])
        
        # Add empty sections
        ET.SubElement(logical_model, "calculatedMeasures")
        ET.SubElement(logical_model, "restrictedMeasures")
        ET.SubElement(logical_model, "localDimensions")

    def _add_layout(self, parent: ET.Element, shapes: List[Dict[str, Any]]):
        """Add layout shapes"""
        layout = ET.SubElement(parent, "layout")
        shapes_container = ET.SubElement(layout, "shapes")
        
        for shape in shapes:
            shape_elem = ET.SubElement(shapes_container, "shape")
            shape_elem.set("expanded", shape['expanded'])
            shape_elem.set("modelObjectName", shape['model_object_name'])
            shape_elem.set("modelObjectNameSpace", shape['model_object_namespace'])
            
            # Add upper left corner
            corner = ET.SubElement(shape_elem, "upperLeftCorner")
            corner.set("x", shape['upper_left_x'])
            corner.set("y", shape['upper_left_y'])
            
            # Add rectangle size
            size = ET.SubElement(shape_elem, "rectangleSize")
            size.set("height", shape['rectangle_height'])
            size.set("width", shape['rectangle_width'])

    def _prettify_xml(self, elem: ET.Element) -> str:
        """Return a pretty-printed XML string"""
        rough_string = ET.tostring(elem, 'unicode')
        reparsed = minidom.parseString(rough_string)
        xml_output = reparsed.toprettyxml(indent="  ")
        
        # Preserve specific XML entities for SAP HANA fidelity
        # Convert carriage returns back to &#xD; entities in formula text
        xml_output = xml_output.replace('\r\n', '&#xD;\n')
        xml_output = xml_output.replace('\r', '&#xD;')
        
        return xml_output

def main():
    parser = argparse.ArgumentParser(description='Generate complete calculation view from parsed JSON')
    parser.add_argument('parsed_json', help='Path to parsed JSON file')
    parser.add_argument('output_xml', help='Path to output XML file')
    parser.add_argument('--view-id', help='Override view ID')
    
    args = parser.parse_args()
    
    # Load parsed data
    with open(args.parsed_json, 'r') as f:
        parsed_data = json.load(f)
    
    # Override view ID if provided
    if args.view_id:
        parsed_data['id'] = args.view_id
    
    # Generate XML
    generator = CompleteCalculationViewGenerator()
    xml_output = generator.generate_from_parsed_data(parsed_data)
    
    # Write to file
    with open(args.output_xml, 'w', encoding='utf-8') as f:
        f.write(xml_output)
    
    print(f"Generated complete calculation view: {args.output_xml}")

if __name__ == '__main__':
    main()