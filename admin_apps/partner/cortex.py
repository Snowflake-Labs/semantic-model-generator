from typing import Any

import streamlit as st
import pandas as pd

from semantic_model_generator.data_processing.proto_utils import proto_to_dict


class CortexDimension:
    def __init__(self,
                 data: dict[str, Any]):
        
        self.data = data
        self.name = data['name']
        self.synonyms = data.get('synonyms', None)
        self.data_type = data.get('data_type', 'TEXT')
        self.expr = data['expr']
        self.description = data.get('description', None)
        self.sample_values = data.get('sample_values', None)
        self.unique = data.get('unique', False)

    def get_name(self):
        return self.name

    def get_data(self):
        return self.data
    
    def get_cortex_type(self):
        return self.data_type
    
    def get_description(self):
        return self.description
    
    def set_description(self, value):
        self.description = value
    
    def get_cortex_section(self):
        return 'dimensions'
    
    def get_key(self):
        return self.expr.upper()
    
    def get_cortex_details(self):
        return self.data
    
    def get_cortex_comparison_dict(self):
        return {
            'field_key': self.get_key(),
            'section': self.get_cortex_section(),
            'field_details': self.get_cortex_details()
        }
    

class CortexTimeDimension(CortexDimension):
    def get_cortex_section(self):
        return 'time_dimensions'
    

class CortexMeasure(CortexDimension):
    def __init__(self, entity):
        super().__init__(entity)
        self.default_aggregation = entity.get('default_aggregation', None)

    def get_cortex_section(self):
        return 'measures'



class CortexSemanticTable:
    def __init__(self,
                 data: dict[str, Any]):
        self.data = data
        self.name = data['name']
        self.description = data['description']
        self.base_table_db = data['base_table']['database']
        self.base_table_schema = data['base_table']['schema']
        self.base_table_table = data['base_table']['table']
        self.dimensions = data['dimensions']
        self.time_dimensions = data['time_dimensions']
        self.measures = data['measures']
    
    def get_data(self):
        return self.data
    
    def get_name(self):
        return self.name
    
    def get_description(self):
        return self.description
    
    def get_cortex_fields(self):
        cortex_fields = []
        for dimension in self.dimensions:
            cortex_fields.append(CortexDimension(dimension).get_cortex_comparison_dict())
        for time_dimension in self.time_dimensions:
            cortex_fields.append(CortexTimeDimension(time_dimension).get_cortex_comparison_dict())
        for measure in self.measures:
            cortex_fields.append(CortexMeasure(measure).get_cortex_comparison_dict())
        
        return cortex_fields
        
    def create_comparison_df(self):
        cortex_fields = self.get_cortex_fields()
        return pd.DataFrame(cortex_fields)
    
    @staticmethod
    def create_cortex_table_list() -> None:
        cortex_semantic = proto_to_dict(st.session_state["semantic_model"])
        # Need to replace table details in current entire yaml
        st.session_state['current_yaml_as_dict'] = cortex_semantic
        tables = []
        for table in cortex_semantic['tables']:
            tables.append(CortexSemanticTable(table))
        st.session_state['cortex_comparison_tables'] = tables

    @staticmethod
    def retrieve_df_by_name(name: str) -> pd.DataFrame:
        for table in st.session_state["cortex_comparison_tables"]:
            if table.get_name() == name:
                return table.create_comparison_df()