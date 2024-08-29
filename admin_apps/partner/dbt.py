import yaml
from typing import Any

import streamlit as st
import pandas as pd

# Partner semantic support instructions
DBT_IMAGE = 'admin_apps/images/dbt-signature_tm_black.png'
DBT_INSTRUCTIONS = """
We extract metadata from your **DBT** semantic yaml file(s) and merge it with a generated Cortex Analyst semantic file.

**Note**: The DBT semantic layer must be sourced from tables/views in Snowflake.
> Steps:
> 1) Upload your dbt semantic (yaml/yml) file(s) below. 
> 2) Select **🛠 Create a new semantic model** to generate a new Cortex Analyst semantic file for Snowflake tables or **✏️ Edit an existing semantic model**.
> 3) Validate the output in the UI.
> 4) Once you've validated the semantic file, click **Partner Semantic** to merge DBT and Cortex Analyst semantic files.  
"""


def upload_dbt_semantic() -> None:
    """
    Upload semantic file(s) for dbt from local source.
    """
    uploaded_files = st.file_uploader(
            f'Upload {st.session_state["partner_tool"]} semantic yaml file(s)',
            type=["yaml", "yml"],
            accept_multiple_files=True,
            key="myfile",
        )
    if uploaded_files:
        partner_semantic = []
        for file in uploaded_files:
            partner_semantic.extend(read_dbt_yaml(file))

        if not partner_semantic:
            st.error(
                "Upload file(s) do not contain required semantic_models section."
            )
        else:
            st.session_state["partner_semantic"] = partner_semantic
        if st.button("Continue", type="primary"):
            st.session_state['partner_setup'] = True
            st.rerun()
    else:
        st.session_state["partner_semantic"] = None


class DBTEntity:
    def __init__(self,
                 entity: dict[str, Any]):
        
        self.entity = entity
        self.name = entity['name']
        self.type = entity.get('type', None)
        self.expr = entity.get('expr', self.name)
        self.description = entity.get('description', None)
        self.cortex_map = {
                'name': self.name,
                'description': self.description,
                'expr': self.expr,
                'data_type': self.get_cortex_type(),
            }

    def get_data(self):
        return self.entity
    
    def get_cortex_type(self):
        return 'TEXT'
    
    def get_cortex_section(self):
        return 'dimensions'
    
    def get_key(self):
        return self.expr.upper()
    
    def get_cortex_details(self):
        return_details = {}
        for k,v in self.cortex_map.items():
            if v is not None:
                return_details[k] = v
        return return_details
    
    def get_cortex_comparison_dict(self):
        return {
            'field_key': self.get_key(),
            'section': self.get_cortex_section(),
            'field_details': self.get_cortex_details()
        }


class DBTMeasure(DBTEntity):
    def __init__(self, entity):
        super().__init__(entity)
        self.agg = entity.get('agg', None)
        self.cortex_map = {
                'name': self.name,
                'description': self.description,
                'expr': self.expr,
                'data_type': self.get_cortex_type(),
                'default_aggregation': self.agg
            }

    def get_cortex_type(self):
        return 'NUMBER'
    
    def get_cortex_section(self):
        return 'measures'
        

class DBTDimension(DBTEntity):
    
    def get_cortex_type(self):
        if self.type == 'time':
            return 'DATETIME'
        else:
            return 'TEXT'
    
    def get_cortex_section(self):
        if self.type == 'time':
            return 'time_dimensions'
        else:
            return 'dimensions'
        

class DBTSemanticModel:
    def __init__(self,
                 data: dict[str, Any]):
        self.data = data
        self.name = data['name']
        self.description = data['description']
        self.entities = data['entities']
        self.dimensions = data['dimensions']
        self.measures = data['measures']
    
    def get_data(self):
        return self.data
    
    def get_name(self):
        return self.name
    
    def get_description(self):
        return self.description
    
    def get_cortex_fields(self):
        cortex_fields = []
        for entity in self.entities:
            cortex_fields.append(DBTEntity(entity).get_cortex_comparison_dict())
        for measure in self.measures:
            cortex_fields.append(DBTMeasure(measure).get_cortex_comparison_dict())
        for dimension in self.dimensions:
            cortex_fields.append(DBTDimension(dimension).get_cortex_comparison_dict())
        
        return cortex_fields
        
    def create_comparison_df(self):
        cortex_fields = self.get_cortex_fields()
        return pd.DataFrame(cortex_fields)
    
    @staticmethod
    def retrieve_df_by_name(name: str) -> 'DBTSemanticModel':
        for model in st.session_state['partner_semantic']:
            if model.get_name() == name:
                return model.create_comparison_df()
    

def read_dbt_yaml(file_path):
    # with open(file_path, 'r') as file:
    data = yaml.safe_load(file_path)
    if 'semantic_models' in data:
        dbt_semantic_models = []
        for semantic_model in data['semantic_models']:
            dbt_semantic_models.append(DBTSemanticModel(semantic_model))
    else:
        st.warning(f"{file_path} does not contain semantic_models section. Skipping.")

    return dbt_semantic_models

