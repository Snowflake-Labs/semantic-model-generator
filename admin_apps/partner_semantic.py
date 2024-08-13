from typing import Optional
import yaml
import json

import pandas as pd
from snowflake.connector import SnowflakeConnection
# from snowflake.snowpark.exceptions import SnowparkSQLException

def unpack_yaml(data):
    """
    Recursively unpacks a YAML structure into a Python dictionary.
    """
    if isinstance(data, dict):
        return {key: unpack_yaml(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [unpack_yaml(item) for item in data]
    else:
        return data

def load_yaml_file(file_paths) -> list[dict]:
    """
    Loads one or more YAML files and combines them into a single list.
    """
    combined_yaml = []
    for file_path in file_paths:
        yaml_content = yaml.safe_load(file_path)
        combined_yaml.append(unpack_yaml(yaml_content))
    return combined_yaml

def extract_key_values(data: list[dict], key: str) -> list[dict]:
    """
    Extracts key's value from a list of dictionaries.
    """
    result = []
    for item in data:
        values = item.get(key, [])
        if isinstance(values, list):
            result.extend(values)
        else:
            result.append(values)
    return result

def extract_dbt_models(yaml_data: list[dict]) -> list:
    """
    Extracts dbt models from a dictionary of YAML data.
    """
    
    return [x.get('name', None) for model in yaml_data for x in model.get('semantic_models', None)]

def extract_expressions_from_sections(data_dict, section_names):
    """
    Extracts data in section_names from a dictionary into a nested dictionary:
    """
    def extract_key(obj):
        return obj.get('expr', obj['name']).lower()
    
    d = {}
    for i in section_names:
        d[i] = {extract_key(obj): obj for obj in data_dict.get(i, [])}
    
    return d

def make_field_df(fields):
    """
    Converts a nested dictionary of fields into a DataFrame.
    """
    rows = []
    for section, entity_list in fields.items():
        for field_key, field_details in entity_list.items():
            rows.append({'section': section,
                        'field_key': field_key,
                        'field_details': field_details
                        })
    return pd.DataFrame(rows)

def create_table_field_df(table_name: str,
                          sections: list[str],
                          yaml_data: list[dict]) -> pd.DataFrame:
    """
    Extracts sections of table_name in yaml_data dictionary as a DataFrame.
    """
    view = [x for x in yaml_data if x.get('name') == table_name][0]
    fields = extract_expressions_from_sections(view, sections)
    fields_df = make_field_df(fields)

    return fields_df

def determine_field_section(section_cortex: str,
                            section_partner: str,
                            field_details_cortex: str,
                            field_details_partner: str):
    """
    Derives intended section of field in cortex analyst model.

    Currently expects dbt as source.
    """

    if section_cortex and field_details_cortex:
        try:
            # field_details_cortex = json.loads(field_details_cortex)
            data_type = field_details_cortex.get('data_type', None)
        except TypeError:
            data_type = 'TEXT'
        return (section_cortex, data_type)
    else: # No matching cortex field found; field is partner is a novel logical field
        if section_partner == 'entities':
            section_cortex = 'dimensions'
            data_type = 'TEXT'
        elif section_partner == 'measures':
            section_cortex = 'measures'
            data_type = 'NUMBER'
        else: # field_details_partner == 'dimensions'
            try:
                # field_details_partner = json.loads(field_details_partner)
                if field_details_partner.get('type') == 'time':
                    section_cortex = 'time_dimensions'
                    data_type = 'DATE'
            except TypeError:
                section_cortex = 'dimensions'
                data_type = 'TEXT'
            else:
                section_cortex = 'dimensions'
                data_type = 'TEXT'
        return (section_cortex, data_type)


def merge_fields(field_key: str,
                 section_cortex: str,
                 section_dbt: str,
                 field_details_cortex: str,
                 field_details_dbt: str) -> tuple[str, str]: # (section, field_details)
    """
    Merges field details from cortex and dbt into a single field returning target section and field details.
    """
    # If the field is present in both models, we keep cortex details and add dbt details
    if section_cortex and section_dbt:
        selected_details = field_details_cortex
        for k in ['description', 'name']:
            selected_details[k] = field_details_dbt.get(k, field_details_cortex.get(k, None))
        selected_details['expr'] = field_key # Unique key will become expr for cortex
        return (section_cortex, selected_details)
    
    # If field exists in dbt but not in cortex, we add shell to cortex to keep field
    elif section_dbt and not section_cortex: 
        if section_dbt == 'entities':
            section_cortex = 'dimensions'
            data_type = 'TEXT'
        elif section_dbt == 'measures':
            section_cortex = 'measures'
            data_type = 'NUMBER'
        else: # section_dbt == 'dimensions'
            if field_details_dbt.get('type') == 'time':
                section_cortex = 'time_dimensions'
                data_type = 'DATE'
            else:
                section_cortex = 'dimensions'
                data_type = 'TEXT'
        return (section_cortex, {
                                'name': field_details_dbt.get('name', None),
                                'synonyms': [' '],
                                'description': field_details_dbt.get('description', None),
                                'expr': field_key,
                                'data_type': data_type
                            })
    
    else:
        return (section_cortex, field_details_cortex)


def run_cortex_complete(conn: SnowflakeConnection,
                        model: str,
                        prompt: str,
                        prompt_args: Optional[dict] = None) -> str:
    
    if prompt_args:
        prompt = prompt.format(**prompt_args).replace("'", "\\'")
    complete_sql = f"SELECT snowflake.cortex.complete('{model}', '{prompt}')"
    response = conn.cursor().execute(complete_sql).fetchone()[0]

    return response

