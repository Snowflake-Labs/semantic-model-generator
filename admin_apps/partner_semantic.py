from typing import Optional
import yaml

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


def run_cortex_complete(conn: SnowflakeConnection,
                        model: str,
                        prompt: str,
                        prompt_args: Optional[dict] = None) -> str:
    
    if prompt_args:
        prompt = prompt.format(**prompt_args).replace("'", "\\'")
    complete_sql = f"SELECT snowflake.cortex.complete('{model}', '{prompt}')"
    response = conn.cursor().execute(complete_sql).fetchone()[0]

    return response
