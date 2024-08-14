from typing import Optional
import yaml

import pandas as pd
from snowflake.connector import SnowflakeConnection
# from snowflake.snowpark.exceptions import SnowparkSQLException






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


def run_cortex_complete(conn: SnowflakeConnection,
                        model: str,
                        prompt: str,
                        prompt_args: Optional[dict] = None) -> str:
    
    if prompt_args:
        prompt = prompt.format(**prompt_args).replace("'", "\\'")
    complete_sql = f"SELECT snowflake.cortex.complete('{model}', '{prompt}')"
    response = conn.cursor().execute(complete_sql).fetchone()[0]

    return response

