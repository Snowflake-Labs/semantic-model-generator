import os
from typing import Optional

import streamlit as st
import looker_sdk
from looker_sdk import models40 as models
from snowflake.connector import SnowflakeConnection


from admin_apps.partner.partner_utils import clean_table_columns

def set_looker_config() -> None:
    """
    Sets Looker SDK connection
    """
    import looker_sdk
    os.environ["LOOKERSDK_BASE_URL"] = st.session_state['looker_base_url'] #If your looker URL has .cloud in it (hosted on GCP), do not include :19999 (ie: https://your.cloud.looker.com).
    os.environ["LOOKERSDK_API_VERSION"] = "4.0" #As of Looker v23.18+, the 3.0 and 3.1 versions of the API are removed. Use "4.0" here.
    os.environ["LOOKERSDK_VERIFY_SSL"] = "true" #Defaults to true if not set. SSL verification should generally be on unless you have a real good reason not to use it. Valid options: true, y, t, yes, 1.
    os.environ["LOOKERSDK_TIMEOUT"] = "120" #Seconds till request timeout. Standard default is 120.

    #Get the following values from your Users page in the Admin panel of your Looker instance > Users > Your user > Edit API keys. If you know your user id, you can visit https://your.looker.com/admin/users/<your_user_id>/edit.
    os.environ["LOOKERSDK_CLIENT_ID"] =  st.session_state['looker_client_id'] #No defaults.
    os.environ["LOOKERSDK_CLIENT_SECRET"] = st.session_state['looker_client_secret'] #No defaults. This should be protected at all costs. Please do not leave it sitting here, even if you don't share this document.

    sdk = looker_sdk.init40()
    return sdk

def get_explore_fields(
        sdk: looker_sdk.sdk.api40.methods.Looker40SDK,
        model_name: str,
        explore_name: str,
        fields: list = None
        ) -> dict:
    """Retrieves dimensions and measure fields from Looker Explore"""

    field_keys = [
        'dimensions',
        'measures'
    ]
    extracted_fields = {}

    if fields:
        response = sdk.lookml_model_explore(
            lookml_model_name=model_name,
            explore_name=explore_name,
            fields=fields
        )
    else:
        response = sdk.lookml_model_explore(
            lookml_model_name=model_name,
            explore_name=explore_name
        )

    # Extract dimensions and measures from the response fields
    # Only need field name, tags, and descriptions
    for k in field_keys:
        for field in response.fields[k]:
            extracted_fields[field['name']] = {
                'description': field['description'],
                'tags': field['tags']
                }

    return extracted_fields

def create_query_id(
        sdk: looker_sdk.sdk.api40.methods.Looker40SDK,
        model_name: str,
        explore_name: str,
        fields: list
        ) -> str:
    """Creates a query ID corresponding to SQL to preview Looker Explore data"""

    query = sdk.create_query(
        body=models.WriteQuery(
            model=model_name,
            view=explore_name,
            fields=fields
        )
    )

    return query.id

def create_explore_ctas(query_string: str,
                        snowflake_context: str,
                        table_name: str) -> str:
    """Augments Looker Explore SQL with CTAS prefix to create a materialized view"""

    # Remove unnecessary lines of sql
    lines = query_string.split('\n')
    filtered_lines = [line for line in lines if not line.strip().startswith(('LIMIT', 'FETCH'))]
    filtered_query_string = '\n'.join(filtered_lines)
    
    # Add CTAS clause
    ctas_clause = f'CREATE OR REPLACE TABLE {snowflake_context}.{table_name} AS\n'
    return ctas_clause + filtered_query_string

def get_explore_sql(sdk: looker_sdk.sdk.api40.methods.Looker40SDK,
                    query_id: str) -> str:
    """Retrieves Looker SQL query of query ID"""
    response = sdk.run_query(query_id=query_id, 
                             result_format='sql')
    return response

def render_looker_explore_as_table(conn: SnowflakeConnection,
                                   model_name: str,
                                   explore_name: str,
                                   snowflake_context: str,
                                   tablename: str,
                                   fields: Optional[list] = None):
    """Creates materialized table corresponding to Looker Explore"""

    sdk = set_looker_config()
    
    # Get fields from explore
    field_metadata = get_explore_fields(
        sdk, model_name, explore_name, fields
        )
    fields = list(field_metadata.keys())

    # Get query to define materialized view
    query_id = create_query_id(
        sdk, model_name, explore_name, fields
        )
    explore_sql = get_explore_sql(sdk, query_id)
    ctas = create_explore_ctas(
        explore_sql, snowflake_context, tablename
        )

    # Create materialized equivalent of Explore
    conn.cursor().execute(ctas)
    # Look creates lower case double-quoted column names with alias prefixes
    clean_table_columns(
        conn, snowflake_context, tablename
        )

    return field_metadata

