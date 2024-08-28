import os
from typing import Optional
from loguru import logger

import streamlit as st
import looker_sdk
from looker_sdk import models40 as models
from snowflake.connector import SnowflakeConnection, ProgrammingError


# from admin_apps.partner.partner_utils import clean_table_columns
from admin_apps.shared_utils import (
    get_available_databases,
    get_available_schemas,
    format_snowflake_context,
)

# Partner semantic support instructions
LOOKER_IMAGE = 'admin_apps/images/looker.png'
LOOKER_INSTRUCTIONS = """
We materialize your Explore dataset in **Looker** as Snowflake table(s) and generate a Cortex Analyst semantic file.
Metadata from your Explore fields will be merged with the generated Cortex Analyst semantic file.

**Note**: Views referenced in the Looker Explores must be tables/views in Snowflake. Looker SDK credentials are required. 
Visit [Looker Authentication SDK Docs](https://cloud.google.com/looker/docs/api-auth#authentication_with_an_sdk) for more information.
> Steps:
> 1) Provide your Looker project details below.
> 2) Specify the Snowflake database and schema to materialize the Explore dataset as table(s).
> 3) A semantic file will be generated for the Snowflake table(s) and metadata from Looker populated.
"""

def update_schemas() -> None:
    """
    Callback to run when the selected databases change. Ensures that if a database is deselected, the corresponding
    schema is also deselected.
    Returns: None

    """
    database = st.session_state["looker_target_database"]

    # Fetch the available schemas for the selected databases
    try:
        if database:
            schemas = get_available_schemas(database)
        else:
            schemas = []
    except ProgrammingError:
        logger.error(
            f"Insufficient permissions to read from database {database}."
        )

    st.session_state["looker_available_schemas"] = schemas

    if st.session_state["looker_target_schema"] in st.session_state["looker_available_schemas"]:
        valid_selected_schemas = st.session_state["looker_target_schema"]
    else:
        valid_selected_schemas = None
    st.session_state["looker_target_schema"] = valid_selected_schemas


def set_looker_semantic() -> None:
    st.write(
        """
        Please fill out the following information about your Looker Explore.
        The Explore will be materialized as a Snowflake table.
        """
    )
    col1, col2 = st.columns(2)

    with col1:
        looker_model_name = st.text_input(
            "Model Name",
            key="looker_model_name",
            help="The name of the LookML Model containing the Looker Explore you would like to replicate in Cortex Analyst.",
            value="jaffle",
        )

        looker_explore_name = st.text_input(
            "Explore Name",
            key="looker_explore_name",
            help="The name of the LookML Explore to replicate in Cortex Analyst.",
            value="jaffle_customers"
        )
    with col2:
        looker_base_url = st.text_input(
            "Looker SDK Base URL",
            key="looker_base_url",
            help="TO DO - add help text with link",
            value="https://snowflakedemo.looker.com"
        )

        looker_client_id = st.text_input(
            "Looker SDK Client ID",
            key="looker_client_id",
            help="TO DO - add help text with link",
            value="3r4Q38HMgTyQfX5KMyrF"
        )

        looker_client_secret = st.text_input(
            "Looker SDK Client Secret",
            key="looker_client_secret",
            help="TO DO - add help text with link",
            type="password",
        )

    st.divider()
    with st.spinner("Loading databases..."):
        available_databases = get_available_databases()
    st.write(
        """
        Please pick a Snowflake destination for the table.
        """)
    st.selectbox(
        label="Database",
        index=None,
        options=available_databases,
        placeholder="Select the database to materialize the Explore dataset as a Snowflake table.",
        on_change=update_schemas,
        key="looker_target_database",
    )

    st.selectbox(
        label="Schema",
        index=None,
        options=st.session_state.get("looker_available_schemas", []),
        placeholder="Select the schema to materialize the Explore dataset as a Snowflake table.",
        key="looker_target_schema",
        format_func=lambda x: format_snowflake_context(x, -1),
    )

    st.text_input(
        "Snowflake Table",
        key="looker_target_table_name",
        help="The name of the LookML Explore to replicate in Cortex Analyst.",
    )


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


def fetch_columns_in_table(conn: SnowflakeConnection, table_name: str) -> list[str]:
    
    """
    Fetches all columns in a Snowflake table table
    Args:
        conn: SnowflakeConnection to run the query
        table_name: The fully-qualified name of the table.

    Returns: a list of qualified schema names (db.schema)
    """

    query = f"show columns in table {table_name};"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return [result[2] for result in results]


def clean_table_columns(conn: SnowflakeConnection,
                        snowflake_context: str,
                        tablename: str) -> None:
    
    """Renames table columns to remove alias prefixes and double quotes"""
    
    columns = fetch_columns_in_table(conn, f'{snowflake_context}.{tablename}')

    for col in columns:
        if '.' in col:
            # new_col = ast.literal_eval(col).split('.')[-1]
            new_col = col.split('.')[-1].upper()
        else:
            new_col = col
        query = f'ALTER TABLE {snowflake_context}.{tablename} RENAME COLUMN "{col}" TO {new_col};'
        conn.cursor().execute(query)


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

