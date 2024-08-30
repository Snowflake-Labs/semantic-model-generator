import os
from typing import Optional, Any
from loguru import logger

import streamlit as st
import looker_sdk
import pandas as pd
from looker_sdk import models40 as models
from snowflake.connector import SnowflakeConnection, ProgrammingError

from semantic_model_generator.data_processing.proto_utils import proto_to_dict

from admin_apps.shared_utils import (
    GeneratorAppScreen,
    get_available_databases,
    get_available_schemas,
    format_snowflake_context,
    get_snowflake_connection,
    input_sample_value_num,
    input_semantic_file_name,
    run_generate_model_str_from_snowflake,
    check_valid_session_state_values,
    set_sit_query_tag,
)

from admin_apps.partner.cortex import (
    CortexSemanticTable,
    CortexDimension,
    CortexMeasure,
    CortexTimeDimension,
)


# Partner semantic support instructions
LOOKER_IMAGE = 'admin_apps/images/looker.png'
LOOKER_INSTRUCTIONS = """
We materialize your Explore dataset in Looker as Snowflake table(s) and generate a Cortex Analyst semantic file.
Metadata from your Explore fields will be merged with the generated Cortex Analyst semantic file.

**Note**: Views referenced in the Looker Explores must be tables/views in Snowflake. Looker SDK credentials are required. 
Visit [Looker Authentication SDK Docs](https://cloud.google.com/looker/docs/api-auth#authentication_with_an_sdk) for more information.

**Tip**: Install Looker's [API Explorer extension](https://cloud.google.com/looker/docs/api-explorer) from the Looker Marketplace to view API credentials directly.

> **Steps**:
> 1) Provide your Looker project details below.
> 2) Specify the Snowflake database and schema to materialize the Explore dataset as table(s).
> 3) A semantic file will be generated for the Snowflake table(s). Click **Integrate Partner** to merge additional Looker metadata if provided.  
"""

def update_schemas() -> None:
    """
    Callback to run when the selected databases change. Ensures that if a database is deselected, the corresponding
    schema is also deselected.
    
    Returns:
        None

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
    """
    Provides interface for user to enter Looker specs for Looker API.
    
    Returns:
        None

    """

    st.write(
        """
        Please fill out the following information about your **Looker Explore**.
        The Explore will be materialized as a Snowflake table.
        """
    )
    with st.spinner("Loading databases..."):
        available_databases = get_available_databases()
    col1, col2 = st.columns(2)

    with col1:
        looker_model_name = st.text_input(
            "Model Name",
            key="looker_model_name",
            help="The name of the LookML Model containing the Looker Explore you would like to replicate in Cortex Analyst.",
        )

        looker_explore_name = st.text_input(
            "Explore Name",
            key="looker_explore_name",
            help="The name of the LookML Explore to replicate in Cortex Analyst.",
        )

        looker_connection_db = st.selectbox(
            label="Looker View Database",
            index=None,
            options=available_databases,
            placeholder="Optional",
            help="Specify Snowflake database if your Looker Views do not explicitly reference Snowflake databases and instead rely on [Looker database connection](https://cloud.google.com/looker/docs/db-config-snowflake#creating-the-connection-to-your-database).",
            key="looker_connection_db",
        )

    with col2:
        looker_base_url = st.text_input(
            "Looker SDK Base URL",
            key="looker_base_url",
            help="Looker Base URL also known as [API Host URL](https://cloud.google.com/looker/docs/admin-panel-platform-api#api_host_url).",
        )

        looker_client_id = st.text_input(
            "Looker SDK Client ID",
            key="looker_client_id",
            help="See [Looker Authentication with an SDK](https://cloud.google.com/looker/docs/api-auth#authentication_with_an_sdk) for API credentials generation.",
        )

        looker_client_secret = st.text_input(
            "Looker SDK Client Secret",
            key="looker_client_secret",
            help="See [Looker Authentication with an SDK](https://cloud.google.com/looker/docs/api-auth#authentication_with_an_sdk) for API credentials generation.",
            type="password",
        )

    st.divider()
    # with st.spinner("Loading databases..."):
    #     available_databases = get_available_databases()
    st.write(
        """
        Please pick a Snowflake destination for the table.
        """)
    col1, col2 = st.columns(2)
    with col1:
        st.selectbox(
            label="Database",
            index=None,
            options=available_databases,
            help="Select the database to materialize the Explore dataset as a Snowflake table.",
            on_change=update_schemas,
            key="looker_target_database",
        )

        st.selectbox(
            label="Schema",
            index=None,
            options=st.session_state.get("looker_available_schemas", []),
            help="Select the schema to materialize the Explore dataset as a Snowflake table.",
            key="looker_target_schema",
            format_func=lambda x: format_snowflake_context(x, -1),
        )

        st.text_input(
            "Snowflake Table",
            key="looker_target_table_name",
            help="Specify the name of the Snowflake table to materialize.",
        )
    with col2:
        model_name = input_semantic_file_name()
        sample_values = input_sample_value_num()

    if st.button("Continue", type="primary"):
        if check_valid_session_state_values([
            "looker_model_name",
            "looker_explore_name",
            "looker_base_url",
            "looker_client_id",
            "looker_client_secret",
            "looker_target_database",
            "looker_target_schema",
            "looker_target_table_name",
        ]):

            with st.spinner("Saving Explore as a Snowflake table..."):
                
                # Cortex model generation reqires fully-qualified table name
                full_tablename = f"{st.session_state['looker_target_schema']}.{st.session_state['looker_target_table_name']}"

                looker_field_metadata = render_looker_explore_as_table(
                                get_snowflake_connection(),
                                st.session_state['looker_model_name'].lower(),
                                st.session_state['looker_explore_name'].lower(),
                                st.session_state['looker_target_schema'],
                                st.session_state['looker_target_table_name'].upper(),
                                st.session_state['looker_connection_db'],
                                [], # TO DO - Add support for field selection
                                )
                
                st.session_state['looker_field_metadata'] = looker_field_metadata
            run_generate_model_str_from_snowflake(
                model_name,
                sample_values,
                [full_tablename],
            )

            st.session_state['partner_setup'] = True
            set_sit_query_tag(
                    get_snowflake_connection(),
                    vendor="looker",
                    action="setup_complete",
                )
            # Take user to iteration using generated semantic file.
            # User can merge Looker metadata with generated semantic file on iteration page.
            st.session_state["page"] = GeneratorAppScreen.ITERATION
            st.rerun()


def set_looker_config() -> None:
    """
    Sets Looker SDK connection
    """

    import looker_sdk
    os.environ["LOOKERSDK_BASE_URL"] = st.session_state['looker_base_url'] #If your looker URL has .cloud in it (hosted on GCP), do not include :19999 (ie: https://your.cloud.looker.com).
    os.environ["LOOKERSDK_API_VERSION"] = "4.0" #As of Looker v23.18+, the 3.0 and 3.1 versions of the API are removed. Use "4.0" here.
    os.environ["LOOKERSDK_VERIFY_SSL"] = "true" #Defaults to true if not set. SSL verification should generally be on unless you have a real good reason not to use it. Valid options: true, y, t, yes, 1.
    os.environ["LOOKERSDK_TIMEOUT"] = "120" #Seconds till request timeout. Standard default is 120.

    # Get the following values from your Users page in the Admin panel of your Looker instance > Users > Your user > Edit API keys. If you know your user id, you can visit https://your.looker.com/admin/users/<your_user_id>/edit.
    os.environ["LOOKERSDK_CLIENT_ID"] =  st.session_state['looker_client_id'] #No defaults.
    os.environ["LOOKERSDK_CLIENT_SECRET"] = st.session_state['looker_client_secret'] #No defaults. This should be protected at all costs. Please do not leave it sitting here, even if you don't share this document.

    sdk = looker_sdk.init40()
    return sdk

def get_explore_fields(
        sdk: looker_sdk.sdk.api40.methods.Looker40SDK,
        model_name: str,
        explore_name: str,
        fields: Optional[list] = None
        ) -> dict[str, dict[str, str]]:
    
    """
    Retrieves dimensions and measure fields from Looker Explore.
    Args:
        sdk (looker_sdk.sdk.api40.methods.Looker40SDK): Looker connection
        model_name (str): Looker model name. Should be lowercase.
        explore_name (str): Looker explore name. Should be lowercase.
        fields (list): List of fields to extract from the Explore. Default is None.

    Returns: dict[str, dict[str, str]] column metadata for the Explore
    """

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
    
    """
    Creates a query ID corresponding to SQL to preview Looker Explore data.
    Args:
        sdk (looker_sdk.sdk.api40.methods.Looker40SDK): Looker connection
        model_name (str): Looker model name. Should be lowercase.
        explore_name (str): Looker explore name. Should be lowercase.
        fields (list): List of fields to extract from the Explore. Default is None.

    Returns: str query ID
    """

    query = sdk.create_query(
        body=models.WriteQuery(
            model=model_name,
            view=explore_name,
            fields=fields
        )
    )

    return query.id

def create_explore_ctas(
        query_string: str,
        snowflake_context: str,
        table_name: str
        ) -> str:
    
    """
    Augments Looker Explore SQL with CTAS prefix to create a materialized view.
    Args:
        query_string (str): SQL select statement to render Looker Explore.
        snowflake_context (str): Database_name.Schema_name for Snowflake
        table_name (str): Name of table to create in snowflake_context context.

    Returns: str CTAS statement
    """

    # Remove unnecessary lines of sql
    lines = query_string.split('\n')
    filtered_lines = [line for line in lines if not line.strip().startswith(('LIMIT', 'FETCH'))]
    filtered_query_string = '\n'.join(filtered_lines)
    
    # Add CTAS clause
    ctas_clause = f'CREATE OR REPLACE TABLE {snowflake_context}.{table_name} AS\n'
    return ctas_clause + filtered_query_string


def get_explore_sql(sdk: looker_sdk.sdk.api40.methods.Looker40SDK,
                    query_id: str) -> str:
    
    """
    Retrieves Looker SQL query of query ID
    Args:
        sdk (looker_sdk.sdk.api40.methods.Looker40SDK): Looker connection
        query_id (str): Query ID

    Returns: str SELECT statement
    """
    
    response = sdk.run_query(query_id=query_id, 
                             result_format='sql')
    return response


def fetch_columns_in_table(conn: SnowflakeConnection, table_name: str) -> list[str]:
    
    """
    Fetches all columns in a Snowflake table table
    Args:
        conn: SnowflakeConnection to run the query
        table_name: The fully-qualified name of the table.

    Returns: a list of qualified column names (db.schema.column)
    """

    query = f"show columns in table {table_name};"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return [result[2] for result in results]


def clean_table_columns(conn: SnowflakeConnection,
                        snowflake_context: str,
                        table_name: str) -> list[str]:
    
    """
    Renames table columns to remove alias prefixes and double quotes.
    Args:
        conn: SnowflakeConnection to run the query
        snowflake_context (str): Database_name.Schema_name for Snowflake
        table_name: The fully-qualified name of the table.

    Returns: a list of column names
    """
    
    columns = fetch_columns_in_table(conn, f'{snowflake_context}.{table_name}')
    new_cols = []

    for col in columns:
        if '.' in col:
            new_col = col.split('.')[-1].upper()
        else:
            new_col = col
        new_cols.append(new_col)
        query = f'ALTER TABLE {snowflake_context}.{table_name} RENAME COLUMN "{col}" TO {new_col};'
        conn.cursor().execute(query)
    return new_cols


def render_looker_explore_as_table(conn: SnowflakeConnection,
                                   model_name: str,
                                   explore_name: str,
                                   snowflake_context: str,
                                   table_name: str,
                                   optional_db: Optional[str] = None,
                                   fields: Optional[list] = None) -> dict[str, dict[str, str]]:
    
    """
    Creates materialized table corresponding to Looker Explore.
    Args:
        conn: SnowflakeConnection to run the query
        model_name (str): Looker model name. Should be lowercase.
        explore_name (str): Looker explore name. Should be lowercase. 
        snowflake_context (str): Database_name.Schema_name for Snowflake
        table_name: The fully-qualified name of the table.
        fields (list): List of fields to extract from the Explore. Default is None.

    Returns: dict[str, dict[str, str]] column metadata
    """

    sdk = set_looker_config()
    
    # Get fields from explore
    try:
        field_metadata = get_explore_fields(
            sdk, model_name, explore_name, fields
            )
    except Exception as e:
        st.error(f"Error fetching Looker Explore fields: {e}")
    if field_metadata:
        fields = list(field_metadata.keys())

        # Get query to define materialized view
        query_id = create_query_id(
            sdk, model_name, explore_name, fields
            )
        explore_sql = get_explore_sql(sdk, query_id)
        ctas = create_explore_ctas(
            explore_sql, snowflake_context, table_name
            )

        # Create materialized equivalent of Explore
        # Looker sources don't require explicit database qualification but instead use connection database implicitly.
        # Set optional_db if user provides it. Need to reset back to original afterwards.
        if optional_db:
            current_db = conn.cursor().execute('SELECT CURRENT_DATABASE();').fetchone()[0]
            conn.cursor().execute(f'USE DATABASE {optional_db};')
            conn.cursor().execute(ctas)
            if current_db: # Original connection does not have a database. No programmatic way to reset to None.
                conn.cursor().execute(f'USE DATABASE {current_db};')
        else:
            conn.cursor().execute(ctas)
        # Looker creates lower case double-quoted column names with alias prefixes
        new_columns = clean_table_columns(
            conn, snowflake_context, table_name
            )

        # Associate new column names with looker field metadata
        column_metadata = dict(zip(new_columns, list(field_metadata.values())))
        return column_metadata


class LookerDimension(CortexDimension):
    """
    Class for Looker dimension-type field.
    """
       
    def __init__(self, data):
        super().__init__(data)
        if self.get_name() in st.session_state['looker_field_metadata']:
            description = st.session_state['looker_field_metadata'][self.get_name()].get('description', None)
        self.description = description

    def get_cortex_comparison_dict(self) -> dict[str, Any]:
        cortex_details = self.get_cortex_details()
        cortex_details['description'] = self.description
        
        return {
            'field_key': self.get_key(),
            'section': self.get_cortex_section(),
            'field_details': cortex_details
        }


class LookerMeasure(CortexMeasure):
    """
    Class for Looker measure-type field.
    """

    def __init__(self, data):
        super().__init__(data)
        if self.get_name() in st.session_state['looker_field_metadata']:
            description = st.session_state['looker_field_metadata'][self.get_name()].get('description', None)
        self.description = description

    def get_cortex_comparison_dict(self) -> dict[str, Any]:
        cortex_details = self.get_cortex_details()
        cortex_details['description'] = self.description
        
        return {
            'field_key': self.get_key(),
            'section': self.get_cortex_section(),
            'field_details': cortex_details
        }


class LookerTimeDimension(CortexTimeDimension):
    """
    Class for Looker time dimension-type field.
    """

    def __init__(self, data):
        super().__init__(data)
        if self.get_name() in st.session_state['looker_field_metadata']:
            description = st.session_state['looker_field_metadata'][self.get_name()].get('description', None)
        self.description = description

    def get_cortex_comparison_dict(self) -> dict[str, Any]:
        cortex_details = self.get_cortex_details()
        cortex_details['description'] = self.description
        
        return {
            'field_key': self.get_key(),
            'section': self.get_cortex_section(),
            'field_details': cortex_details
        }


class LookerSemanticTable(CortexSemanticTable):
    """
    Class for single Looker logical table in semantic file.

    The generated Cortex semantic file is used as a basis for the Looker model
    with minor adjustments for Looker-specific metadata
    """

    def get_cortex_fields(self):
        """
        Processes and returns raw field data as vendor-specific field objects.
        """

        cortex_fields = []
        for dimension in self.dimensions:
            cortex_fields.append(LookerDimension(dimension).get_cortex_comparison_dict())
        for time_dimension in self.time_dimensions:
            cortex_fields.append(LookerTimeDimension(time_dimension).get_cortex_comparison_dict())
        for measure in self.measures:
            cortex_fields.append(LookerMeasure(measure).get_cortex_comparison_dict())
        
        return cortex_fields
    
    
    @staticmethod
    def create_cortex_table_list() -> None:
        cortex_semantic = proto_to_dict(st.session_state["semantic_model"])
        tables = []
        for table in cortex_semantic['tables']:
            tables.append(LookerSemanticTable(table))
        st.session_state['partner_semantic'] = tables


    @staticmethod
    def retrieve_df_by_name(name: str) -> pd.DataFrame:
        for table in st.session_state["partner_semantic"]:
            if table.get_name() == name:
                return table.create_comparison_df()

