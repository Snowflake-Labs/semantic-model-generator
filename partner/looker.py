import os
from typing import Any, Optional, Union

import pandas as pd
import streamlit as st
from loguru import logger
from snowflake.connector import ProgrammingError, SnowflakeConnection

from app_utils.shared_utils import (
    GeneratorAppScreen,
    check_valid_session_state_values,
    format_snowflake_context,
    get_available_databases,
    get_available_schemas,
    get_available_warehouses,
    get_sit_query_tag,
    get_snowflake_connection,
    input_sample_value_num,
    input_semantic_file_name,
    run_generate_model_str_from_snowflake,
    set_sit_query_tag,
    set_table_comment,
)
from partner.cortex import (
    CortexDimension,
    CortexMeasure,
    CortexSemanticTable,
    CortexTimeDimension,
)
from semantic_model_generator.data_processing.proto_utils import proto_to_dict

try:
    import looker_sdk
    from looker_sdk import models40 as models
except ImportError:
    raise ImportError(
        "The looker extra is required. You can install it using pip:\n\n"
        "pip install -e '.[looker]'\n"
    )


# Partner semantic support instructions
LOOKER_IMAGE = "images/looker.png"
LOOKER_INSTRUCTIONS = """
We materialize your [Explore](https://cloud.google.com/looker/docs/reference/param-explore-explore) dataset in Looker as Snowflake table(s) and generate a Cortex Analyst semantic file.
Metadata from your Explore fields can be merged with the generated Cortex Analyst semantic file.

**Note**: Views referenced in the Looker Explores must be tables/views in Snowflake. Looker SDK credentials are required.
Visit [Looker Authentication SDK Docs](https://cloud.google.com/looker/docs/api-auth#authentication_with_an_sdk) for credential information.
If using Streamlit in Snowflake, an external access integration is **required**. See project README for external access integration instructions.


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
    schemas = []
    try:
        if database:
            schemas = get_available_schemas(database)
    except ProgrammingError:
        logger.error(f"Insufficient permissions to read from database {database}.")

    st.session_state["looker_available_schemas"] = schemas

    if (
        st.session_state["looker_target_schema"]
        in st.session_state["looker_available_schemas"]
    ):
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
    with st.spinner("Loading warehouses..."):
        available_warehouses = get_available_warehouses()
    col1, col2 = st.columns(2)

    with col1:
        st.text_input(
            "Model Name",
            key="looker_model_name",
            help="The name of the LookML Model containing the Looker Explore you would like to replicate in Cortex Analyst.",
        )

        st.text_input(
            "Explore Name",
            key="looker_explore_name",
            help="The name of the LookML Explore to replicate in Cortex Analyst.",
        )

        st.selectbox(
            label="Looker View Database",
            index=None,
            options=available_databases,
            placeholder="Optional",
            help="Specify Snowflake database if your Looker Views do not explicitly reference Snowflake databases and instead rely on [Looker database connection](https://cloud.google.com/looker/docs/db-config-snowflake#creating-the-connection-to-your-database).",
            key="looker_connection_db",
        )

    with col2:
        st.text_input(
            "Looker SDK Base URL",
            key="looker_base_url",
            help="Looker Base URL also known as [API Host URL](https://cloud.google.com/looker/docs/admin-panel-platform-api#api_host_url).",
        )

        st.text_input(
            "Looker SDK Client ID",
            key="looker_client_id",
            help="See [Looker Authentication with an SDK](https://cloud.google.com/looker/docs/api-auth#authentication_with_an_sdk) for API credentials generation.",
        )

        st.text_input(
            "Looker SDK Client Secret",
            key="looker_client_secret",
            help="""See [Looker Authentication with an SDK](https://cloud.google.com/looker/docs/api-auth#authentication_with_an_sdk) for API credentials generation.
            Note that the client secret must be passed through external access integration secret if using Streamlit in Snowflake setup.""",
            type="password",
            disabled=st.session_state.get(
                "sis", False
            ),  # Requires external access in SiS setup
        )

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.write(
            """
            Please pick a Snowflake destination for the table.
            """
        )
    with col2:
        dynamic_table = st.checkbox(
            "Dynamic Table",
            key="dynamic",
            value=False,
        )
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
        target_lag: int = st.selectbox(  # type: ignore
            "Dynamic Table Target Lag",
            list(range(1, 41)),
            index=0,
            key="target_lag",
            disabled=not dynamic_table,
            help="Specifies the maximum amount of time that the dynamic table’s content should lag behind updates to the base tables.",
        )
        target_lag_unit: str = st.selectbox(  # type: ignore
            "Target Lag Unit",
            ["seconds", "minutes", "hours", "days"],
            index=1,
            key="target_lag_unit",
            disabled=not dynamic_table,
            help="Specifies the maximum amount of time that the dynamic table’s content should lag behind updates to the base tables.",
        )
        dynamic_warehouse: str = st.selectbox(  # type: ignore
            "Warehouse",
            available_warehouses,
            index=None,
            key="dynamic_warehouse",
            disabled=not dynamic_table,
            help="Specifies the name of the warehouse that provides the compute resources for refreshing the dynamic table.",
        )
    st.divider()
    st.write(
        "Please fill out the following fields to start building your semantic model."
    )
    col1, col2 = st.columns(2)
    with col1:
        model_name = input_semantic_file_name()
    with col2:
        sample_values = input_sample_value_num()

    experimental_features = st.checkbox(
        "Enable joins (optional)",
        help="Checking this box will enable you to add/edit join paths in your semantic model. If enabling this setting, please ensure that you have the proper parameters set on your Snowflake account. Reach out to your account team for access.",
    )

    if st.button("Continue", type="primary"):
        st.session_state["experimental_features"] = experimental_features

        if check_valid_session_state_values(
            [
                "looker_model_name",
                "looker_explore_name",
                "looker_base_url",
                "looker_client_id",
                "looker_target_database",
                "looker_target_schema",
                "looker_target_table_name",
            ]
        ):

            with st.spinner("Saving Explore as a Snowflake table..."):

                # Cortex model generation reqires fully-qualified table name
                full_tablename = f"{st.session_state['looker_target_schema']}.{st.session_state['looker_target_table_name']}".upper()

                looker_columns = render_looker_explore_as_table(
                    conn=get_snowflake_connection(),
                    model_name=st.session_state["looker_model_name"].lower(),
                    explore_name=st.session_state["looker_explore_name"].lower(),
                    table_name=full_tablename,
                    optional_db=st.session_state["looker_connection_db"],
                    fields=None,  # TO DO - Add support for field selection
                    dynamic=dynamic_table,
                    target_lag=target_lag,
                    target_lag_unit=target_lag_unit,
                    warehouse=dynamic_warehouse,
                )
                st.session_state["looker_field_metadata"] = looker_columns
            if st.session_state[
                "looker_field_metadata"
            ]:  # Create view only if full rendering is successful
                try:
                    run_generate_model_str_from_snowflake(
                        model_name,
                        sample_values,
                        [full_tablename],
                        allow_joins=experimental_features,
                    )
                except ValueError as e:
                    st.error(e)
                    st.stop()
                st.session_state["partner_setup"] = True

                # Tag looker setup with SIT query tag
                set_sit_query_tag(
                    get_snowflake_connection(),
                    vendor="looker",
                    action="setup_complete",
                )
                # Tag table with SIT query tag
                # We set table comment after generating semantic file to avoid using comment in description generation
                set_table_comment(
                    get_snowflake_connection(),
                    full_tablename,
                    get_sit_query_tag(vendor="looker", action="materialize"),
                    table_type="DYNAMIC" if dynamic_table else None,
                )

                # Take user to iteration using generated semantic file.
                # User can merge Looker metadata with generated semantic file on iteration page.
                st.session_state["page"] = GeneratorAppScreen.ITERATION
                st.rerun()


def set_looker_config() -> looker_sdk.sdk.api40.methods.Looker40SDK:
    """
    Sets Looker SDK connection
    """

    try:
        import looker_sdk
    except ImportError:
        raise ImportError(
            "The looker extra is required. You can install it using pip:\n\n"
            "pip install -e '.[looker]'\n"
        )

    os.environ["LOOKERSDK_BASE_URL"] = st.session_state[
        "looker_base_url"
    ]  # If your looker URL has .cloud in it (hosted on GCP), do not include :19999 (ie: https://your.cloud.looker.com).
    os.environ["LOOKERSDK_API_VERSION"] = (
        "4.0"  # As of Looker v23.18+, the 3.0 and 3.1 versions of the API are removed. Use "4.0" here.
    )
    os.environ["LOOKERSDK_VERIFY_SSL"] = (
        "true"
        # Defaults to true if not set. SSL verification should generally be on unless you have a real good reason not to use it. Valid options: true, y, t, yes, 1.
    )
    os.environ["LOOKERSDK_TIMEOUT"] = (
        "120"  # Seconds till request timeout. Standard default is 120.
    )

    # Get the following values from your Users page in the Admin panel of your Looker instance > Users > Your user > Edit API keys. If you know your user id, you can visit https://your.looker.com/admin/users/<your_user_id>/edit.
    os.environ["LOOKERSDK_CLIENT_ID"] = st.session_state["looker_client_id"]
    # User enters client secret in streamlit app in local run
    # In SiS setup, it must be passed through external access integration secret
    if st.session_state.get("sis", False):
        import _snowflake

        # Use the _snowflake library to access secrets
        os.environ["LOOKERSDK_CLIENT_SECRET"] = _snowflake.get_generic_secret_string(
            "looker_client_secret"
        )
    else:
        os.environ["LOOKERSDK_CLIENT_SECRET"] = st.session_state["looker_client_secret"]

    sdk = looker_sdk.init40()
    return sdk


def get_explore_fields(
    sdk: looker_sdk.sdk.api40.methods.Looker40SDK,
    model_name: str,
    explore_name: str,
    fields: Optional[str] = None,
) -> dict[str, dict[str, str]]:
    """
    Retrieves dimensions and measure fields from Looker Explore.
    Args:
        sdk (looker_sdk.sdk.api40.methods.Looker40SDK): Looker connection
        model_name (str): Looker model name. Should be lowercase.
        explore_name (str): Looker explore name. Should be lowercase.
        fields (str): List-like str of fields to extract from the Explore. Default is None.
                      Example: "id, name, description, fields",

    Returns: dict[str, dict[str, str]] column metadata for the Explore
    """

    field_keys = ["dimensions", "measures"]
    extracted_fields = {}

    if fields:
        response = sdk.lookml_model_explore(
            lookml_model_name=model_name,
            explore_name=explore_name,
            fields=fields,  # example "id, name, description, fields",
        )
    else:
        response = sdk.lookml_model_explore(  # type: ignore
            lookml_model_name=model_name, explore_name=explore_name
        )

    # Extract dimensions and measures from the response fields
    # Only need field name, tags, and descriptions
    for k in field_keys:
        if k in response.fields:  # type: ignore
            for field in response.fields[k]:  # type: ignore
                extracted_fields[field["name"]] = {
                    "description": field["description"],
                    "tags": field["tags"],
                }

    return extracted_fields


def create_query_id(
    sdk: looker_sdk.sdk.api40.methods.Looker40SDK,
    model_name: str,
    explore_name: str,
    fields: Optional[list[str]] = None,
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
        body=models.WriteQuery(model=model_name, view=explore_name, fields=fields)
    )

    id: str = query.id  # type: ignore

    return id


def create_explore_ctas(
    query_string: str,
    table_name: str,
    column_list: list[str],
    dynamic: Optional[bool] = False,
    target_lag: Optional[int] = 20,
    target_lag_unit: Optional[str] = "minutes",
    warehouse: Optional[str] = None,
) -> str:
    """
    Augments Looker Explore SQL with CTAS prefix to create a materialized view.
    Args:
        query_string (str): SQL select statement to render Looker Explore.
        table_name (str): Fully-qualified name of table to create in Snowflake.
        column_list (list[str]): List of column names to create in Snowflake table.
        dynamic (bool): Flag to create dynamic table. Default is False.
        target_lag (int): Target lag for dynamic table. Default is 20.
        target_lag_unit (str): Target lag unit for dynamic table. Default is "minutes".
        warehouse (str): Snowflake warehouse for dynamic table. Default is None.

    Returns: str CTAS statement
    """

    # Remove unnecessary lines of sql
    lines = query_string.split("\n")
    filtered_lines = [
        line for line in lines if not line.strip().startswith(("LIMIT", "FETCH"))
    ]
    # full_table_name = f"{snowflake_context}.{table_name}"
    filtered_query_string = "\n".join(filtered_lines)
    columns = ", ".join(column_list)

    if dynamic:
        if not [x for x in (target_lag, target_lag_unit, warehouse) if x is None]:
            ctas_clause = f"""
            CREATE OR REPLACE DYNAMIC TABLE {table_name}({columns})
            TARGET_LAG = '{target_lag} {target_lag_unit}'
            WAREHOUSE = {warehouse}
            """
        else:
            st.warning(
                """
                       Dynamic materialization requires target_lag, target_lag_unit, and warehouse.
                       Standard materialization will be used.
                       """
            )
            ctas_clause = f"CREATE OR REPLACE TABLE {table_name}({columns}) "
    else:
        ctas_clause = f"CREATE OR REPLACE TABLE {table_name}({columns}) "

    ctas_clause += " AS\n" + filtered_query_string
    return ctas_clause


def get_explore_sql(
    sdk: looker_sdk.sdk.api40.methods.Looker40SDK, query_id: str
) -> str:
    """
    Retrieves Looker SQL query of query ID
    Args:
        sdk (looker_sdk.sdk.api40.methods.Looker40SDK): Looker connection
        query_id (str): Query ID

    Returns: str SELECT statement
    """

    response: str = sdk.run_query(  # type: ignore
        query_id=query_id, result_format="sql"
    )
    return response


def prep_column_names(columns: list[str]) -> list[str]:
    """
    Prepares column names for Snowflake table creation
    Args:
        columns: List of column names

    Returns: a list of column names
    """

    new_cols = []
    for col in columns:
        if "." in col:
            new_col = col.split(".")[-1].upper()
        else:
            new_col = col
        new_cols.append(new_col)
    return new_cols


def render_looker_explore_as_table(
    conn: SnowflakeConnection,
    model_name: str,
    explore_name: str,
    table_name: str,
    optional_db: Optional[str] = None,
    fields: Optional[str] = None,
    dynamic: Optional[bool] = False,
    target_lag: Optional[int] = 20,
    target_lag_unit: Optional[str] = "minutes",
    warehouse: Optional[str] = None,
) -> Union[None, dict[str, dict[str, str]]]:
    """
    Creates materialized table corresponding to Looker Explore.
    Args:
        conn: SnowflakeConnection to run the query
        model_name (str): Looker model name. Should be lowercase.
        explore_name (str): Looker explore name. Should be lowercase.
        table_name: Fully-qualified name of table to create in Snowflake.
        fields (list): List-like str of fields to extract from the Explore. Default is None.
                       Example: "id, name, description, fields",
        dynamic (bool): Flag to create dynamic table. Default is False.
        target_lag (int): Target lag for dynamic table. Default is 20.
        target_lag_unit (str): Target lag unit for dynamic table. Default is "minutes".
        warehouse (str): Snowflake warehouse for dynamic table. Default is None.

    Returns: dict[str, dict[str, str]] column metadata
    """

    sdk = set_looker_config()

    # Get fields from explore
    try:
        field_metadata = get_explore_fields(sdk, model_name, explore_name, fields)
        if field_metadata:
            metadata_fields = list(field_metadata.keys())
            clean_columns = prep_column_names(metadata_fields)
        else:
            metadata_fields = None
    except Exception as e:
        st.error(f"Error fetching Looker Explore fields: {e}")
        return None

    # Get query to define materialized view
    try:
        query_id = create_query_id(sdk, model_name, explore_name, metadata_fields)
        explore_sql = get_explore_sql(sdk, query_id)
    except Exception as e:
        st.error(f"Error fetching Looker Explore SQL: {e}")
        return None
    ctas = create_explore_ctas(
        explore_sql,
        table_name,
        clean_columns,
        dynamic,
        target_lag,
        target_lag_unit,
        warehouse,
    )

    # Create materialized equivalent of Explore
    # Looker sources don't require explicit database qualification but instead use connection database implicitly.
    # Set optional_db if user provides it. May need to reset back to original afterwards.
    if optional_db:
        current_db = conn.cursor().execute("SELECT CURRENT_DATABASE();").fetchone()[0]  # type: ignore
        conn.cursor().execute(f"USE DATABASE {optional_db};")
        conn.cursor().execute(ctas)
        if (
            current_db
        ):  # Original connection does not have a database. No programmatic way to reset to None.
            conn.cursor().execute(f"USE DATABASE {current_db};")
    else:
        conn.cursor().execute(ctas)

    # Associate new column names with looker field metadata
    column_metadata = dict(zip(clean_columns, list(field_metadata.values())))
    return column_metadata


class LookerDimension(CortexDimension):
    """
    Class for Looker dimension-type field.
    """

    def __init__(self, data: dict[str, Any]):
        super().__init__(data)
        if self.get_name() in st.session_state["looker_field_metadata"]:
            description = st.session_state["looker_field_metadata"][
                self.get_name()
            ].get("description", None)
        self.description = description

    def get_cortex_comparison_dict(self) -> dict[str, Any]:
        cortex_details = self.get_cortex_details()
        cortex_details["description"] = self.description

        return {
            "field_key": self.get_key(),
            "section": self.get_cortex_section(),
            "field_details": cortex_details,
        }


class LookerMeasure(CortexMeasure):
    """
    Class for Looker measure-type field.
    """

    def __init__(self, data: dict[str, Any]):
        super().__init__(data)
        if self.get_name() in st.session_state["looker_field_metadata"]:
            description = st.session_state["looker_field_metadata"][
                self.get_name()
            ].get("description", None)
        self.description = description

    def get_cortex_comparison_dict(self) -> dict[str, Any]:
        cortex_details = self.get_cortex_details()
        cortex_details["description"] = self.description

        return {
            "field_key": self.get_key(),
            "section": self.get_cortex_section(),
            "field_details": cortex_details,
        }


class LookerTimeDimension(CortexTimeDimension):
    """
    Class for Looker time dimension-type field.
    """

    def __init__(self, data: dict[str, Any]):
        super().__init__(data)
        if self.get_name() in st.session_state["looker_field_metadata"]:
            description = st.session_state["looker_field_metadata"][
                self.get_name()
            ].get("description", None)
        self.description = description

    def get_cortex_comparison_dict(self) -> dict[str, Any]:
        cortex_details = self.get_cortex_details()
        cortex_details["description"] = self.description

        return {
            "field_key": self.get_key(),
            "section": self.get_cortex_section(),
            "field_details": cortex_details,
        }


class LookerSemanticTable(CortexSemanticTable):
    """
    Class for single Looker logical table in semantic file.

    The generated Cortex semantic file is used as a basis for the Looker model
    with minor adjustments for Looker-specific metadata
    """

    def get_cortex_fields(self) -> list[dict[str, Any]]:
        """
        Processes and returns raw field data as vendor-specific field objects.
        """

        cortex_fields = []
        if self.dimensions:
            for dimension in self.dimensions:
                cortex_fields.append(
                    LookerDimension(dimension).get_cortex_comparison_dict()
                )
        if self.time_dimensions:
            for time_dimension in self.time_dimensions:
                cortex_fields.append(
                    LookerTimeDimension(time_dimension).get_cortex_comparison_dict()
                )
        if self.measures:
            for measure in self.measures:
                cortex_fields.append(
                    LookerMeasure(measure).get_cortex_comparison_dict()
                )

        return cortex_fields

    @staticmethod
    def create_cortex_table_list() -> None:
        cortex_semantic = proto_to_dict(st.session_state["semantic_model"])
        tables = []
        for table in cortex_semantic["tables"]:
            tables.append(LookerSemanticTable(table))
        st.session_state["partner_semantic"] = tables

    @staticmethod
    def retrieve_df_by_name(name: str) -> pd.DataFrame:
        for table in st.session_state["partner_semantic"]:
            if table.get_name() == name:
                return table.create_comparison_df()
