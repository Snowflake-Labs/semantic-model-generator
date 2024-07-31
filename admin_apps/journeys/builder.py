import streamlit as st
from loguru import logger
from snowflake.connector import ProgrammingError

from admin_apps.shared_utils import GeneratorAppScreen, get_snowflake_connection
from semantic_model_generator.generate_model import generate_model_str_from_snowflake
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    fetch_databases,
    fetch_schemas_in_database,
    fetch_tables_views_in_schema,
)


@st.cache_resource(show_spinner=False)
def get_available_tables(schema: str) -> list[str]:
    """
    Simple wrapper around fetch_table_names to cache the results.

    Returns: list of fully qualified table names
    """

    return fetch_tables_views_in_schema(get_snowflake_connection(), schema)


@st.cache_resource(show_spinner=False)
def get_available_schemas(db: str) -> list[str]:
    """
    Simple wrapper around fetch_schemas to cache the results.

    Returns: list of schema names
    """

    return fetch_schemas_in_database(get_snowflake_connection(), db)


@st.cache_resource(show_spinner=False)
def get_available_databases() -> list[str]:
    """
    Simple wrapper around fetch_databases to cache the results.

    Returns: list of database names
    """

    return fetch_databases(get_snowflake_connection())


def update_schemas_and_tables() -> None:
    """
    Callback to run when the selected databases change. Ensures that if a database is deselected, the corresponding
    schemas and tables are also deselected.
    Returns: None

    """
    databases = st.session_state["selected_databases"]

    # Fetch the available schemas for the selected databases
    schemas = []
    for db in databases:
        try:
            schemas.extend(get_available_schemas(db))
        except ProgrammingError:
            logger.info(
                f"Insufficient permissions to read from database {db}, skipping"
            )

    st.session_state["available_schemas"] = schemas

    # Enforce that the previously selected schemas are still valid
    valid_selected_schemas = [
        schema for schema in st.session_state["selected_schemas"] if schema in schemas
    ]
    st.session_state["selected_schemas"] = valid_selected_schemas
    update_tables()


def update_tables() -> None:
    """
    Callback to run when the selected schemas change. Ensures that if a schema is deselected, the corresponding
    tables are also deselected.
    """
    schemas = st.session_state["selected_schemas"]

    # Fetch the available tables for the selected schemas
    tables = []
    for schema in schemas:
        try:
            tables.extend(get_available_tables(schema))
        except ProgrammingError:
            logger.info(
                f"Insufficient permissions to read from schema {schema}, skipping"
            )
    st.session_state["available_tables"] = tables

    # Enforce that the previously selected tables are still valid
    valid_selected_tables = [
        table for table in st.session_state["selected_tables"] if table in tables
    ]
    st.session_state["selected_tables"] = valid_selected_tables


@st.experimental_dialog("Selecting your tables", width="large")
def table_selector_dialog() -> None:
    st.write(
        "Please fill out the following fields to start building your semantic model."
    )
    model_name = st.text_input(
        "Semantic Model Name (no .yaml suffix)",
        help="The name of the semantic model you are creating. This is separate from the filename, which we will set later.",
    )
    sample_values = st.selectbox(
        "Maximum number of sample values per column",
        list(range(1, 40)),
        index=0,
        help="NOTE: For dimensions, time measures, and measures, we enforce a minimum of 25, 3, and 3 sample values respectively.",
    )
    st.markdown("")

    if "selected_databases" not in st.session_state:
        st.session_state["selected_databases"] = []

    if "selected_schemas" not in st.session_state:
        st.session_state["selected_schemas"] = []

    if "selected_tables" not in st.session_state:
        st.session_state["selected_tables"] = []

    with st.spinner("Loading databases..."):
        available_databases = get_available_databases()

    st.multiselect(
        label="Databases",
        options=available_databases,
        placeholder="Select the databases that contain the tables you'd like to include in your semantic model.",
        on_change=update_schemas_and_tables,
        key="selected_databases",
    )

    st.multiselect(
        label="Schemas",
        options=st.session_state.get("available_schemas", []),
        placeholder="Select the schemas that contain the tables you'd like to include in your semantic model.",
        on_change=update_tables,
        key="selected_schemas",
    )

    st.multiselect(
        label="Tables",
        options=st.session_state.get("available_tables", []),
        placeholder="Select the tables you'd like to include in your semantic model.",
        key="selected_tables",
    )

    st.markdown("<div style='margin: 240px;'></div>", unsafe_allow_html=True)
    submit = st.button("Submit", use_container_width=True, type="primary")
    if submit:
        if not model_name:
            st.error("Please provide a name for your semantic model.")
        elif not st.session_state["selected_tables"]:
            st.error("Please select at least one table to proceed.")
        else:
            with st.spinner("Generating model. This may take a minute or two..."):
                yaml_str = generate_model_str_from_snowflake(
                    base_tables=st.session_state["selected_tables"],
                    snowflake_account=st.session_state["account_name"],
                    semantic_model_name=model_name,
                    n_sample_values=sample_values,  # type: ignore
                    conn=get_snowflake_connection(),
                )

                st.session_state["yaml"] = yaml_str
                st.session_state["page"] = GeneratorAppScreen.ITERATION
                st.rerun()


def show() -> None:
    table_selector_dialog()
