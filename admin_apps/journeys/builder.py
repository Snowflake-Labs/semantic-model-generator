import streamlit as st
from loguru import logger
from snowflake.connector import ProgrammingError

from admin_apps.shared_utils import (
    GeneratorAppScreen,
    get_snowflake_connection,
    get_available_tables,
    get_available_schemas,
    get_available_databases,
    format_snowflake_context,
)
from semantic_model_generator.generate_model import generate_model_str_from_snowflake


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


@st.dialog("Selecting your tables", width="large")
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

    # Circumvent the table selection process if the user has already set up Looker
    # TO DO - Default the entire table selection context in the next logical block
    if (st.session_state.get("partner_setup", False) 
        and st.session_state.get("partner_tool", None) == 'looker'):
        # Dialog box resets prior dialog box widget key values so we need to set session state at first render only
        if ('looker_target_schema' in st.session_state) and ('looker_target_table_name' in st.session_state):
            st.session_state["selected_tables"] = [f"{st.session_state['looker_target_schema']}.{st.session_state['looker_target_table_name']}"]

        st.selectbox(
            label="Tables",
            options=st.session_state["selected_tables"],
            placeholder="Select the tables you'd like to include in your semantic model.",
            key="selected_tables",
            default = st.session_state["selected_tables"],
            disabled=True,
        )

    else:
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
            # default=st.session_state.get("selected_databases", []),
        )

        st.multiselect(
            label="Schemas",
            options=st.session_state.get("available_schemas", []),
            placeholder="Select the schemas that contain the tables you'd like to include in your semantic model.",
            on_change=update_tables,
            key="selected_schemas",
            format_func=lambda x: format_snowflake_context(x, -1),
        )

        st.multiselect(
            label="Tables",
            options=st.session_state.get("available_tables", []),
            placeholder="Select the tables you'd like to include in your semantic model.",
            key="selected_tables",
            format_func=lambda x: format_snowflake_context(x, -1),
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
