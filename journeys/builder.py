from dataclasses import dataclass

import streamlit as st
from loguru import logger

from semantic_model_generator.data_processing import proto_utils
from semantic_model_generator.generate_model import append_comment_to_placeholders
from semantic_model_generator.generate_model import comment_out_section
from semantic_model_generator.protos import semantic_model_pb2
from snowflake.connector import ProgrammingError
from streamlit_extras.tags import tagger_component

from app_utils.shared_utils import (
    GeneratorAppScreen,
    format_snowflake_context,
    get_available_databases,
    get_available_schemas,
    get_available_tables,
    input_semantic_file_name,
)
from app_utils.shared_utils import get_available_warehouses
from app_utils.shared_utils import get_snowflake_connection
from semantic_model_generator.data_processing.data_types import CortexSearchService
from semantic_model_generator.generate_model import _get_placeholder_joins
from semantic_model_generator.generate_model import _raw_table_to_semantic_context_table
from semantic_model_generator.snowflake_utils.snowflake_connector import DIMENSION_DATATYPES
from semantic_model_generator.snowflake_utils.snowflake_connector import get_table_representation
from semantic_model_generator.snowflake_utils.snowflake_connector import get_valid_schemas_tables_columns_df
from semantic_model_generator.validate.context_length import validate_context_length


@dataclass(frozen=True)
class CortexSearchConfig:
    service_name: str
    column_name: str
    table_fqn: str
    warehouse_name: str
    target_lag: str


def init_session_state() -> None:
    default_state = {
        "cortex_search_configs": dict(),
        "enable_joins": False,
        "selected_databases": list(),
        "selected_schemas": list(),
        "selected_tables": list(),
        "build_semantic_model": False,
        "table_selector_submitted": False,
    }
    for key, value in default_state.items():
        if key not in st.session_state:
            st.session_state[key] = value


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


@st.experimental_fragment()
def table_selector_fragment() -> None:
    st.markdown(
        "## Please fill out the following fields to start building your semantic model."
    )
    _ = input_semantic_file_name()
    st.markdown("")

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
        format_func=lambda x: format_snowflake_context(x, -1),
    )

    st.multiselect(
        label="Tables",
        options=st.session_state.get("available_tables", []),
        placeholder="Select the tables you'd like to include in your semantic model.",
        key="selected_tables",
        format_func=lambda x: format_snowflake_context(x, -1),
    )

    st.checkbox(
        label="Enable joins (optional)",
        help=(
            "Checking this box will enable you to add/edit join paths in your semantic "
            "model. If enabling this setting, please ensure that you have the proper "
            "parameters set on your Snowflake account. Reach out to your account team "
            "for access."
        ),
        key="experimental_features",
    )

    if st.button(
        "Create Semantic Model Skeleton",
        key="table_selector_button",
        use_container_width=True,
        type="primary",
        disabled=st.session_state["table_selector_submitted"],
    ):
        if not st.session_state["semantic_model_name"]:
            st.error("Please provide a name for your semantic model.")
        elif not st.session_state["selected_tables"]:
            st.error("Please select at least one table to proceed.")
        else:
            st.session_state["table_selector_submitted"] = True

        st.rerun()


@st.cache_resource(show_spinner=False)
def generate_table_configs() -> None:
    with st.spinner(
        "Writing Semantic Model Descriptions Using AI (this may take a moment) ..."
    ):
        tables = []
        conn = get_snowflake_connection()
        for table_fqn in st.session_state.get("selected_tables", []):
            columns_df = get_valid_schemas_tables_columns_df(
                conn=conn,
                table_fqn=table_fqn,
            )
            table = get_table_representation(
                session=st.session_state["session"],
                table_fqn=table_fqn,
                max_string_sample_values=16,
                columns_df=columns_df,
                max_workers=1,
            )
            tables.append(table)

        # We will need warehouses for search integration.
        st.session_state["available_warehouses"] = get_available_warehouses()
        st.session_state["tables"] = tables


@st.experimental_fragment()
def cortex_search_fragment() -> None:
    tables = st.session_state["tables"]

    # Let's use a form instead of a button
    possible_columns = list()
    for table in tables:
        for column in table.columns:
            if (column.column_type in DIMENSION_DATATYPES) and (column.values is None):
                table_column = f"{table.name}.{column.column_name}"
                possible_columns.append(table_column)

    cortex_search_configs = st.session_state["cortex_search_configs"]

    def _add_search_form(column: str) -> None:
        with st.form(key=f"add_cortex_search_form_{column}"):
            st.write("Add Cortex Search Integration")
            service_name = st.text_input(
                label=f"Cortex Search Service Name",
                value=f"CORTEX_SEARCH.{column}",
                key=f"cortex_search_service_name_{column}",
            )
            warehouse_name = st.selectbox(
                label="Warehouse",
                options=get_available_warehouses(),
                key=f"cortex_search_warehouse_name_{column}",
            )
            target_lag = st.text_input(
                label="Target Lag",
                value="1 hour",
                key=f"cortex_search_target_lag_{column}",
            )

            if st.form_submit_button(label="Add Cortex Search Integration"):
                table_name, column_name = column.rsplit(".", 1)
                cortex_search_config = CortexSearchConfig(
                    service_name=service_name,
                    column_name=column_name,
                    table_fqn=table_name,
                    warehouse_name=warehouse_name,
                    target_lag=target_lag,
                )
                cortex_search_configs[column] = cortex_search_config
                st.rerun()


    def _remove_search_form(column: str) -> None:
        with st.form(key=f"remove_cortex_search_form_{column}"):
            st.write("Remove Cortex Search Integration")
            if st.form_submit_button(label="Remove Cortex Search Integration"):
                del cortex_search_configs[column]
                st.rerun()

    st.markdown("## Optionally Integrate Cortex Search For High Cardinality Dimensions")
    if len(cortex_search_configs) > 0:
        tagger_component(
            content="Columns with Search Integrations: ",
            tags=list(cortex_search_configs),
        )
    else:
        st.write("Columns with Search Integrations: None")

    current_column = st.selectbox(
        label="Table Column To Setup Cortex Search",
        options=possible_columns,
        key="cortex_search_possible_columns",
    )
    if current_column not in cortex_search_configs:
        _add_search_form(current_column)
    else:
        _remove_search_form(current_column)

    if st.button(
        "Build Semantic Model",
        key="setup_table_button",
        use_container_width=True,
        type="primary",
        disabled=st.session_state["build_semantic_model"],
    ):
        st.session_state["build_semantic_model"] = True

        # Add Cortex Search Configs
        for table in tables:
            for column in table.columns:
                search_key = f"{table.name}.{column.column_name}"
                if search_key in cortex_search_configs:
                    config: CortexSearchConfig = cortex_search_configs[search_key]
                    database, schema, _ = config.table_fqn.split(".")
                    column.cortex_search_service = CortexSearchService(
                        database=database,
                        schema=schema,
                        service=config.service_name,
                        literal_column=config.column_name,
                    )

        # TODO(kschmaus) create cortex search instances, use spinner ...

        st.rerun()
        # TODO(kschmaus): this should probably it's own function ...

        # TODO(kschmaus): at a certain point this shouldn't live in builder ...
        placeholder_relationships = (
            _get_placeholder_joins()
            if st.session_state["enable_joins"]
            else None
        )
        table_objects = [
            _raw_table_to_semantic_context_table(raw_table=table) for table in tables
        ]
        context = semantic_model_pb2.SemanticModel(
            name=st.session_state["semantic_model_name"],
            tables=table_objects,
            relationships=placeholder_relationships,
        )
        # Validate the generated yaml is within context limits.
        # We just throw a warning here to allow users to update.
        validate_context_length(context)

        yaml_str = proto_utils.proto_to_yaml(context)
        # Once we have the yaml, update to include to # <FILL-OUT> tokens.
        yaml_str = append_comment_to_placeholders(yaml_str)
        # Comment out the filters section as we don't have a way to auto-generate these yet.
        yaml_str = comment_out_section(yaml_str, "filters")
        yaml_str = comment_out_section(yaml_str, "relationships")
        st.session_state["yaml"] = yaml_str

        st.code(st.session_state["yaml"])

        # TODO(kschmaus): this ends things
        st.session_state["page"] = GeneratorAppScreen.ITERATION
        st.rerun()


def show() -> None:
    # Initialize session state.
    init_session_state()

    # Prompt user for table selection.
    table_selector_fragment()

    # Stop app until user table selection is complete.
    if not st.session_state["table_selector_submitted"]:
        st.stop()

    # Fetch table config and generate descriptions.
    generate_table_configs()

    # Setup cortex search integration
    cortex_search_fragment()

    # Stop app until user submits semantic model config.
    if not st.session_state["build_semantic_model"]:
        st.stop()

    # TODO(kschmaus): final function?