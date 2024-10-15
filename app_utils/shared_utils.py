from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from io import StringIO
from typing import Any, Optional, List

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
from PIL import Image
from snowflake.connector import ProgrammingError
from snowflake.connector.connection import SnowflakeConnection

from semantic_model_generator.data_processing.proto_utils import (
    proto_to_yaml,
    yaml_to_semantic_model,
)
from semantic_model_generator.generate_model import (
    generate_model_str_from_snowflake,
    raw_schema_to_semantic_context,
)
from semantic_model_generator.protos import semantic_model_pb2
from semantic_model_generator.protos.semantic_model_pb2 import Dimension, Table
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    SnowflakeConnector,
    fetch_databases,
    fetch_schemas_in_database,
    fetch_tables_views_in_schema,
    fetch_warehouses,
    fetch_stages_in_schema,
    fetch_yaml_names_in_stage,
)

from semantic_model_generator.snowflake_utils.env_vars import (  # noqa: E402
    SNOWFLAKE_ACCOUNT_LOCATOR,
    SNOWFLAKE_HOST,
    SNOWFLAKE_USER,
    assert_required_env_vars,
)

SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT_LOCATOR", "")

# Add a logo on the top-left corner of the app
LOGO_URL_LARGE = "https://upload.wikimedia.org/wikipedia/commons/thumb/f/ff/Snowflake_Logo.svg/2560px-Snowflake_Logo.svg.png"
LOGO_URL_SMALL = (
    "https://logos-world.net/wp-content/uploads/2022/11/Snowflake-Symbol.png"
)


@st.cache_resource
def get_connector() -> SnowflakeConnector:
    """
    Instantiates a SnowflakeConnector using the provided credentials. This is mainly used to instantiate a
    SnowflakeConnection which can execute queries.
    Returns: SnowflakeConnector object
    """
    return SnowflakeConnector(
        account_name=SNOWFLAKE_ACCOUNT,
        max_workers=1,
    )


def set_streamlit_location() -> bool:
    """
    Sets sis in session_state to True if the streamlit app is in SiS.
    """
    HOME = os.getenv("HOME", None)
    if HOME == "/home/udf":
        sis = True
    else:
        sis = False
    return sis


@st.experimental_dialog(title="Setup")
def env_setup_popup(missing_env_vars: list[str]) -> None:
    """
    Renders a dialog box to prompt the user to set the required connection setup.
    Args:
        missing_env_vars: A list of missing environment variables.
    """
    formatted_missing_env_vars = "\n".join(f"- **{s}**" for s in missing_env_vars)
    st.markdown(
        f"""Oops! It looks like the following required environment variables are missing: \n{formatted_missing_env_vars}\n\n
Please follow the [setup instructions](https://github.com/Snowflake-Labs/semantic-model-generator?tab=readme-ov-file#setup) to properly configure your environment. Restart this app after you've set the required environment variables."""
    )
    st.stop()


@st.cache_resource(show_spinner=False)
def get_snowflake_connection() -> SnowflakeConnection:
    """
    Opens a general python connector connection to Snowflake.
    Marked with st.cache_resource in order to reuse this connection across the app.
    Returns: SnowflakeConnection
    """

    if st.session_state["sis"]:
        # Import SiS-required modules
        import sys
        from snowflake.snowpark.context import get_active_session

        # Non-Anaconda supported packages must be added to path to import from stage
        addl_modules = [
            "strictyaml.zip",
            "looker_sdk.zip",
        ]
        sys.path.extend(addl_modules)
        return get_active_session().connection
    else:
        # Rely on streamlit connection that is built on top of many ways to build snowflake connection
        try:
            return st.connection("snowflake").raw_connection
        except Exception:
            # Continue to support original implementation that relied on environment vars
            missing_env_vars = assert_required_env_vars()
            if missing_env_vars:
                env_setup_popup(missing_env_vars)
            else:
                return get_connector().open_connection(db_name="")


@st.cache_resource(show_spinner=False)
def set_snowpark_session(_conn: Optional[SnowflakeConnection] = None) -> None:
    """
    Creates a snowpark for python session.
    Marked with st.cache_resource in order to reuse this connection across the app.
    If the app is running in SiS, it will use the active session.
    If the app is running locally with a python connector connection is available, it will create a new session.
    Snowpark session necessary for upload/downloads in SiS.
    Returns: Snowpark session
    """

    if st.session_state["sis"]:
        from snowflake.snowpark.context import get_active_session

        session = get_active_session()
    else:
        session = Session.builder.configs({"connection": _conn}).create()
    st.session_state["session"] = session


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


@st.cache_resource(show_spinner=False)
def get_available_warehouses() -> list[str]:
    """
    Simple wrapper around fetch_warehouses to cache the results.

    Returns: list of warehouse names
    """

    return fetch_warehouses(get_snowflake_connection())


@st.cache_resource(show_spinner=False)
def get_available_stages(schema: str) -> List[str]:
    """
    Fetches the available stages from the Snowflake account.

    Returns:
        List[str]: A list of available stages.
    """
    return fetch_stages_in_schema(get_snowflake_connection(), schema)


def stage_selector_container() -> None | List[str]:
    """
    Common component that encapsulates db/schema/stage selection for the admin app.
    When a db/schema/stage is selected, it is saved to the session state for reading elsewhere.
    Returns: None
    """
    available_schemas = []
    available_stages = []

    # First, retrieve all databases that the user has access to.
    stage_database = st.selectbox(
        "Stage database",
        options=get_available_databases(),
        index=None,
        key="selected_iteration_database",
    )
    if stage_database:
        # When a valid database is selected, fetch the available schemas in that database.
        try:
            available_schemas = get_available_schemas(stage_database)
        except (ValueError, ProgrammingError):
            st.error("Insufficient permissions to read from the selected database.")
            st.stop()

    stage_schema = st.selectbox(
        "Stage schema",
        options=available_schemas,
        index=None,
        key="selected_iteration_schema",
        format_func=lambda x: format_snowflake_context(x, -1),
    )
    if stage_schema:
        # When a valid schema is selected, fetch the available stages in that schema.
        try:
            available_stages = get_available_stages(stage_schema)
        except (ValueError, ProgrammingError):
            st.error("Insufficient permissions to read from the selected schema.")
            st.stop()

    files = st.selectbox(
        "Stage name",
        options=available_stages,
        index=None,
        key="selected_iteration_stage",
        format_func=lambda x: format_snowflake_context(x, -1),
    )
    return files


@st.cache_resource(show_spinner=False)
def get_yamls_from_stage(stage: str, include_yml: bool = False) -> List[str]:
    """
    Fetches the YAML files from the specified stage.

    Args:
        stage (str): The name of the stage to fetch the YAML files from.
        include_yml: If True, will look for .yaml and .yml. If False, just .yaml. Defaults to False.

    Returns:
        List[str]: A list of YAML files in the specified stage.
    """
    return fetch_yaml_names_in_stage(get_snowflake_connection(), stage, include_yml)


def set_account_name(
    conn: SnowflakeConnection, SNOWFLAKE_ACCOUNT: Optional[str] = None
) -> None:
    """
    Sets account_name in st.session_state.
    Used to consolidate from various connection methods.
    """
    # SNOWFLAKE_ACCOUNT may be specified from user's environment variables
    # This will not be the case for connections.toml so need to set it ourselves
    if not SNOWFLAKE_ACCOUNT:
        SNOWFLAKE_ACCOUNT = (
            conn.cursor().execute("SELECT CURRENT_ACCOUNT()").fetchone()[0]
        )
    st.session_state["account_name"] = SNOWFLAKE_ACCOUNT


def set_host_name(
    conn: SnowflakeConnection, SNOWFLAKE_HOST: Optional[str] = None
) -> None:
    """
    Sets host_name in st.session_state.
    Used to consolidate from various connection methods.
    Value only necessary for open-source implementation.
    """
    if st.session_state["sis"]:
        st.session_state["host_name"] = ""
    else:
        # SNOWFLAKE_HOST may be specified from user's environment variables
        # This will not be the case for connections.toml so need to set it ourselves
        if not SNOWFLAKE_HOST:
            SNOWFLAKE_HOST = conn.host
        st.session_state["host_name"] = SNOWFLAKE_HOST


def set_user_name(
    conn: SnowflakeConnection, SNOWFLAKE_USER: Optional[str] = None
) -> None:
    """
    Sets user_name in st.session_state.
    Used to consolidate from various connection methods.
    """
    if st.session_state["sis"]:
        st.session_state["user_name"] = st.experimental_user.user_name
    # SNOWFLAKE_USER may be specified from user's environment variables
    # This will not be the case for connections.toml so need to set it ourselves
    if not SNOWFLAKE_USER:
        SNOWFLAKE_USER = conn.cursor().execute("SELECT CURRENT_USER()").fetchone()[0]
    st.session_state["user_name"] = SNOWFLAKE_USER


class GeneratorAppScreen(str, Enum):
    """
    Enum defining different pages in the app.
    There are two full page experiences - "onboarding" and "iteration", and the builder flow is simply a popup
    that leads into the iteration flow.
    """

    ONBOARDING = "onboarding"
    ITERATION = "iteration"


def return_home_button() -> None:
    if st.button("Return to Home"):
        st.session_state["page"] = GeneratorAppScreen.ONBOARDING
        # Reset environment variables related to the semantic model, so that builder/iteration flows can start fresh.
        if "semantic_model" in st.session_state:
            del st.session_state["semantic_model"]
        if "yaml" in st.session_state:
            del st.session_state["yaml"]
        if "snowflake_stage" in st.session_state:
            del st.session_state["snowflake_stage"]
        st.rerun()


def update_last_validated_model() -> None:
    """Whenever user validated, update the last_validated_model to track semantic_model,
    except for verified_queries field."""
    st.session_state.last_validated_model.CopyFrom(st.session_state.semantic_model)
    # Do not save verfieid_queries field for the latest validated.
    del st.session_state.last_validated_model.verified_queries[:]


def changed_from_last_validated_model() -> bool:
    """Compare the last validated model against latest semantic model,
    except for verified_queries field."""

    for field in st.session_state.semantic_model.DESCRIPTOR.fields:
        if field.name != "verified_queries":
            model_value = getattr(st.session_state.semantic_model, field.name)
            last_validated_value = getattr(
                st.session_state.last_validated_model, field.name
            )
            if model_value != last_validated_value:
                return True
    return False


def init_session_states() -> None:
    # semantic_model stores the proto of generated semantic model using app.
    if "semantic_model" not in st.session_state:
        st.session_state.semantic_model = semantic_model_pb2.SemanticModel()
    # validated stores the status if the generated yaml has ever been validated.
    if "validated" not in st.session_state:
        st.session_state.validated = None
    # last_validated_model stores the proto (without verfied queries) from last successful validation.
    if "last_validated_model" not in st.session_state:
        st.session_state.last_validated_model = semantic_model_pb2.SemanticModel()

    # Chat display settings.
    if "chat_debug" not in st.session_state:
        st.session_state.chat_debug = False
    if "multiturn" not in st.session_state:
        st.session_state.multiturn = False

    # initialize session states for the chat page.
    if "messages" not in st.session_state:
        # messages store all chat histories
        st.session_state.messages = []
        # suggestions store suggested questions (if reject to answer) generated by the api during chat.
        st.session_state.suggestions = []
        # active_suggestion stores the active suggestion selected by the user
        st.session_state.active_suggestion = None
        # indicates if the user is editing the generated SQL for the verified query.
        st.session_state.editing = False
        # indicates if the user has confirmed his/her edits for the verified query.
        st.session_state.confirmed_edits = False


@st.experimental_dialog("Edit Dimension")  # type: ignore[misc]
def edit_dimension(table_name: str, dim: semantic_model_pb2.Dimension) -> None:
    """
    Renders a dialog box to edit an existing dimension.
    """
    key_prefix = f"{table_name}-{dim.name}"
    dim.name = st.text_input("Name", dim.name, key=f"{key_prefix}-edit-dim-name")
    dim.expr = st.text_input(
        "SQL Expression", dim.expr, key=f"{key_prefix}-edit-dim-expr"
    )
    dim.description = st.text_area(
        "Description", dim.description, key=f"{key_prefix}-edit-dim-description"
    )
    # Allow users to edit synonyms through a data_editor.
    synonyms_df = st.data_editor(
        pd.DataFrame(list(dim.synonyms), columns=["Synonyms"]),
        num_rows="dynamic",
        key=f"{key_prefix}-edit-dim-synonyms",
    )
    # Store the current values in data_editor in the protobuf.
    del dim.synonyms[:]
    for _, row in synonyms_df.iterrows():
        if row["Synonyms"]:
            dim.synonyms.append(row["Synonyms"])

    # TODO(nsehrawat): Change to a select box with a list of all data types.
    dim.data_type = st.text_input(
        "Data type", dim.data_type, key=f"{key_prefix}-edit-dim-datatype"
    )
    dim.unique = st.checkbox(
        "Does it have unique values?",
        value=dim.unique,
        key=f"{key_prefix}-edit-dim-unique",
    )
    # Allow users to edit sample values through a data_editor.
    sample_values_df = st.data_editor(
        pd.DataFrame(list(dim.sample_values), columns=["Sample Values"]),
        num_rows="dynamic",
        key=f"{key_prefix}-edit-dim-sample-values",
    )
    # Store the current values in data_editor in the protobuf.
    del dim.sample_values[:]
    for _, row in sample_values_df.iterrows():
        if row["Sample Values"]:
            dim.sample_values.append(row["Sample Values"])

    if st.button("Save"):
        st.rerun()


@st.experimental_dialog("Add Dimension")  # type: ignore[misc]
def add_dimension(table: semantic_model_pb2.Table) -> None:
    """
    Renders a dialog box to add a new dimension.
    """
    dim = Dimension()
    dim.name = st.text_input("Name", key=f"{table.name}-add-dim-name")
    dim.expr = st.text_input("SQL Expression", key=f"{table.name}-add-dim-expr")
    dim.description = st.text_area(
        "Description", key=f"{table.name}-add-dim-description"
    )
    synonyms_df = st.data_editor(
        pd.DataFrame(list(dim.synonyms), columns=["Synonyms"]),
        num_rows="dynamic",
        key=f"{table.name}-add-dim-synonyms",
    )
    for _, row in synonyms_df.iterrows():
        if row["Synonyms"]:
            dim.synonyms.append(row["Synonyms"])

    dim.data_type = st.text_input("Data type", key=f"{table.name}-add-dim-datatype")
    dim.unique = st.checkbox(
        "Does it have unique values?", key=f"{table.name}-add-dim-unique"
    )
    sample_values_df = st.data_editor(
        pd.DataFrame(list(dim.sample_values), columns=["Sample Values"]),
        num_rows="dynamic",
        key=f"{table.name}-add-dim-sample-values",
    )
    del dim.sample_values[:]
    for _, row in sample_values_df.iterrows():
        if row["Sample Values"]:
            dim.sample_values.append(row["Sample Values"])

    if st.button("Add"):
        table.dimensions.append(dim)
        st.rerun()


@st.experimental_dialog("Edit Measure")  # type: ignore[misc]
def edit_measure(table_name: str, measure: semantic_model_pb2.Measure) -> None:
    """
    Renders a dialog box to edit an existing measure.
    """
    key_prefix = f"{table_name}-{measure.name}"
    measure.name = st.text_input(
        "Name", measure.name, key=f"{key_prefix}-edit-measure-name"
    )
    measure.expr = st.text_input(
        "SQL Expression", measure.expr, key=f"{key_prefix}-edit-measure-expr"
    )
    measure.description = st.text_area(
        "Description", measure.description, key=f"{key_prefix}-edit-measure-description"
    )
    synonyms_df = st.data_editor(
        pd.DataFrame(list(measure.synonyms), columns=["Synonyms"]),
        num_rows="dynamic",
        key=f"{key_prefix}-edit-measure-synonyms",
    )
    del measure.synonyms[:]
    for _, row in synonyms_df.iterrows():
        if row["Synonyms"]:
            measure.synonyms.append(row["Synonyms"])

    measure.data_type = st.text_input(
        "Data type", measure.data_type, key=f"{key_prefix}-edit-measure-data-type"
    )

    aggr_options = semantic_model_pb2.AggregationType.keys()
    # Replace the 'aggregation_type_unknown' string with an empty string for a better display of options.
    aggr_options[0] = ""
    default_aggregation_idx = next(
        (
            i
            for i, s in enumerate(semantic_model_pb2.AggregationType.values())
            if s == measure.default_aggregation
        ),
        0,
    )

    default_aggregation = st.selectbox(
        "Default Aggregation",
        aggr_options,
        index=default_aggregation_idx,
        key=f"{key_prefix}-edit-measure-default-aggregation",
    )
    if default_aggregation:
        try:
            measure.default_aggregation = semantic_model_pb2.AggregationType.Value(
                default_aggregation
            )  # type: ignore[assignment]
        except ValueError as e:
            st.error(f"Invalid default_aggregation: {e}")
    else:
        measure.default_aggregation = (
            semantic_model_pb2.AggregationType.aggregation_type_unknown
        )

    sample_values_df = st.data_editor(
        pd.DataFrame(list(measure.sample_values), columns=["Sample Values"]),
        num_rows="dynamic",
        key=f"{key_prefix}-edit-measure-sample-values",
    )
    del measure.sample_values[:]
    for _, row in sample_values_df.iterrows():
        if row["Sample Values"]:
            measure.sample_values.append(row["Sample Values"])

    if st.button("Save"):
        st.rerun()


@st.experimental_dialog("Add Measure")  # type: ignore[misc]
def add_measure(table: semantic_model_pb2.Table) -> None:
    """
    Renders a dialog box to add a new measure.
    """
    with st.form(key="add-measure"):
        measure = semantic_model_pb2.Measure()
        measure.name = st.text_input("Name", key=f"{table.name}-add-measure-name")
        measure.expr = st.text_input(
            "SQL Expression", key=f"{table.name}-add-measure-expr"
        )
        measure.description = st.text_area(
            "Description", key=f"{table.name}-add-measure-description"
        )
        synonyms_df = st.data_editor(
            pd.DataFrame(list(measure.synonyms), columns=["Synonyms"]),
            num_rows="dynamic",
            key=f"{table.name}-add-measure-synonyms",
        )
        del measure.synonyms[:]
        for _, row in synonyms_df.iterrows():
            if row["Synonyms"]:
                measure.synonyms.append(row["Synonyms"])

        measure.data_type = st.text_input(
            "Data type", key=f"{table.name}-add-measure-data-type"
        )
        aggr_options = semantic_model_pb2.AggregationType.keys()
        # Replace the 'aggregation_type_unknown' string with an empty string for a better display of options.
        aggr_options[0] = ""
        default_aggregation = st.selectbox(
            "Default Aggregation",
            aggr_options,
            key=f"{table.name}-edit-measure-default-aggregation",
        )
        if default_aggregation:
            try:
                measure.default_aggregation = semantic_model_pb2.AggregationType.Value(
                    default_aggregation
                )  # type: ignore[assignment]
            except ValueError as e:
                st.error(f"Invalid default_aggregation: {e}")

        sample_values_df = st.data_editor(
            pd.DataFrame(list(measure.sample_values), columns=["Sample Values"]),
            num_rows="dynamic",
            key=f"{table.name}-add-measure-sample-values",
        )
        del measure.sample_values[:]
        for _, row in sample_values_df.iterrows():
            if row["Sample Values"]:
                measure.sample_values.append(row["Sample Values"])

        add_button = st.form_submit_button("Add")

    if add_button:
        table.measures.append(measure)
        st.rerun()


@st.experimental_dialog("Edit Time Dimension")  # type: ignore[misc]
def edit_time_dimension(
    table_name: str, tdim: semantic_model_pb2.TimeDimension
) -> None:
    """
    Renders a dialog box to edit a time dimension.
    """
    key_prefix = f"{table_name}-{tdim.name}"
    tdim.name = st.text_input("Name", tdim.name, key=f"{key_prefix}-edit-tdim-name")
    tdim.expr = st.text_input(
        "SQL Expression", tdim.expr, key=f"{key_prefix}-edit-tdim-expr"
    )
    tdim.description = st.text_area(
        "Description",
        tdim.description,
        key=f"{key_prefix}-edit-tdim-description",
    )
    synonyms_df = st.data_editor(
        pd.DataFrame(list(tdim.synonyms), columns=["Synonyms"]),
        num_rows="dynamic",
        key=f"{key_prefix}-tdim-edit-measure-synonyms",
    )
    del tdim.synonyms[:]
    for _, row in synonyms_df.iterrows():
        if row["Synonyms"]:
            tdim.synonyms.append(row["Synonyms"])

    tdim.data_type = st.text_input(
        "Data type", tdim.data_type, key=f"{key_prefix}-edit-tdim-datatype"
    )
    tdim.unique = st.checkbox("Does it have unique values?", value=tdim.unique)
    sample_values_df = st.data_editor(
        pd.DataFrame(list(tdim.sample_values), columns=["Sample Values"]),
        num_rows="dynamic",
        key=f"{key_prefix}-edit-tdim-sample-values",
    )
    del tdim.sample_values[:]
    for _, row in sample_values_df.iterrows():
        if row["Sample Values"]:
            tdim.sample_values.append(row["Sample Values"])

    if st.button("Save"):
        st.rerun()


@st.experimental_dialog("Add Time Dimension")  # type: ignore[misc]
def add_time_dimension(table: semantic_model_pb2.Table) -> None:
    """
    Renders a dialog box to add a new time dimension.
    """
    tdim = semantic_model_pb2.TimeDimension()
    tdim.name = st.text_input("Name", key=f"{table.name}-add-tdim-name")
    tdim.expr = st.text_input("SQL Expression", key=f"{table.name}-add-tdim-expr")
    tdim.description = st.text_area(
        "Description", key=f"{table.name}-add-tdim-description"
    )
    synonyms_df = st.data_editor(
        pd.DataFrame(list(tdim.synonyms), columns=["Synonyms"]),
        num_rows="dynamic",
        key=f"{table.name}-add-tdim-synonyms",
    )
    del tdim.synonyms[:]
    for _, row in synonyms_df.iterrows():
        if row["Synonyms"]:
            tdim.synonyms.append(row["Synonyms"])

    # TODO(nsehrawat): Change the set of allowed data types here.
    tdim.data_type = st.text_input("Data type", key=f"{table.name}-add-tdim-data-types")
    tdim.unique = st.checkbox(
        "Does it have unique values?", key=f"{table.name}-add-tdim-unique"
    )
    sample_values_df = st.data_editor(
        pd.DataFrame(list(tdim.sample_values), columns=["Sample Values"]),
        num_rows="dynamic",
        key=f"{table.name}-add-tdim-sample-values",
    )
    del tdim.sample_values[:]
    for _, row in sample_values_df.iterrows():
        if row["Sample Values"]:
            tdim.sample_values.append(row["Sample Values"])

    if st.button("Add", key=f"{table.name}-add-tdim-add"):
        table.time_dimensions.append(tdim)
        st.rerun()


def delete_dimension(table: semantic_model_pb2.Table, idx: int) -> None:
    """
    Inline deletes the dimension at a particular index in a Table protobuf.
    """
    if len(table.dimensions) < idx:
        return
    del table.dimensions[idx]


def delete_measure(table: semantic_model_pb2.Table, idx: int) -> None:
    """
    Inline deletes the measure at a particular index in a Table protobuf.
    """
    if len(table.measures) < idx:
        return
    del table.measures[idx]


def delete_time_dimension(table: semantic_model_pb2.Table, idx: int) -> None:
    """
    Inline deletes the time dimension at a particular index in a Table protobuf.
    """
    if len(table.time_dimensions) < idx:
        return
    del table.time_dimensions[idx]


def display_table(table_name: str) -> None:
    """
    Display all the data related to a logical table.
    """
    for t in st.session_state.semantic_model.tables:
        if t.name == table_name:
            table: semantic_model_pb2.Table = t
            break

    st.write("#### Table metadata")
    table.name = st.text_input("Table Name", table.name)
    fqn_columns = st.columns(3)
    with fqn_columns[0]:
        table.base_table.database = st.text_input(
            "Physical Database",
            table.base_table.database,
            key=f"{table_name}-base_database",
        )
    with fqn_columns[1]:
        table.base_table.schema = st.text_input(
            "Physical Schema",
            table.base_table.schema,
            key=f"{table_name}-base_schema",
        )
    with fqn_columns[2]:
        table.base_table.table = st.text_input(
            "Physical Table", table.base_table.table, key=f"{table_name}-base_table"
        )

    table.description = st.text_area(
        "Description", table.description, key=f"{table_name}-description"
    )

    synonyms_df = st.data_editor(
        pd.DataFrame(list(table.synonyms), columns=["Synonyms"]),
        num_rows="dynamic",
        key=f"{table_name}-synonyms",
        use_container_width=True,
    )
    del table.synonyms[:]
    for idx, row in synonyms_df.iterrows():
        if row["Synonyms"]:
            table.synonyms.append(row["Synonyms"])

    st.write("#### Dimensions")
    header = ["Name", "Expression", "Data Type"]
    header_cols = st.columns(len(header) + 1)
    for i, h in enumerate(header):
        header_cols[i].write(f"###### {h}")

    for idx, dim in enumerate(table.dimensions):
        cols = st.columns(len(header) + 1)
        cols[0].write(dim.name)
        cols[1].write(dim.expr)
        cols[2].write(dim.data_type)
        with cols[-1]:
            subcols = st.columns(2)
            if subcols[0].button(
                "Edit",
                key=f"{table_name}-edit-dimension-{idx}",
            ):
                edit_dimension(table_name, dim)
            subcols[1].button(
                "Delete",
                key=f"{table_name}-delete-dimension-{idx}",
                on_click=delete_dimension,
                args=(
                    table,
                    idx,
                ),
            )

    if st.button("Add Dimension", key=f"{table_name}-add-dimension"):
        add_dimension(table)

    st.write("#### Measures")
    header_cols = st.columns(len(header) + 1)
    for i, h in enumerate(header):
        header_cols[i].write(f"###### {h}")

    for idx, measure in enumerate(table.measures):
        cols = st.columns(len(header) + 1)
        cols[0].write(measure.name)
        cols[1].write(measure.expr)
        cols[2].write(measure.data_type)
        with cols[-1]:
            subcols = st.columns(2)
            if subcols[0].button("Edit", key=f"{table_name}-edit-measure-{idx}"):
                edit_measure(table_name, measure)
            subcols[1].button(
                "Delete",
                key=f"{table_name}-delete-measure-{idx}",
                on_click=delete_measure,
                args=(
                    table,
                    idx,
                ),
            )

    if st.button("Add Measure", key=f"{table_name}-add-measure"):
        add_measure(table)

    st.write("#### Time Dimensions")
    header_cols = st.columns(len(header) + 1)
    for i, h in enumerate(header):
        header_cols[i].write(f"###### {h}")

    for idx, tdim in enumerate(table.time_dimensions):
        cols = st.columns(len(header) + 1)
        cols[0].write(tdim.name)
        cols[1].write(tdim.expr)
        cols[2].write(tdim.data_type)
        with cols[-1]:
            subcols = st.columns(2)
            if subcols[0].button("Edit", key=f"{table_name}-edit-tdim-{idx}"):
                edit_time_dimension(table_name, tdim)
            subcols[1].button(
                "Delete",
                key=f"{table_name}-delete-tdim-{idx}",
                on_click=delete_time_dimension,
                args=(
                    table,
                    idx,
                ),
            )

    if st.button("Add Time Dimension", key=f"{table_name}-add-tdim"):
        add_time_dimension(table)


@st.experimental_dialog("Add Table")  # type: ignore[misc]
def add_new_table() -> None:
    """
    Renders a dialog box to add a new logical table.
    """
    table = Table()
    table.name = st.text_input("Table Name")
    for t in st.session_state.semantic_model.tables:
        if t.name == table.name:
            st.error(f"Table called '{table.name}' already exists")

    table.base_table.database = st.text_input("Physical Database")
    table.base_table.schema = st.text_input("Physical Schema")
    table.base_table.table = st.text_input("Physical Table")
    st.caption(":gray[Synonyms (hover the table to add new rows!)]")
    synonyms_df = st.data_editor(
        pd.DataFrame(columns=["Synonyms"]),
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
    )
    for _, row in synonyms_df.iterrows():
        if row["Synonyms"]:
            table.synonyms.append(row["Synonyms"])
    table.description = st.text_area("Description", key="add-new-table-description")
    if st.button("Add"):
        with st.spinner(text="Fetching table details from database ..."):
            try:
                semantic_model = raw_schema_to_semantic_context(
                    base_tables=[
                        f"{table.base_table.database}.{table.base_table.schema}.{table.base_table.table}"
                    ],
                    semantic_model_name="foo",  # A placeholder name that's not used anywhere.
                    conn=get_snowflake_connection(),
                )
            except Exception as ex:
                st.error(f"Error adding table: {ex}")
                return
            table.dimensions.extend(semantic_model.tables[0].dimensions)
            table.measures.extend(semantic_model.tables[0].measures)
            table.time_dimensions.extend(semantic_model.tables[0].time_dimensions)
            for t in st.session_state.semantic_model.tables:
                if t.name == table.name:
                    st.error(f"Table called '{table.name}' already exists")
                    return
        st.session_state.semantic_model.tables.append(table)
        st.rerun()


def display_semantic_model() -> None:
    """
    Renders the entire semantic model.
    """
    semantic_model = st.session_state.semantic_model
    with st.form(border=False, key="create"):
        name = st.text_input(
            "Name",
            semantic_model.name,
            placeholder="My semantic model",
        )

        description = st.text_area(
            "Description",
            semantic_model.description,
            key="display-semantic-model-description",
            placeholder="The model describes the data and metrics available for Foocorp",
        )

        left, right = st.columns((1, 4))
        if left.form_submit_button("Create", use_container_width=True):
            st.session_state.semantic_model.name = name
            st.session_state.semantic_model.description = description
            st.session_state["next_is_unlocked"] = True
            right.success("Successfully created model. Updating...")
            time.sleep(1.5)
            st.rerun()


def edit_semantic_model() -> None:
    st.write("#### Tables")
    for t in st.session_state.semantic_model.tables:
        with st.expander(t.name):
            display_table(t.name)
    if st.button("Add Table"):
        add_new_table()


def import_yaml() -> None:
    """
    Renders a page to import an existing yaml file.
    """
    uploaded_file = st.file_uploader(
        "Choose a semantic model YAML file",
        type=[".yaml", ".yml"],
        accept_multiple_files=False,
    )
    pb: Optional[semantic_model_pb2.SemanticModel] = None

    if uploaded_file is not None:
        try:
            yaml_str = StringIO(uploaded_file.getvalue().decode("utf-8")).read()
            pb = yaml_to_semantic_model(yaml_str)
        except Exception as ex:
            st.error(f"Failed to import: {ex}")
            return
        if pb is None:
            st.error("Failed to import, did you choose a file?")
            return

        st.session_state["semantic_model"] = pb
        st.success(f"Successfully imported **{pb.name}**!")
        st.session_state["next_is_unlocked"] = True
        if "yaml_just_imported" not in st.session_state:
            st.session_state["yaml_just_imported"] = True
            st.rerun()


@st.experimental_dialog("Model YAML", width="large")  # type: ignore
def show_yaml_in_dialog() -> None:
    yaml = proto_to_yaml(st.session_state.semantic_model)
    st.code(
        yaml,
        language="yaml",
        line_numbers=True,
    )


def upload_yaml(file_name: str) -> None:
    """util to upload the semantic model."""
    import os
    import tempfile

    yaml = proto_to_yaml(st.session_state.semantic_model)

    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_file_path = os.path.join(temp_dir, f"{file_name}.yaml")

        with open(tmp_file_path, "w") as temp_file:
            temp_file.write(yaml)

        st.session_state.session.file.put(
            tmp_file_path,
            f"@{st.session_state.snowflake_stage.stage_name}",
            auto_compress=False,
            overwrite=True,
        )


def validate_and_upload_tmp_yaml(conn: SnowflakeConnection) -> None:
    """
    Validate the semantic model.
    If successfully validated, upload a temp file into stage, to allow chatting and adding VQR against it.
    """
    from semantic_model_generator.validate_model import validate

    yaml_str = proto_to_yaml(st.session_state.semantic_model)
    try:
        # whenever valid, upload to temp stage path.
        validate(yaml_str, SNOWFLAKE_ACCOUNT, conn)
        # upload_yaml(_TMP_FILE_NAME)
        st.session_state.validated = True
        update_last_validated_model()
    except Exception as e:
        st.warning(f"Invalid YAML: {e} please fix!")

    st.success("Successfully validated your model!")
    st.session_state["next_is_unlocked"] = True


def semantic_model_exists() -> bool:
    if "semantic_model" in st.session_state:
        if hasattr(st.session_state.semantic_model, "name"):
            if isinstance(st.session_state.semantic_model.name, str):
                model_name: str = st.session_state.semantic_model.name.strip()
                return model_name != ""
    return False


def stage_exists() -> bool:
    return "snowflake_stage" in st.session_state


def model_is_validated() -> bool:
    if semantic_model_exists():
        return st.session_state.validated  # type: ignore
    return False


def download_yaml(file_name: str, stage_name: str) -> str:
    """util to download a semantic YAML from a stage."""
    import os
    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        # Downloads the YAML to {temp_dir}/{file_name}.
        st.session_state.session.file.get(
            f"@{stage_name}/{file_name}", temp_dir
        )

        tmp_file_path = os.path.join(temp_dir, f"{file_name}")
        with open(tmp_file_path, "r") as temp_file:
            # Read the raw contents from {temp_dir}/{file_name} and return it as a string.
            yaml_str = temp_file.read()
            return yaml_str


def get_sit_query_tag(
    vendor: Optional[str] = None, action: Optional[str] = None
) -> str:
    """
    Returns SIT query tag.
    Returns: str
    """

    query_tag = {
        "origin": "sf_sit",
        "name": "skimantics",
        "version": {"major": 1, "minor": 0},
        "attributes": {"vendor": vendor, "action": action},
    }
    return json.dumps(query_tag)


def set_sit_query_tag(
    conn: SnowflakeConnection,
    vendor: Optional[str] = None,
    action: Optional[str] = None,
) -> None:
    """
    Sets query tag on a single zero-compute ping for tracking.
    Only used if the app is running in the OSS environment.

    Returns: None
    """
    if not st.session_state["sis"]:
        query_tag = get_sit_query_tag(vendor, action)

        conn.cursor().execute(f"alter session set query_tag='{query_tag}'")
        conn.cursor().execute("SELECT 'SKIMANTICS';")
        conn.cursor().execute("alter session set query_tag=''")


def set_table_comment(
    conn: SnowflakeConnection,
    tablename: str,
    comment: str,
    table_type: Optional[str] = None,
) -> None:
    """
    Sets comment on provided table.
    Returns: None
    """
    if table_type is None:
        table_type = ""
    query = f"ALTER {table_type} TABLE {tablename} SET COMMENT = '{comment}'"
    conn.cursor().execute(query)


def render_image(image_file: str, size: tuple[int, int]) -> None:
    """
    Renders image in streamlit app with custom width x height by pixel.
    """
    image = Image.open(image_file)
    new_image = image.resize(size)
    st.image(new_image)


def format_snowflake_context(context: str, index: Optional[int] = None) -> str:
    """
    Extracts the desired part of the Snowflake context.
    """
    if index and "." in context:
        split_context = context.split(".")
        try:
            return split_context[index]
        except IndexError:  # Return final segment if mis-typed index
            return split_context[-1]
    else:
        return context


def check_valid_session_state_values(vars: list[str]) -> bool:
    """
    Returns False if any vars are not found or None in st.session_state.
    Args:
        vars (list[str]): List of variables to check in st.session_state

    Returns: bool
    """
    empty_vars = []
    for var in vars:
        if var not in st.session_state:
            empty_vars.append(var)
    if empty_vars:
        st.error(f"Please enter values for {vars}.")
        return False
    else:
        return True


def run_cortex_complete(
    conn: SnowflakeConnection,
    model: str,
    prompt: str,
    prompt_args: Optional[dict[str, Any]] = None,
) -> str | None:

    if prompt_args:
        prompt = prompt.format(**prompt_args).replace("'", "\\'")
    complete_sql = f"SELECT snowflake.cortex.complete('{model}', '{prompt}')"
    response = conn.cursor().execute(complete_sql)

    if response:
        output: str = response.fetchone()[0]  # type: ignore
        return output
    else:
        return None


def input_semantic_file_name() -> str:
    """
    Prompts the user to input the name of the semantic model they are creating.
    Returns:
        str: The name of the semantic model.
    """

    model_name = st.text_input(
        "Semantic Model Name (no .yaml suffix)",
        help="The name of the semantic model you are creating. This is separate from the filename, which we will set later.",
    )
    return model_name


def input_sample_value_num() -> int:
    """
    Function to prompt the user to input the maximum number of sample values per column.
    Returns:
        int: The maximum number of sample values per column.
    """

    sample_values: int = st.selectbox(  # type: ignore
        "Maximum number of sample values per column",
        list(range(1, 40)),
        index=2,
        help="Specifies the maximum number of distinct sample values we fetch for each column. We suggest keeping this number as low as possible to reduce latency.",
    )
    return sample_values


def run_generate_model_str_from_snowflake(
    model_name: str,
    sample_values: int,
    base_tables: list[str],
    allow_joins: Optional[bool] = False,
) -> None:
    """
    Runs generate_model_str_from_snowflake to generate cortex semantic shell.
    Args:
        model_name (str): Semantic file name (without .yaml suffix).
        sample_values (int): Number of sample values to provide for each table in generation.
        base_tables (list[str]): List of fully-qualified Snowflake tables to include in the semantic model.

    Returns: None
    """

    if not model_name:
        st.error("Please provide a name for your semantic model.")
    elif not base_tables:
        st.error("Please select at least one table to proceed.")
    else:
        with st.spinner("Generating model. This may take a minute or two..."):
            yaml_str = generate_model_str_from_snowflake(
                base_tables=base_tables,
                semantic_model_name=model_name,
                n_sample_values=sample_values,  # type: ignore
                conn=get_snowflake_connection(),
                allow_joins=allow_joins,
            )

            st.session_state["yaml"] = yaml_str


@dataclass
class AppMetadata:
    """
    Metadata about the active semantic model and environment variables
    being in used in the app session.
    """

    @property
    def user(self) -> str | None:
        return os.getenv("SNOWFLAKE_USER")

    @property
    def stage(self) -> str | None:
        if stage_exists():
            stage = st.session_state.snowflake_stage
            return f"{stage.stage_database}.{stage.stage_schema}.{stage.stage_name}"
        return None

    @property
    def model(self) -> str | None:
        if semantic_model_exists():
            return st.session_state.semantic_model.name  # type: ignore
        return None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "User": self.user,
            "Stage": self.stage,
            "Model": self.model,
        }

    def show_as_dataframe(self) -> None:
        data = self.to_dict()
        st.dataframe(
            data,
            column_config={"value": st.column_config.Column(label="Value")},
            use_container_width=True,
        )


@dataclass
class SnowflakeStage:
    stage_database: str
    stage_schema: str
    stage_name: str

    def to_dict(self) -> dict[str, str]:
        return {
            "Database": self.stage_database,
            "Schema": self.stage_schema,
            "Stage": self.stage_name,
        }
