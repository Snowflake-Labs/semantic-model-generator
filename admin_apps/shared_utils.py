from __future__ import annotations

import json
import os
import time
from loguru import logger
from dataclasses import dataclass
from PIL import Image
from datetime import datetime
from enum import Enum
from io import StringIO
from typing import Any, Optional

import numpy as np
import pandas as pd
import streamlit as st
import yaml
from snowflake.connector import SnowflakeConnection, ProgrammingError

from semantic_model_generator.data_processing.proto_utils import (
    proto_to_dict,
    proto_to_yaml,
    yaml_to_semantic_model,
)
from semantic_model_generator.generate_model import raw_schema_to_semantic_context
from semantic_model_generator.protos import semantic_model_pb2
from semantic_model_generator.protos.semantic_model_pb2 import Dimension, Table
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    SnowflakeConnector,
    set_database,
    set_schema,
    fetch_databases,
    fetch_schemas_in_database,
    fetch_tables_views_in_schema,
)

from admin_apps.partner.looker import render_looker_explore_as_table

SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT_LOCATOR", "")
_TMP_FILE_NAME = f"admin_app_temp_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# Add a logo on the top-left corner of the app
LOGO_URL_LARGE = "https://upload.wikimedia.org/wikipedia/commons/thumb/f/ff/Snowflake_Logo.svg/2560px-Snowflake_Logo.svg.png"
LOGO_URL_SMALL = (
    "https://logos-world.net/wp-content/uploads/2022/11/Snowflake-Symbol.png"
)

# Partner semantic support instructions
DBT_IMAGE = 'admin_apps/images/dbt-signature_tm_black.png'
LOOKER_IMAGE = 'admin_apps/images/looker.png'
DBT_INSTRUCTIONS = """
We extract metadata from your **DBT** semantic yaml file(s) and merge it with a generated Cortex Analyst semantic file.

**Note**: The DBT semantic layer must be sourced from tables/views in Snowflake.
> Steps:
> 1) Upload your dbt semantic (yaml/yml) file(s) below. 
> 2) Select **🛠 Create a new semantic model** to generate a new Cortex Analyst semantic file for Snowflake tables or **✏️ Edit an existing semantic model**.
> 3) Validate the output in the UI.
> 4) Once you've validated the semantic file, click **Partner Semantic** to merge DBT and Cortex Analyst semantic files.  
"""
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


@st.cache_resource(show_spinner=False)
def get_snowflake_connection() -> SnowflakeConnection:
    """
    Opens a general connection to Snowflake using the provided SnowflakeConnector
    Marked with st.cache_resource in order to reuse this connection across the app.
    Returns: SnowflakeConnection
    """
    return get_connector().open_connection(db_name="")

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


class GeneratorAppScreen(str, Enum):
    """
    Enum defining different pages in the app.
    There are two full page experiences - "onboarding" and "iteration", and the builder flow is simply a popup
    that leads into the iteration flow.
    """

    ONBOARDING = "onboarding"
    ITERATION = "iteration"


def add_logo() -> None:
    st.logo(
        image=LOGO_URL_LARGE,
        link="https://www.snowflake.com/en/data-cloud/cortex/",
        icon_image=LOGO_URL_SMALL,
    )


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


@st.dialog("Edit Dimension")  # type: ignore[misc]
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


@st.dialog("Add Dimension")  # type: ignore[misc]
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


@st.dialog("Edit Measure")  # type: ignore[misc]
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


@st.dialog("Add Measure")  # type: ignore[misc]
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


@st.dialog("Edit Time Dimension")  # type: ignore[misc]
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


@st.dialog("Add Time Dimension")  # type: ignore[misc]
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


@st.dialog("Add Table")  # type: ignore[misc]
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
                    snowflake_account=SNOWFLAKE_ACCOUNT,
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

        left, right = st.columns((1, 4), vertical_alignment="center")
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


@st.dialog("Model YAML", width="large")  # type: ignore
def show_yaml_in_dialog() -> None:
    yaml = proto_to_yaml(st.session_state.semantic_model)
    st.code(
        yaml,
        language="yaml",
        line_numbers=True,
    )


def upload_yaml(file_name: str, conn: SnowflakeConnection) -> None:
    """util to upload the semantic model."""
    import os
    import tempfile

    yaml = proto_to_yaml(st.session_state.semantic_model)

    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_file_path = os.path.join(temp_dir, f"{file_name}.yaml")

        with open(tmp_file_path, "w") as temp_file:
            temp_file.write(yaml)

        set_database(conn, st.session_state.snowflake_stage.stage_database)
        set_schema(conn, st.session_state.snowflake_stage.stage_schema)
        upload_sql = f"PUT file://{tmp_file_path} @{st.session_state.snowflake_stage.stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        conn.cursor().execute(upload_sql)

        if file_name != _TMP_FILE_NAME:
            # If the user did official uploading, delete the saved temp file from stage.
            try:
                delete_tmp_sql = f"REMOVE @{st.session_state.snowflake_stage.stage_name}/{_TMP_FILE_NAME}.yaml"
                conn.cursor().execute(delete_tmp_sql)
            except Exception:
                pass


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


def get_environment_variables() -> dict[str, str | None]:
    import os

    return {
        key: os.getenv(key)
        for key in (
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_ROLE",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_HOST",
            "SNOWFLAKE_ACCOUNT_LOCATOR",
        )
    }


def environment_variables_exist() -> bool:
    snowflake_env = get_environment_variables()
    return all([env is not None for env in snowflake_env.values()])


def model_is_validated() -> bool:
    if semantic_model_exists():
        return st.session_state.validated  # type: ignore
    return False


def download_yaml(file_name: str, conn: SnowflakeConnection) -> str:
    """util to download a semantic YAML from a stage."""
    import os
    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        set_database(conn, st.session_state.snowflake_stage.stage_database)
        set_schema(conn, st.session_state.snowflake_stage.stage_schema)
        # Downloads the YAML to {temp_dir}/{file_name}.
        download_yaml_sql = f"GET @{st.session_state.snowflake_stage.stage_name}/{file_name} file://{temp_dir}"
        conn.cursor().execute(download_yaml_sql)

        tmp_file_path = os.path.join(temp_dir, f"{file_name}")
        with open(tmp_file_path, "r") as temp_file:
            # Read the raw contents from {temp_dir}/{file_name} and return it as a string.
            yaml_str = temp_file.read()
            return yaml_str


def unpack_yaml(
    data: Any | dict[str, Any] | list[Any]
) -> Any | dict[str, Any] | list[str]:
    """
    Recursively unpacks a YAML structure into a Python dictionary.
    """
    if isinstance(data, dict):
        return {key: unpack_yaml(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [unpack_yaml(item) for item in data]
    else:
        return data


def load_yaml_file(file_paths: list[Any]) -> list[Any]:  # type: ignore
    """
    Loads one or more YAML files and combines them into a single list.
    """
    combined_yaml = []
    for file_path in file_paths:
        yaml_content = yaml.safe_load(file_path)
        combined_yaml.append(unpack_yaml(yaml_content))
    return combined_yaml


def extract_key_values(data: list[dict[str, Any]], key: str) -> list[Any]:
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


def extract_expressions_from_sections(
    data_dict: dict[str, Any], section_names: list[str]
) -> dict[str, dict[str, Any]]:
    """
    Extracts data in section_names from a dictionary into a nested dictionary:
    """

    def extract_dbt_field_key(obj: dict[str, Any]) -> str | Any:
        return obj.get("expr", obj["name"]).lower()

    d = {}
    for i in section_names:
        if st.session_state["partner_tool"] == 'dbt':
            d[i] = {extract_dbt_field_key(obj): obj for obj in data_dict.get(i, [])}

    return d

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

def upload_dbt_semantic() -> None:
    """
    Upload semantic file(s) for dbt from local source.
    """
    uploaded_files = st.file_uploader(
            f'Upload {st.session_state["partner_tool"]} semantic yaml file(s)',
            type=["yaml", "yml"],
            accept_multiple_files=True,
            key="myfile",
        )
    if uploaded_files:
        partner_semantic = extract_key_values(
            load_yaml_file(uploaded_files), "semantic_models"
        )
        if not partner_semantic:
            st.error(
                "Upload file does not contain required semantic_models section."
            )
        else:
            st.session_state["partner_semantic"] = partner_semantic
            st.session_state["uploaded_semantic_files"] = [
                i.name for i in uploaded_files
            ]
            # Where logical fields are captured in semantic file
            st.session_state['field_section_names'] = ["dimensions", "measures", "entities"]
            # Field-level metadata common to both cortex and partner
            st.session_state['common_fields'] = ["name", "description"]
    else:
        st.session_state["partner_semantic"] = None

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
    )

    st.text_input(
        "Snowflake Table",
        key="looker_target_table_name",
        help="The name of the LookML Explore to replicate in Cortex Analyst.",
    )
    

def set_partner_instructions() -> None:
    """
    Sets instructions and partner logo in session_state based on selected partner.
    """
    if st.session_state.get("partner_tool", None):
        if st.session_state["partner_tool"] == "dbt":
            instructions = DBT_INSTRUCTIONS
            image = DBT_IMAGE
            image_size = (72, 32)
        elif st.session_state["partner_tool"] == "looker":
            instructions = LOOKER_INSTRUCTIONS
            image = LOOKER_IMAGE
            image_size = (72, 72)
        st.session_state["partner_instructions"] = instructions
        st.session_state["partner_image"] = image
        st.session_state["partner_image_size"] = image_size


def render_image(image_file: str, size: tuple[int, int]) -> None:
    """
    Renders image in streamlit app with custom width x height by pixel.
    """
    image = Image.open(image_file)
    new_image = image.resize(size)
    st.image(new_image)


def configure_partner_semantic() -> None:
    """
    Upload semantic files from local source.
    """
    from admin_apps.journeys import builder

    partners = [None, "dbt", "looker"]

    partner_tool = st.selectbox(
        "Select the partner tool",
        partners,
        index = None,
        key="partner_tool",
        on_change=set_partner_instructions()
    )
    if st.session_state.get("partner_tool", None):
        with st.expander(f"{st.session_state.get('partner_tool', '').title()} Instructions", expanded=True):
            render_image(st.session_state['partner_image'], st.session_state['partner_image_size'])
            st.write(st.session_state['partner_instructions'])
    if st.session_state["partner_tool"] == "dbt":
        upload_dbt_semantic()
    if st.session_state["partner_tool"] == "looker":
        set_looker_semantic()
    if st.session_state.get("partner_tool", None):
        if st.button("Continue", type="primary"):
            if st.session_state["partner_tool"] == "looker":
                with st.spinner("Saving Explore as a Snowflake table..."):
                    looker_field_metadata = render_looker_explore_as_table(
                                    get_snowflake_connection(),
                                    st.session_state['looker_model_name'],
                                    st.session_state['looker_explore_name'],
                                    st.session_state['looker_target_schema'],
                                    st.session_state['looker_target_table_name'],
                                    [] # TO DO - Add support for field selection
                                    )
                    st.session_state['partner_setup'] = True

                    st.rerun()
            if st.session_state["partner_tool"] == "dbt":
                st.session_state['partner_setup'] = True
                st.rerun()


class PartnerCompareRow:
    def __init__(self, row_data: pd.Series) -> None:  # type: ignore
        self.row_data = row_data
        self.key = row_data["field_key"]
        self.cortex_metadata = (
            self.row_data["field_details_cortex"]
            if self.row_data["field_details_cortex"]
            else {}
        )
        self.partner_metadata = (
            self.row_data["field_details_partner"]
            if self.row_data["field_details_partner"]
            else {}
        )

    def render_row(self) -> None | dict[str, Any]:  # type: ignore
        toggle_options = ["merged", "cortex", "partner", "remove"]
        metadata = {}

        # Create metadata based for each field given merging or singular semantic file useage of the field
        # Merge will merge the 2 based on user-selected preference
        if self.cortex_metadata and self.partner_metadata:
            metadata["merged"] = self.cortex_metadata.copy()
            if st.session_state["partner_metadata_preference"] == "Partner":
                for n in st.session_state['common_fields']:
                    metadata["merged"][n] = self.partner_metadata.get(
                        n, self.cortex_metadata.get(n, None)
                    )
            else:
                for n in st.session_state['common_fields']:
                    metadata["merged"][n] = self.cortex_metadata.get(
                        n, self.partner_metadata.get(n, None)
                    )

        else:
            metadata["merged"] = {}
        metadata["partner"] = (
            {field: self.partner_metadata.get(field) for field in st.session_state['common_fields']}
            if self.partner_metadata
            else {}
        )
        metadata["cortex"] = self.cortex_metadata if self.cortex_metadata else {}
        metadata["remove"] = {}

        if metadata["merged"]:
            toggle_default = "merged"
        elif metadata["partner"]:
            if st.session_state["keep_extra_partner"]:
                toggle_default = "partner"
            else:
                toggle_default = "remove"
        elif metadata["cortex"]:
            if st.session_state["keep_extra_cortex"]:
                toggle_default = "cortex"
            else:
                toggle_default = "remove"
        else:
            toggle_default = "remove"

        key_col, detail_col = st.columns((0.5, 1))
        with key_col:
            st.write(self.key)
            # We want to disable non-options but always keep remove option
            revised_options = [
                i for i in toggle_options if metadata[i] or i == "remove"
            ]
            detail_selection: str = st.radio(
                "Keep",  # type: ignore
                index=revised_options.index(toggle_default),
                options=revised_options,
                key=f"row_{self.key}",
                format_func=lambda x: x.capitalize(),
                label_visibility="collapsed",
            )
        with detail_col:
            if metadata[detail_selection]:
                st.json(
                    {
                        k: v
                        for k, v in metadata[detail_selection].items()
                        if k in st.session_state['common_fields'] and v is not None
                    }
                )
            else:
                st.write("NA")
        st.divider()
        # Extract the selected metadata if not set to remove
        if detail_selection != "remove":
            selected_metadata: dict[str, Any] = metadata[detail_selection]
            # Add expr to selected metadata if it's not included which is the case for dbt
            selected_metadata["expr"] = self.key
            return selected_metadata


def make_field_df(fields: dict[str, Any]) -> pd.DataFrame:
    """
    Converts a nested dictionary of fields into a DataFrame.
    """
    rows = []
    for section, entity_list in fields.items():
        for field_key, field_details in entity_list.items():
            rows.append(
                {
                    "section": section,
                    "field_key": field_key,
                    "field_details": field_details,
                }
            )
    return pd.DataFrame(rows)


def create_table_field_df(
    table_name: str, sections: list[str], yaml_data: list[dict[str, Any]]
) -> pd.DataFrame:
    """
    Extracts sections of table_name in yaml_data dictionary as a DataFrame.
    """
    view = [x for x in yaml_data if x.get("name") == table_name][0]
    fields = extract_expressions_from_sections(view, sections)
    fields_df = make_field_df(fields)

    return fields_df

def determine_field_section_dbt(
    section_cortex: str,
    section_partner: str,
    field_details_cortex: dict[str, str],
    field_details_partner: dict[str, str],
) -> tuple[str, str | None]:
    """
    Derives intended section and data type of field in cortex analyst model.

    Function assumes dbt as partner.
    """
    if section_cortex and field_details_cortex:
        try:
            data_type = field_details_cortex.get("data_type", None)
        except TypeError:
            data_type = "TEXT"
        return (section_cortex, data_type)
    else:  # No matching cortex field found; field is partner is a novel logical field
        if section_partner == "entities":
            section_cortex = "dimensions"
            data_type = "TEXT"
        elif section_partner == "measures":
            section_cortex = "measures"
            data_type = "NUMBER"
        else:  # field_details_partner == 'dimensions'
            try:
                if field_details_partner.get("type") == "time":
                    section_cortex = "time_dimensions"
                    data_type = "DATE"
            except TypeError:
                section_cortex = "dimensions"
                data_type = "TEXT"
            else:
                section_cortex = "dimensions"
                data_type = "TEXT"
        return (section_cortex, data_type)


def determine_field_section(
    section_cortex: str,
    section_partner: str,
    field_details_cortex: dict[str, str],
    field_details_partner: dict[str, str],
) -> tuple[str, str | None]:
    """
    Derives intended section and data type of field in cortex analyst model.
    """

    if st.session_state["partner_tool"] == "dbt":
        (section_cortex, data_type) = determine_field_section_dbt(
            section_cortex,
            section_partner,
            field_details_cortex,
            field_details_partner
            )
        return (section_cortex, data_type)


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


@st.dialog("Integrate partner tool semantic specs", width="large")
def integrate_partner_semantics() -> None:
    st.write(
        "Specify how to merge semantic metadata from partner tools with Cortex Analyst's semantic model."
    )

    COMPARE_SEMANTICS_HELP = """Which semantic file should be checked first for necessary metadata.
    Where metadata is missing, the other semantic file will be checked."""

    INTEGRATE_HELP = """Merge the selected Snowflake and Partner tables' semantics together."""

    SAVE_HELP = """Save the merges to the Cortex Analyst semantic model for validation and iteration."""

    KEEP_CORTEX_HELP = """Retain fields that are found in Cortex Analyst semantic model
    but not in Partner semantic model."""

    KEEP_PARTNER_HELP = """Retain fields that are found in Partner semantic model
    but not in Cortex Analyst semantic model."""


    if (
        st.session_state.get("partner_semantic", None)
        and st.session_state.get("partner_tool", None)
        and st.session_state.get("uploaded_semantic_files", None)
    ):

        # Get cortex semantic file as dictionary
        cortex_semantic = proto_to_dict(st.session_state["semantic_model"])
        cortex_tables = extract_key_values(cortex_semantic["tables"], "name")
        partner_tables = extract_key_values(
            st.session_state["partner_semantic"], "name"
        )
        st.write("Select which logical tables/views to compare and merge.")
        c1, c2 = st.columns(2)
        with c1:
            semantic_cortex_tbl: str = st.selectbox("Snowflake", cortex_tables)  # type: ignore
        with c2:
            semantic_partner_tbl: str = st.selectbox("Partner", partner_tables)  # type: ignore

        st.session_state["partner_metadata_preference"] = st.selectbox(
            "For fields shared in both sources, which source should be checked first for common metadata?",
            ["Partner", "Cortex"],
            index=0,
            help=COMPARE_SEMANTICS_HELP,
        )
        orphan_label, orphan_col1, orphan_col2 = st.columns(
            3, vertical_alignment="center", gap="small"
        )
        with orphan_label:
            st.write("Retain unmatched fields:")
        with orphan_col1:
            st.session_state["keep_extra_cortex"] = st.toggle(
                "Cortex", value=True, help=KEEP_CORTEX_HELP
            )
        with orphan_col2:
            st.session_state["keep_extra_partner"] = st.toggle(
                "Partner", value=True, help=KEEP_PARTNER_HELP
            )
        with st.expander("Advanced configuration", expanded=False):
            st.caption("Only shared metadata information displayed")
            # Create dataframe of each semantic file's fields with mergeable keys
            partner_fields_df = create_table_field_df(
                semantic_partner_tbl,  # type: ignore
                st.session_state['field_section_names'],
                st.session_state["partner_semantic"],
            )
            cortex_fields_df = create_table_field_df(
                semantic_cortex_tbl,  # type: ignore
                ["dimensions", "time_dimensions", "measures"],
                cortex_semantic["tables"],
            )
            combined_fields_df = cortex_fields_df.merge(
                partner_fields_df,
                on="field_key",
                how="outer",
                suffixes=("_cortex", "_partner"),
            ).replace(np.nan, None)
            # Convert json strings to dict for easier extraction later
            for col in ["field_details_cortex", "field_details_partner"]:
                combined_fields_df[col] = combined_fields_df[col].apply(
                    lambda x: (
                        json.loads(x)
                        if not pd.isnull(x) and not isinstance(x, dict)
                        else x
                    )
                )
            # Create containers and store them in a dictionary
            containers = {
                "dimensions": st.container(),
                "measures": st.container(),
                "time_dimensions": st.container(),
            }

            # Assign labels to the containers
            for key in containers.keys():
                containers[key].write(key.replace("_", " ").title())

            # Initialize sections as empty lists
            sections: dict[str, list[dict[str, Any]]] = {
                key: [] for key in containers.keys()
            }

            for k, v in combined_fields_df.iterrows():
                # Get destination section for cortex analyst semantic file
                target_section, target_data_type = determine_field_section(
                    v["section_cortex"],
                    v["section_partner"],
                    v["field_details_cortex"],
                    v["field_details_partner"],
                )
                with containers[target_section]:
                    selected_metadata = PartnerCompareRow(v).render_row()
                    if selected_metadata:
                        selected_metadata["data_type"] = target_data_type
                        sections[target_section].append(selected_metadata)

        integrate_col, commit_col, _ = st.columns((1, 1, 5), gap="small")
        with integrate_col:
            merge_button = st.button(
                "Merge",
                help=INTEGRATE_HELP,
                use_container_width=True
            )
        with commit_col:
            reset_button = st.button(
                "Save",
                help=SAVE_HELP,
                use_container_width=True,
            )

        if merge_button:
            # Update fields in cortex semantic model
            for i, tbl in enumerate(cortex_semantic["tables"]):
                if tbl.get("name", None) == semantic_cortex_tbl:
                    for k in sections.keys():
                        cortex_semantic["tables"][i][k] = sections[k]
            # Submitted changes to fields will be captured in the yaml editor
            # User will need to make necessary modifications there before validating/uploading
            try:
                st.session_state["yaml"] = yaml.dump(cortex_semantic, sort_keys=False)
                st.session_state["semantic_model"] = yaml_to_semantic_model(
                    st.session_state["yaml"]
                )
                merge_msg = st.success("Merging...")
                time.sleep(1)
                merge_msg.empty()
            except Exception as e:
                st.error(f"Integration failed: {e}")

        if reset_button:
            st.success(
                "Integration complete! Please validate your semantic model before uploading."
            )
            time.sleep(1.5)
            st.rerun()  # Lazy alternative to resetting all configurations


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
