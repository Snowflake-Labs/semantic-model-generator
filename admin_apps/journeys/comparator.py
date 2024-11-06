from typing import Any

import pandas as pd
import sqlglot
import streamlit as st
from loguru import logger
from snowflake.connector import SnowflakeConnection
from streamlit_monaco import st_monaco

from admin_apps.shared_utils import GeneratorAppScreen, return_home_button, send_message
from semantic_model_generator.data_processing.proto_utils import yaml_to_semantic_model
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    SnowflakeConnector,
)
from semantic_model_generator.validate_model import validate

MODEL1_PATH = "model1_path"
MODEL1_YAML = "model1_yaml"
MODEL2_PATH = "model2_path"
MODEL2_YAML = "model2_yaml"


def init_session_states() -> None:
    st.session_state["page"] = GeneratorAppScreen.COMPARATOR


def comparator_app() -> None:
    return_home_button()
    st.write("## Compare two semantic models")
    col1, col2 = st.columns(2)
    with col1, st.container(border=True):
        st.write(f"Model 1 from: `{st.session_state[MODEL1_PATH]}`")
        content1 = st_monaco(
            value=st.session_state[MODEL1_YAML],
            height="400px",
            language="yaml",
        )

    with col2, st.container(border=True):
        st.write(f"Model 2 from: `{st.session_state[MODEL2_PATH]}`")
        content2 = st_monaco(
            value=st.session_state[MODEL2_YAML],
            height="400px",
            language="yaml",
        )

    if st.button("Validate models"):
        with st.spinner(f"validating {st.session_state[MODEL1_PATH]}..."):
            try:
                validate(content1, st.session_state.account_name)
                st.session_state["model1_valid"] = True
                st.session_state[MODEL1_YAML] = content1
            except Exception as e:
                st.error(f"Validation failed on the first model with error: {e}")
                st.session_state["model1_valid"] = False

        with st.spinner(f"validating {st.session_state[MODEL2_PATH]}..."):
            try:
                validate(content2, st.session_state.account_name)
                st.session_state["model2_valid"] = True
                st.session_state[MODEL2_YAML] = content2
            except Exception as e:
                st.error(f"Validation failed on the second model with error: {e}")
                st.session_state["model2_valid"] = False

        if st.session_state.get("model1_valid", False) and st.session_state.get(
            "model2_valid", False
        ):
            st.success("Both models are correct.")
            st.session_state["validated"] = True
        else:
            st.error("Please fix the models and try again.")
            st.session_state["validated"] = False

    if (
        content1 != st.session_state[MODEL1_YAML]
        or content2 != st.session_state[MODEL2_YAML]
    ):
        st.info("Please validate the models again after making changes.")
        st.session_state["validated"] = False

    if not st.session_state.get("validated", False):
        st.info("Please validate the models first.")
    else:
        prompt = st.text_input(
            "What question would you like to ask the Cortex Analyst?"
        )
        if prompt:
            st.write(f"Asking both models question: {prompt}")
            user_message = [
                {"role": "user", "content": [{"type": "text", "text": prompt}]}
            ]
            connection = SnowflakeConnector(
                account_name=st.session_state.account_name,
                max_workers=1,
            ).open_connection(db_name="")

            col1, col2 = st.columns(2)
            ask_cortex_analyst(
                user_message,
                st.session_state[MODEL1_YAML],
                connection,
                col1,
                "Model 1 is thinking...",
            )
            ask_cortex_analyst(
                user_message,
                st.session_state[MODEL2_YAML],
                connection,
                col2,
                "Model 2 is thinking...",
            )

    # TODO:
    # - Show the differences
    # - Check if both models are pointing at the same table


def ask_cortex_analyst(
    prompt: str,
    semantic_model: str,
    conn: SnowflakeConnection,
    container: Any,
    spinner_text: str,
) -> None:
    """Ask the Cortex Analyst a question and display the response.

    Args:
        prompt (str): The question to ask the Cortex Analyst.
        semantic_model (str): The semantic model to use for the question.
        conn (SnowflakeConnection): The Snowflake connection to use for the question.
        container (st.DeltaGenerator): The streamlit container to display the response (e.g. st.columns()).
        spinner_text (str): The text to display in the waiting spinner

    Returns:
        None

    """
    with container, st.container(border=True), st.spinner(spinner_text):
        json_resp = send_message(conn, prompt, yaml_to_semantic_model(semantic_model))
        display_content(conn, json_resp["message"]["content"])
        st.json(json_resp, expanded=False)


@st.cache_data(show_spinner=False)
def prettify_sql(sql: str) -> str:
    """
    Prettify SQL using SQLGlot with an option to use the Snowflake dialect for syntax checks.

    Args:
    sql (str): SQL query string to be formatted.

    Returns:
    str: Formatted SQL string or input SQL if sqlglot failed to parse.
    """
    try:
        # Parse the SQL using SQLGlot
        expression = sqlglot.parse_one(sql, dialect="snowflake")

        # Generate formatted SQL, specifying the dialect if necessary for specific syntax transformations
        formatted_sql: str = expression.sql(dialect="snowflake", pretty=True)
        return formatted_sql
    except Exception as e:
        logger.debug(f"Failed to prettify SQL: {e}")
        return sql


def display_content(
    conn: SnowflakeConnection,
    content: list[dict[str, Any]],
) -> None:
    """Displays a content item for a message from the Cortex Analyst."""
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion in item["suggestions"]:
                    st.markdown(f"- {suggestion}")
        elif item["type"] == "sql":
            with st.container(height=500, border=False):
                sql = item["statement"]
                sql = prettify_sql(sql)
                with st.container(height=250, border=False):
                    st.code(item["statement"], language="sql")
                try:
                    df = pd.read_sql(sql, conn)
                    st.dataframe(df, hide_index=True)
                except Exception as e:
                    st.error(f"Failed to execute SQL: {e}")
        else:
            logger.warning(f"Unknown content type: {item['type']}")
            st.write(item)


def is_session_state_initialized() -> bool:
    return all(
        [
            MODEL1_YAML in st.session_state,
            MODEL2_YAML in st.session_state,
            MODEL1_PATH in st.session_state,
            MODEL2_PATH in st.session_state,
        ]
    )


@st.dialog("Welcome to the Cortex Analyst Annotation Workspace! 📝", width="large")
def init_dialog() -> None:
    init_session_states()

    st.write(
        "Please choose the two semantic model files that you would like to compare."
    )

    model_1_file = st.file_uploader(
        "Choose first semantic model file",
        type=["yaml"],
        help="Choose a local YAML file that contains semantic model.",
    )
    model_2_file = st.file_uploader(
        "Choose second semantic model file",
        type=["yaml"],
        help="Choose a local YAML file that contains semantic model.",
    )

    if st.button("Compare"):
        if model_1_file is None or model_2_file is None:
            st.error("Please upload the both models first.")
        else:
            st.session_state[MODEL1_PATH] = model_1_file.name
            st.session_state[MODEL1_YAML] = model_1_file.getvalue().decode("utf-8")
            st.session_state[MODEL2_PATH] = model_2_file.name
            st.session_state[MODEL2_YAML] = model_2_file.getvalue().decode("utf-8")
            st.rerun()

    return_home_button()


def show() -> None:
    init_session_states()
    if is_session_state_initialized():
        comparator_app()
    else:
        init_dialog()
