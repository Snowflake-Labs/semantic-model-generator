import json
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import sqlglot
import streamlit as st
from app_utils import (
    _TMP_FILE_NAME,
    SNOWFLAKE_ACCOUNT,
    changed_from_last_validated_model,
)
from snowflake.connector import SnowflakeConnection

from semantic_model_generator.data_processing.cte_utils import (
    context_to_column_format,
    expand_all_logical_tables_as_ctes,
    logical_table_name,
    remove_ltable_cte,
)
from semantic_model_generator.protos import semantic_model_pb2
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    SnowflakeConnector,
)

HOST = os.environ["SNOWFLAKE_HOST"]
USER = os.environ["SNOWFLAKE_USER"]

STAGE_DATABASE = st.session_state.snowflake_stage.stage_database
STAGE_SCHEMA = st.session_state.snowflake_stage.stage_schema
STAGE = st.session_state.snowflake_stage.stage_name

connector = SnowflakeConnector(
    account_name=SNOWFLAKE_ACCOUNT,
    max_workers=1,
)


def get_file_name() -> str:
    if "file_name" in st.session_state:
        return f"{st.session_state.file_name}.yaml"
    else:
        return f"{_TMP_FILE_NAME}.yaml"


def pretty_print_sql(sql: str) -> str:
    """
    Pretty prints SQL using SQLGlot with an option to use the Snowflake dialect for syntax checks.

    Args:
    sql (str): SQL query string to be formatted.

    Returns:
    str: Formatted SQL string.
    """
    # Parse the SQL using SQLGlot
    expression = sqlglot.parse_one(sql, dialect="snowflake")

    # Generate formatted SQL, specifying the dialect if necessary for specific syntax transformations
    formatted_sql: str = expression.sql(dialect="snowflake", pretty=True)
    return formatted_sql


def send_message(conn: SnowflakeConnection, prompt: str) -> Dict[str, Any]:
    """Calls the REST API and returns the response."""
    request_body = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
        "modelPath": get_file_name(),
    }
    num_retry, max_retries = 0, 10
    while True:
        resp = requests.post(
            (
                f"https://{HOST}/api/v2/databases/{STAGE_DATABASE}/schemas/{STAGE_SCHEMA}/copilots/{STAGE}/chats/-/messages"
            ),
            json=request_body,
            headers={
                "Authorization": f'Snowflake Token="{conn.rest.token}"',  # type: ignore[union-attr]
                "Content-Type": "application/json",
            },
            # This is only to skip verifying host match.
            verify=False,
        )
        if resp.status_code < 400:
            json_resp: Dict[str, Any] = resp.json()
            return json_resp
        else:
            if num_retry >= max_retries:
                resp.raise_for_status()
            num_retry += 1
        time.sleep(1)


def process_message(conn: SnowflakeConnection, prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(conn=conn, prompt=prompt)
            content = response["messages"][-1]["content"]
            display_content(conn=conn, content=content)
    st.session_state.messages.append({"role": "assistant", "content": content})


def show_expr_for_ref(message_index: int) -> None:
    """Display the column name and expression as a dataframe, to help user write VQR against logical table/columns."""
    tbl_names = list(st.session_state.ctx_table_col_expr_dict.keys())
    # add multi-select on tbl_name
    tbl_options = tbl_names
    selected_tbl = st.selectbox(
        "Select table for the SQL", tbl_options, key=f"table_options_{message_index}"
    )
    col_dict = st.session_state.ctx_table_col_expr_dict[selected_tbl]
    col_df = pd.DataFrame(
        {"Column Name": k, "Column Expression": v} for k, v in col_dict.items()
    )
    with st.expander("Expand to see available columns and expr", expanded=True):
        st.dataframe(col_df, hide_index=True)


def edit_verified_query(
    conn: SnowflakeConnection, sql: str, question: str, message_index: int
) -> None:
    """Allow user to correct generated SQL and add to verfied queries.
    Note: Verified queries needs to be against logical table/column."""

    expr_col, sql_edit_col = st.columns(2)
    with expr_col:
        show_expr_for_ref(message_index)
    with sql_edit_col:
        sql_without_cte = remove_ltable_cte(sql)
        user_updated_sql = st.text_area(
            "Edit SQL below (write SQL against logical tables/columns on the left):",
            value=sql_without_cte,
            height=250,
            help="This is the original SQL as generated by the Model. Please update as needed to create a sql suitable for your verified query repository.",
            key=f"updated_sql_key_{message_index}",
        )
        st.caption("👆 You can edit the SQL query above to correct or optimize it.")
        confirm_button = st.button(
            "Click to confirm edits and compile",
            key=f"confirm_edits_idx_{message_index}",
        )
        if confirm_button:
            st.session_state.confirmed_edits = True

        if st.session_state.confirmed_edits:
            try:
                sql_to_execute = expand_all_logical_tables_as_ctes(
                    user_updated_sql, st.session_state.ctx
                )
                df = pd.read_sql(sql_to_execute, conn)
                st.code(user_updated_sql)
                st.dataframe(df)
                if st.button(
                    "Click to save modified query to yaml",
                    key=f"confirm_vqr_idx_{message_index}",
                ):
                    add_verified_query(question, sql)
                    st.session_state.editing = False
                    st.session_state.confirmed_edits = False
            except Exception as e:
                raise ValueError(
                    f"Edited SQL not compatible with semantic model provided, please double check: {e}"
                )


def add_verified_query(question: str, sql: str) -> None:
    """Save verified question and SQL into an in-memory list with additional details."""
    # Verified queries follow the Snowflake definitions.
    verified_query = semantic_model_pb2.VerifiedQuery(
        question=question,
        sql=sql,
        verified_by=USER,
        verified_at=int(time.time()),
    )
    st.session_state.semantic_model.verified_queries.append(verified_query)
    st.success(
        "Verified Query Added! You can go back to validate your YAML again and upload; or keep adding more verified queries."
    )


def display_content(
    conn: SnowflakeConnection,
    content: List[Dict[str, Any]],
    message_index: Optional[int] = None,
) -> None:
    """Displays a content item for a message. For generated SQL, allow user to add to verified queries directly or edit then add."""
    message_index = message_index or len(st.session_state.messages)
    sql = ""
    question = ""
    for item in content:
        if item["type"] == "text":
            if question == "" and "__" in item["text"]:
                question = item["text"].split("__")[1]
            # If API rejects to answer directly and provided disambiguate suggestions, we'll return text with <SUGGESTION> as prefix.
            if "<SUGGESTION>" in item["text"]:
                suggestion_response = json.loads(item["text"][12:])[0]
                st.markdown(suggestion_response["explanation"])
                with st.expander("Suggestions", expanded=True):
                    for suggestion_index, suggestion in enumerate(
                        suggestion_response["suggestions"]
                    ):
                        if st.button(
                            suggestion, key=f"{message_index}_{suggestion_index}"
                        ):
                            st.session_state.active_suggestion = suggestion
            else:
                st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            sql = item["statement"]
            sql = pretty_print_sql(sql)
            with st.expander("SQL Query", expanded=True):
                st.code(item["statement"], language="sql")
            df = pd.read_sql(sql, conn)
            st.dataframe(df)
            if st.button(
                "Click to save to verified query",
                key=f"save_idx_{message_index}",
            ):
                add_verified_query(question, remove_ltable_cte(sql))

            if st.button(
                "Click to edit for verified query",
                key=f"edits_idx_{message_index}",
            ):
                st.session_state.editing = True
            if st.session_state.editing:
                edit_verified_query(conn, sql, question, message_index)


def chat_and_edit_vqr(conn: SnowflakeConnection) -> None:
    # Convert semantic model to column format to be backward compatible with some old utils.
    st.session_state.ctx = context_to_column_format(st.session_state.semantic_model)
    ctx_table_col_expr_dict = {
        logical_table_name(t): {c.name: c.expr for c in t.columns}
        for t in st.session_state.ctx.tables
    }

    st.session_state.ctx_table_col_expr_dict = ctx_table_col_expr_dict

    for message_index, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            display_content(
                conn=conn, content=message["content"], message_index=message_index
            )

    if user_input := st.chat_input("What is your question?"):
        process_message(conn=conn, prompt=user_input)

    if st.session_state.active_suggestion:
        process_message(conn=conn, prompt=st.session_state.active_suggestion)
        st.session_state.active_suggestion = None


if not st.session_state.validated:
    st.warning("Please validate your yaml first!")
elif changed_from_last_validated_model():
    st.warning(
        "Your semantic model has changed since last validation. Please re-validate your model before chatting"
    )
else:
    with connector.connect(
        db_name=st.session_state.snowflake_stage.stage_database,
        schema_name=st.session_state.snowflake_stage.stage_schema,
    ) as conn:
        chat_and_edit_vqr(conn)
