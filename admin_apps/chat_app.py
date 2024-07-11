import json
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import sqlglot
import streamlit as st
from shared_utils import (
    SNOWFLAKE_ACCOUNT,
    SnowflakeStage,
    add_logo,
    changed_from_last_validated_model,
    download_yaml,
    init_session_states,
    upload_yaml,
    validate_and_upload_tmp_yaml,
)
from snowflake.connector import SnowflakeConnection
from streamlit.delta_generator import DeltaGenerator
from streamlit_monaco import st_monaco

from semantic_model_generator.data_processing.cte_utils import (
    context_to_column_format,
    expand_all_logical_tables_as_ctes,
    logical_table_name,
    remove_ltable_cte,
)
from semantic_model_generator.data_processing.proto_utils import (
    proto_to_yaml,
    yaml_to_semantic_model,
)
from semantic_model_generator.protos import semantic_model_pb2
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    SnowflakeConnector,
)
from semantic_model_generator.validate_model import validate

st.set_page_config(layout="wide", page_icon="💬", page_title="Chat app")
init_session_states()


@st.cache_resource
def get_connector() -> SnowflakeConnector:
    return SnowflakeConnector(
        account_name=SNOWFLAKE_ACCOUNT,
        max_workers=1,
    )


connector = get_connector()


def get_file_name() -> str:
    return st.session_state.file_name  # type: ignore


@st.cache_data(show_spinner=False)
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


API_ENDPOINT = "https://{HOST}/api/v2/cortex/analyst/message"


@st.cache_data(ttl=60, show_spinner=False)
def send_message(_conn: SnowflakeConnection, prompt: str) -> Dict[str, Any]:
    """Calls the REST API and returns the response."""
    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            },
        ],
        "semantic_model": proto_to_yaml(st.session_state.semantic_model),
    }

    host = st.session_state.host_name
    num_retry, max_retries = 0, 10
    while True:
        resp = requests.post(
            API_ENDPOINT.format(
                HOST=host,
            ),
            json=request_body,
            headers={
                "Authorization": f'Snowflake Token="{_conn.rest.token}"',  # type: ignore[union-attr]
                "Content-Type": "application/json",
            },
        )
        if resp.status_code < 400:
            json_resp: Dict[str, Any] = resp.json()
            return json_resp
        else:
            if num_retry >= max_retries:
                resp.raise_for_status()
            num_retry += 1
        time.sleep(1)


def process_message(_conn: SnowflakeConnection, prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(_conn=_conn, prompt=prompt)
            content = response["message"]["content"]
            display_content(conn=_conn, content=content)
    st.session_state.messages.append({"role": "assistant", "content": content})


def show_expr_for_ref(message_index: int) -> None:
    """Display the column name and expression as a dataframe, to help user write VQR against logical table/columns."""
    tbl_names = list(st.session_state.ctx_table_col_expr_dict.keys())
    # add multi-select on tbl_name
    tbl_options = tbl_names
    selected_tbl = st.selectbox(
        "Select table for the SQL", tbl_options, key=f"table_options_{message_index}"
    )
    if selected_tbl is not None:
        col_dict = st.session_state.ctx_table_col_expr_dict[selected_tbl]
        col_df = pd.DataFrame(
            {"Column Name": k, "Column Expression": v} for k, v in col_dict.items()
        )
        st.dataframe(col_df, hide_index=True, use_container_width=True, height=250)


@st.experimental_dialog("Edit", width="large")
def edit_verified_query(
    conn: SnowflakeConnection, sql: str, question: str, message_index: int
) -> None:
    """Allow user to correct generated SQL and add to verfied queries.
    Note: Verified queries needs to be against logical table/column."""

    # When opening the modal, we haven't run the query yet, so set this bit to False.
    st.session_state["error_state"] = None
    st.caption("**CHEAT SHEET**")
    st.markdown(
        "This section is useful for you to check available columns and expressions. **NOTE**: Only reference `Column Name` in your SQL, not `Column Expression`."
    )
    show_expr_for_ref(message_index)
    st.markdown("")
    st.divider()

    sql_without_cte = remove_ltable_cte(sql)
    st.markdown(
        "You can edit the SQL below. Make sure to use the `Column Name` column in the **Cheat sheet** above for tables/columns available."
    )

    with st.container(border=False):
        st.caption("**SQL**")
        with st.container(border=True):
            user_updated_sql = st_monaco(
                value=sql_without_cte, language="sql", height=200
            )
            run = st.button("Run", use_container_width=True)

            if run:
                try:
                    sql_to_execute = expand_all_logical_tables_as_ctes(
                        user_updated_sql, st.session_state.ctx
                    )

                    # TODO: figure out how to reuse the original connection, it's closed by this point
                    with connector.connect(
                        db_name=st.session_state.snowflake_stage.stage_database,
                        schema_name=st.session_state.snowflake_stage.stage_schema,
                    ) as connection:
                        st.session_state["successful_sql"] = False
                        df = pd.read_sql(sql_to_execute, connection)
                        st.code(user_updated_sql)
                        st.caption("**Output data**")
                        st.dataframe(df)
                        st.session_state["successful_sql"] = True

                except Exception as e:
                    st.session_state["error_state"] = (
                        f"Edited SQL not compatible with semantic model provided, please double check: {e}"
                    )

            if st.session_state["error_state"] is not None:
                st.error(st.session_state["error_state"])

            elif st.session_state.get("successful_sql", False):
                # Moved outside the `if run:` block to ensure it's always evaluated
                save = st.button(
                    "Save as verified query",
                    use_container_width=True,
                    disabled=not st.session_state.get("successful_sql", False),
                )
                if save:
                    add_verified_query(question, user_updated_sql)
                    st.session_state["editing"] = False
                    st.session_state["confirmed_edits"] = True


def add_verified_query(question: str, sql: str) -> None:
    """Save verified question and SQL into an in-memory list with additional details."""
    # Verified queries follow the Snowflake definitions.
    verified_query = semantic_model_pb2.VerifiedQuery(
        question=question,
        sql=sql,
        verified_by=st.session_state["user_name"],
        verified_at=int(time.time()),
    )
    st.session_state.semantic_model.verified_queries.append(verified_query)
    st.success(
        "Verified Query Added! You can go back to validate your YAML again and upload; or keep adding more verified queries."
    )
    st.rerun()


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
            with st.container(height=500, border=False):
                sql = item["statement"]
                sql = pretty_print_sql(sql)
                with st.container(height=250, border=False):
                    st.code(item["statement"], language="sql")

                df = pd.read_sql(sql, conn)
                st.dataframe(df, hide_index=True)

                left, right = st.columns(2)
                if right.button(
                    "Save as verified query",
                    key=f"save_idx_{message_index}",
                    use_container_width=True,
                ):
                    add_verified_query(question, remove_ltable_cte(sql))

                if left.button(
                    "Edit",
                    key=f"edits_idx_{message_index}",
                    use_container_width=True,
                ):
                    edit_verified_query(conn, sql, question, message_index)


def chat_and_edit_vqr(_conn: SnowflakeConnection) -> None:
    messages = st.container(height=600, border=False)

    # Convert semantic model to column format to be backward compatible with some old utils.
    st.session_state.ctx = context_to_column_format(st.session_state.semantic_model)
    ctx_table_col_expr_dict = {
        logical_table_name(t): {c.name: c.expr for c in t.columns}
        for t in st.session_state.ctx.tables
    }

    st.session_state.ctx_table_col_expr_dict = ctx_table_col_expr_dict

    if len(st.session_state.messages) == 0:
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "text",
                        "text": FIRST_MESSAGE,
                    }
                ],
            }
        ]

    for message_index, message in enumerate(st.session_state.messages):
        with messages:
            with st.chat_message(message["role"]):
                display_content(
                    conn=_conn, content=message["content"], message_index=message_index
                )

    if user_input := st.chat_input("What is your question?"):
        with messages:
            process_message(_conn=_conn, prompt=user_input)

    if st.session_state.active_suggestion:
        with messages:
            process_message(_conn=_conn, prompt=st.session_state.active_suggestion)
        st.session_state.active_suggestion = None


@st.experimental_dialog("Upload", width="small")
def upload_dialog(content: str) -> None:
    st.markdown("This will upload your YAML to the following Snowflake stage.")
    st.write(st.session_state.snowflake_stage.to_dict())

    def upload_handler(file_name: str) -> None:
        if not st.session_state.validated and changed_from_last_validated_model():
            with st.spinner(
                "Your semantic model has changed since last validation. Re-validating before uploading..."
            ):
                validate_and_upload_tmp_yaml()

        st.session_state.semantic_model = yaml_to_semantic_model(content)
        with st.spinner(
            f"Uploading @{st.session_state.snowflake_stage.stage_name}/{file_name}.yaml..."
        ):
            upload_yaml(file_name)
        st.success(
            f"Uploaded @{st.session_state.snowflake_stage.stage_name}/{file_name}.yaml!"
        )
        st.session_state.last_saved_yaml = content
        time.sleep(1.5)
        st.rerun()

    new_name = st.text_input(
        key="upload_yaml_final_name",
        label="Enter the file name to upload (no need for .yaml suffix):",
    )

    if st.button("Submit Upload"):
        upload_handler(new_name)


def update_container(
    container: DeltaGenerator, content: str, prefix: Optional[str]
) -> None:
    """
    Update the given Streamlit container with the provided content.

    Args:
        container (DeltaGenerator): The Streamlit container to update.
        content (str): The content to be displayed in the container.
        prefix (str): The prefix to be added to the content.
    """

    # Clear container
    container.empty()

    if content == "success":
        content = "  ·  :green[✅  Model up-to-date and validated]"
    elif content == "editing":
        content = "  ·  :gray[✏️  Editing...]"
    elif content == "failed":
        content = "  ·  :red[❌  Validation failed. Please fix the errors]"

    if prefix:
        content = prefix + content

    container.markdown(content)


@st.experimental_dialog("Error", width="small")
def exception_as_dialog(e: Exception) -> None:
    st.error(f"An error occurred: {e}")


# TODO: how to properly mark fragment back?
# @st.experimental_fragment
def yaml_editor(yaml_str: str) -> None:
    """
    Editor for YAML content. Meant to be used on the left side
    of the app.

    Args:
        yaml_str (str): YAML content to be edited.
        status_container (DeltaGenerator): Container in
            which we will write the edition status (validated, editing
            or failed).
    """

    content = st_monaco(
        value=yaml_str,
        height="600px",
        language="yaml",
    )

    button_container = st.container()
    status_container_title = "**Edit**"
    status_container = st.empty()

    with button_container:
        left, right, _ = st.columns((1, 1, 2))
        if left.button("Save", use_container_width=True, help=SAVE_HELP):
            # Validate new content
            try:
                validate(content, snowflake_account=st.session_state.account_name)
                st.session_state["validated"] = True
                update_container(status_container, "success", prefix=status_container_title)
            except Exception as e:
                st.session_state["validated"] = False
                update_container(status_container, "failed", prefix=status_container_title)
                exception_as_dialog(e)

            st.session_state.semantic_model = yaml_to_semantic_model(content)
            st.session_state.last_saved_yaml = content
        if right.button(
            "Upload",
            use_container_width=True,
            help=UPLOAD_HELP,
        ):
            upload_dialog(content)

    # When no change, show success
    if content == st.session_state.last_saved_yaml:
        update_container(status_container, "success", prefix=status_container_title)

    else:
        update_container(status_container, "editing", prefix=status_container_title)
        st.session_state["validated"] = False


@st.experimental_dialog(
    "Welcome to the Chat app! 💬",
    width="large",
)
def set_up_requirements() -> None:
    st.markdown(
        "Before we get started, let's make sure we have everything set up. If you'd like to populate these values by default, please follow the [environment variable setup instructions](https://github.com/Snowflake-Labs/semantic-model-generator/blob/main/README.md#setup)."
    )
    account_name = st.text_input(
        "Account", value=os.environ.get("SNOWFLAKE_ACCOUNT_LOCATOR")
    )
    host_name = st.text_input("Host", value=os.environ.get("SNOWFLAKE_HOST"))
    user_name = st.text_input("User", value=os.environ.get("SNOWFLAKE_USER"))
    stage_database = st.text_input("Stage database", value="")
    stage_schema = st.text_input("Stage schema", value="")
    stage_name = st.text_input("Stage name", value="")
    file_name = st.text_input("File name", value="<your_file>.yaml")
    if st.button("Submit"):
        st.session_state["snowflake_stage"] = SnowflakeStage(
            stage_database=stage_database,
            stage_schema=stage_schema,
            stage_name=stage_name,
        )
        st.session_state["account_name"] = account_name
        st.session_state["host_name"] = host_name
        st.session_state["user_name"] = user_name
        st.session_state["file_name"] = file_name
        st.rerun()


# First, user must set up some requirements (stage, host, user, etc.)

if "snowflake_stage" not in st.session_state:
    set_up_requirements()
    st.stop()

add_logo()

if "last_saved_yaml" not in st.session_state:
    yaml = download_yaml(st.session_state.file_name)
    st.session_state["last_saved_yaml"] = yaml
    st.session_state["semantic_model"] = yaml_to_semantic_model(yaml)
if "yaml" not in st.session_state:
    yaml = download_yaml(st.session_state.file_name)
    st.session_state["yaml"] = yaml
    st.session_state["semantic_model"] = yaml_to_semantic_model(yaml)

# Now, user can interact with both panels
left, right = st.columns(2)
yaml_container = left.container(height=760)
chat_container = right.container(height=760)

SAVE_HELP = """Save changes to the active semantic model in this app. This is
useful so you can then play with it in the chat panel on the right side."""

UPLOAD_HELP = """Upload the YAML to the Snowflake stage. You want to do that whenever
you think your semantic model is doing great and should be pushed to prod! Note that
the semantic model must be validated to be uploaded."""

with yaml_container:
    yaml_editor(
        proto_to_yaml(st.session_state["semantic_model"]),
    )

FIRST_MESSAGE = f"""Welcome! 😊
In this app, you can iteratively edit the semantic model YAML
on the left side, and test it out in a chat setting here on the right side.

Just so you know, I'm currently using the semantic model `{st.session_state.file_name}`.

How can I help you today?
"""

with chat_container:
    st.markdown("**Chat**")
    with connector.connect(
        db_name=st.session_state.snowflake_stage.stage_database,
        schema_name=st.session_state.snowflake_stage.stage_schema,
    ) as conn:
        chat_and_edit_vqr(conn)
