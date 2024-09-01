import json
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import sqlglot
import streamlit as st
from snowflake.connector import ProgrammingError, SnowflakeConnection
from streamlit.delta_generator import DeltaGenerator
from streamlit_monaco import st_monaco

from admin_apps.partner.partner_utils import integrate_partner_semantics
from admin_apps.shared_utils import (
    GeneratorAppScreen,
    SnowflakeStage,
    add_logo,
    changed_from_last_validated_model,
    download_yaml,
    format_snowflake_context,
    get_available_databases,
    get_available_schemas,
    get_snowflake_connection,
    init_session_states,
    upload_yaml,
    validate_and_upload_tmp_yaml,
)
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
from semantic_model_generator.snowflake_utils.env_vars import (
    SNOWFLAKE_ACCOUNT_LOCATOR,
    SNOWFLAKE_HOST,
    SNOWFLAKE_USER,
)
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    fetch_stages_in_schema,
    fetch_yaml_names_in_stage,
    set_database,
    set_schema,
)
from semantic_model_generator.validate_model import validate


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
        raise Exception(f"Failed request with status {resp.status_code}: {resp.text}")


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


@st.dialog("Edit", width="large")
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

                    connection = get_snowflake_connection()
                    if "snowflake_stage" in st.session_state:
                        set_database(
                            connection, st.session_state.snowflake_stage.stage_database
                        )
                        set_schema(
                            connection, st.session_state.snowflake_stage.stage_schema
                        )
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
        name=question,
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
    if "semantic_model" in st.session_state:
        st.session_state.ctx = context_to_column_format(st.session_state.semantic_model)
        ctx_table_col_expr_dict = {
            logical_table_name(t): {c.name: c.expr for c in t.columns}
            for t in st.session_state.ctx.tables
        }

        st.session_state.ctx_table_col_expr_dict = ctx_table_col_expr_dict

    FIRST_MESSAGE = "Welcome! 😊 In this app, you can iteratively edit the semantic model YAML on the left side, and test it out in a chat setting here on the right side. How can I help you today?"

    if "messages" not in st.session_state or len(st.session_state.messages) == 0:
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

    chat_placeholder = (
        "What is your question?"
        if st.session_state["validated"]
        else "Please validate your semantic model before chatting."
    )
    if user_input := st.chat_input(
        chat_placeholder, disabled=not st.session_state["validated"]
    ):
        with messages:
            process_message(_conn=_conn, prompt=user_input)

    if st.session_state.active_suggestion:
        with messages:
            process_message(_conn=_conn, prompt=st.session_state.active_suggestion)
        st.session_state.active_suggestion = None


@st.dialog("Upload", width="small")
def upload_dialog(content: str) -> None:
    def upload_handler(file_name: str) -> None:
        if not st.session_state.validated and changed_from_last_validated_model():
            with st.spinner(
                "Your semantic model has changed since last validation. Re-validating before uploading..."
            ):
                validate_and_upload_tmp_yaml(conn=get_snowflake_connection())

        st.session_state.semantic_model = yaml_to_semantic_model(content)
        with st.spinner(
            f"Uploading @{st.session_state.snowflake_stage.stage_name}/{file_name}.yaml..."
        ):
            upload_yaml(file_name, conn=get_snowflake_connection())
        st.success(
            f"Uploaded @{st.session_state.snowflake_stage.stage_name}/{file_name}.yaml!"
        )
        st.session_state.last_saved_yaml = content
        time.sleep(1.5)
        st.rerun()

    if "snowflake_stage" in st.session_state:
        # When opening the iteration app directly, we collect stage information already when downloading the YAML.
        # We only need to ask for the new file name in this case.
        with st.form("upload_form_name_only"):
            st.markdown("This will upload your YAML to the following Snowflake stage.")
            st.write(st.session_state.snowflake_stage.to_dict())
            new_name = st.text_input(
                key="upload_yaml_final_name",
                label="Enter the file name to upload (omit .yaml suffix):",
            )

            if st.form_submit_button("Submit Upload"):
                upload_handler(new_name)
    else:
        # If coming from the builder flow, we need to ask the user for the exact stage path to upload to.
        st.markdown("Please enter the destination of your YAML file.")
        stage_selector_container()
        new_name = st.text_input("File name (omit .yaml suffix)", value="")

        if st.button("Submit Upload"):
            if (
                not st.session_state["selected_iteration_database"]
                or not st.session_state["selected_iteration_schema"]
                or not st.session_state["selected_iteration_stage"]
                or not new_name
            ):
                st.error("Please fill in all fields.")
                return

            st.session_state["snowflake_stage"] = SnowflakeStage(
                stage_database=st.session_state["selected_iteration_database"],
                stage_schema=st.session_state["selected_iteration_schema"],
                stage_name=st.session_state["selected_iteration_stage"],
            )
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


@st.dialog("Error", width="small")
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
        (one, two, three, four) = st.columns(4)
        if one.button("Validate", use_container_width=True, help=VALIDATE_HELP):
            # Validate new content
            try:
                validate(
                    content,
                    snowflake_account=st.session_state.account_name,
                    conn=get_snowflake_connection(),
                )
                st.session_state["validated"] = True
                update_container(
                    status_container, "success", prefix=status_container_title
                )
                st.session_state.semantic_model = yaml_to_semantic_model(content)
                st.session_state.last_saved_yaml = content
            except Exception as e:
                st.session_state["validated"] = False
                update_container(
                    status_container, "failed", prefix=status_container_title
                )
                exception_as_dialog(e)

            # Rerun the app if validation was successful.
            # We shouldn't rerun if validation failed as the error popup would immediately dismiss.
            # This must be done outside of the try/except because the generic Exception handling is catching the
            # exception that st.rerun() properly raises to halt execution.
            # This is fixed in later versions of Streamlit, but other refactors to the code are required to upgrade.
            if st.session_state["validated"]:
                st.rerun()

        if content:
            two.download_button(
                label="Download",
                data=content,
                file_name="semantic_model.yaml",
                mime="text/yaml",
                use_container_width=True,
                help=DOWNLOAD_HELP,
            )

        if three.button(
            "Upload",
            use_container_width=True,
            help=UPLOAD_HELP,
        ):
            upload_dialog(content)
        if st.session_state.get("partner_setup", False):
            if four.button(
                "Integrate Partner",
                use_container_width=True,
                help=PARTNER_SEMANTIC_HELP,
                disabled=not st.session_state["validated"],
            ):
                integrate_partner_semantics()

    # Render the validation state (success=True, failed=False, editing=None) in the editor.
    if st.session_state.validated:
        update_container(status_container, "success", prefix=status_container_title)
    elif st.session_state.validated is not None and not st.session_state.validated:
        update_container(status_container, "failed", prefix=status_container_title)
    else:
        update_container(status_container, "editing", prefix=status_container_title)


@st.cache_resource(show_spinner=False)
def get_available_stages(schema: str) -> List[str]:
    """
    Fetches the available stages from the Snowflake account.

    Returns:
        List[str]: A list of available stages.
    """
    return fetch_stages_in_schema(get_snowflake_connection(), schema)


@st.cache_resource(show_spinner=False)
def get_yamls_from_stage(stage: str) -> List[str]:
    """
    Fetches the YAML files from the specified stage.

    Args:
        stage (str): The name of the stage to fetch the YAML files from.

    Returns:
        List[str]: A list of YAML files in the specified stage.
    """
    return fetch_yaml_names_in_stage(get_snowflake_connection(), stage)


def stage_selector_container() -> None:
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
            set_database(get_snowflake_connection(), stage_database)
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
            set_schema(get_snowflake_connection(), stage_schema)
            available_stages = get_available_stages(stage_schema)
        except (ValueError, ProgrammingError):
            st.error("Insufficient permissions to read from the selected schema.")
            st.stop()

    st.selectbox(
        "Stage name",
        options=available_stages,
        index=None,
        key="selected_iteration_stage",
        format_func=lambda x: format_snowflake_context(x, -1),
    )


@st.dialog("Welcome to the Iteration app! 💬", width="large")
def set_up_requirements() -> None:
    """
    Collects existing YAML location from the user so that we can download it.
    """
    st.markdown(
        "Fill in the Snowflake stage details to download your existing YAML file."
    )

    stage_selector_container()

    # Based on the currently selected stage, show a dropdown of YAML files for the user to pick from.
    available_files = []
    if (
        "selected_iteration_stage" in st.session_state
        and st.session_state["selected_iteration_stage"]
    ):
        # When a valid stage is selected, fetch the available YAML files in that stage.
        try:
            available_files = get_yamls_from_stage(
                st.session_state["selected_iteration_stage"]
            )
        except (ValueError, ProgrammingError):
            st.error("Insufficient permissions to read from the selected stage.")
            st.stop()

    file_name = st.selectbox("File name", options=available_files, index=None)

    if st.button(
        "Submit",
        disabled=not st.session_state["selected_iteration_database"]
        or not st.session_state["selected_iteration_schema"]
        or not st.session_state["selected_iteration_stage"]
        or not file_name,
    ):
        st.session_state["snowflake_stage"] = SnowflakeStage(
            stage_database=st.session_state["selected_iteration_database"],
            stage_schema=st.session_state["selected_iteration_schema"],
            stage_name=st.session_state["selected_iteration_stage"],
        )
        st.session_state["account_name"] = SNOWFLAKE_ACCOUNT_LOCATOR
        st.session_state["host_name"] = SNOWFLAKE_HOST
        st.session_state["user_name"] = SNOWFLAKE_USER
        st.session_state["file_name"] = file_name
        st.session_state["page"] = GeneratorAppScreen.ITERATION
        st.rerun()


VALIDATE_HELP = """Save and validate changes to the active semantic model in this app. This is
useful so you can then play with it in the chat panel on the right side."""

DOWNLOAD_HELP = (
    """Download the currently loaded semantic model to your local machine."""
)

UPLOAD_HELP = """Upload the YAML to the Snowflake stage. You want to do that whenever
you think your semantic model is doing great and should be pushed to prod! Note that
the semantic model must be validated to be uploaded."""

PARTNER_SEMANTIC_HELP = """Uploaded semantic files from a partner tool?
Use this feature to integrate partner semantic specs into Cortex Analyst's spec.
Note that the Cortex Analyst semantic model must be validated before integrating partner semantics."""


def show() -> None:
    init_session_states()

    if "snowflake_stage" not in st.session_state and "yaml" not in st.session_state:
        # If the user is jumping straight into the iteration flow and not coming from the builder flow,
        # we need to collect credentials and load YAML from stage.
        # If coming from the builder flow, there's no need to collect this information until the user wants to upload.
        set_up_requirements()
    else:
        add_logo()
        if "yaml" not in st.session_state:
            # Only proceed to download the YAML from stage if we don't have one from the builder flow.
            yaml = download_yaml(st.session_state.file_name, get_snowflake_connection())
            st.session_state["yaml"] = yaml
            st.session_state["semantic_model"] = yaml_to_semantic_model(yaml)
            if "last_saved_yaml" not in st.session_state:
                st.session_state["last_saved_yaml"] = yaml

        left, right = st.columns(2)
        yaml_container = left.container(height=760)
        chat_container = right.container(height=760)

        with yaml_container:
            # Attempt to use the semantic model stored in the session state.
            # If there is not one present (e.g. they are coming from the builder flow and haven't filled out the
            # placeholders yet), we should still let them edit, so use the raw YAML.
            if st.session_state.semantic_model.name != "":
                editor_contents = proto_to_yaml(st.session_state["semantic_model"])
            else:
                editor_contents = st.session_state["yaml"]

            yaml_editor(editor_contents)

        with chat_container:
            st.markdown("**Chat**")
            # We still initialize an empty connector and pass it down in order to propagate the connector auth token.
            chat_and_edit_vqr(get_snowflake_connection())
