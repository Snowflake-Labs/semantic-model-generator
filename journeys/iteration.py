import json
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import sqlglot
import streamlit as st
from snowflake.connector import ProgrammingError, SnowflakeConnection
from streamlit import config
from snowflake.connector.pandas_tools import write_pandas
from streamlit.delta_generator import DeltaGenerator
from streamlit_extras.row import row
from streamlit_extras.stylable_container import stylable_container
from semantic_model_generator.snowflake_utils.snowflake_connector import fetch_table

from app_utils.chat import send_message
from app_utils.shared_utils import (
    GeneratorAppScreen,
    SnowflakeStage,
    changed_from_last_validated_model,
    download_yaml,
    get_snowflake_connection,
    get_yamls_from_stage,
    init_session_states,
    return_home_button,
    schema_selector_container,
    stage_selector_container,
    table_selector_container,
    upload_yaml,
    validate_and_upload_tmp_yaml,
    validate_table_exist,
    validate_table_schema,
)
from journeys.evaluation import evaluation_mode_show
from journeys.joins import joins_dialog
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
    create_table_in_schema,
    execute_query,
    fetch_table,
    get_table_hash,
)
from semantic_model_generator.validate_model import validate

EVALUATION_TABLE_SCHEMA = {
    "ID": "VARCHAR",
    "QUERY": "VARCHAR",
    "GOLD_SQL": "VARCHAR",
}
RESULTS_TABLE_SCHEMA = {
    "ID": "VARCHAR",
    "ANALYST_TEXT": "VARCHAR",
    "ANALYST_SQL": "VARCHAR",
    "CORRECT": "BOOLEAN",
    "EXPLANATION": "VARCHAR",
}

LLM_JUDTE_PROMPT_TEMPLATE = """\
[INST] Your task is to determine whether the two given dataframes are
equivalent semantically in the context of a question. You should attempt to
answer the given question by using the data in each dataframe. If the two
answers are equivalent, those two dataframes are considered equivalent.
Otherwise, they are not equivalent. Please also provide your reasoning.
If they are equivalent, output "REASON: <reason>. ANSWER: true". If they are
not equivalent, output "REASON: <reason>. ANSWER: false".

### QUESTION: {input_question}

* DATAFRAME 1:
{frame1_str}

* DATAFRAME 2:
{frame2_str}

Are the two dataframes equivalent?
OUTPUT:
[/INST] """

# Set minCachedMessageSize to 500 MB to disable forward message cache:
# st.set_config would trigger an error, only the set_config from config module works
config.set_option("global.minCachedMessageSize", 500 * 1e6)


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


def process_message(_conn: SnowflakeConnection, prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    user_message = {"role": "user", "content": [{"type": "text", "text": prompt}]}
    st.session_state.messages.append(user_message)
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            # Depending on whether multiturn is enabled, we either send just the user message or the entire chat history.
            request_messages = (
                st.session_state.messages[1:]  # Skip the welcome message
                if st.session_state.multiturn
                else [user_message]
            )
            try:
                response = send_message(
                    _conn=_conn,
                    semantic_model=proto_to_yaml(st.session_state.semantic_model),
                    messages=request_messages,
                )
                content = response["message"]["content"]
                # Grab the request ID from the response and stash it in the chat message object.
                request_id = response["request_id"]
                display_content(conn=_conn, content=content, request_id=request_id)
                st.session_state.messages.append(
                    {"role": "analyst", "content": content, "request_id": request_id}
                )
            except ValueError as e:
                st.error(e)
                # Remove the user message from the chat history if the request fails.
                # We should only save messages to history for successful (user, assistant) turns.
                st.session_state.messages.pop()


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
        # Workaround for column_width bug in dataframe object within nested dialog
        st.table(col_df.set_index(col_df.columns[1]))


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

    sql_without_cte = remove_ltable_cte(
        sql, table_names=[t.name for t in st.session_state.semantic_model.tables]
    )
    st.markdown(
        "You can edit the SQL below. Make sure to use the `Column Name` column in the **Cheat sheet** above for tables/columns available."
    )

    with st.container(border=False):
        st.caption("**SQL**")
        with st.container(border=True):
            css_yaml_editor = """
                textarea{
                    font-size: 14px;
                    color: #2e2e2e;
                    font-family:Menlo;
                    background-color: #fbfbfb;
                }
                """
            # Style text_area to mirror st.code
            with stylable_container(
                key="customized_text_area", css_styles=css_yaml_editor
            ):
                user_updated_sql = st.text_area(
                    label="sql_editor",
                    label_visibility="collapsed",
                    value=sql_without_cte,
                )
            run = st.button("Run", use_container_width=True)

            if run:
                try:
                    sql_to_execute = expand_all_logical_tables_as_ctes(
                        user_updated_sql, st.session_state.ctx
                    )

                    connection = get_snowflake_connection()
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
                mark_as_onboarding = st.checkbox(
                    "Mark as onboarding question",
                    key=f"edit_onboarding_idx_{message_index}",
                    help="Mark this question as an onboarding verified query.",
                )
                save = st.button(
                    "Save as verified query",
                    use_container_width=True,
                    disabled=not st.session_state.get("successful_sql", False),
                )
                if save:
                    sql_no_analyst_comment = user_updated_sql.replace(
                        " /* Generated by Cortex Analyst */", ""
                    )
                    add_verified_query(
                        question,
                        sql_no_analyst_comment,
                        is_onboarding_question=mark_as_onboarding,
                    )
                    st.session_state["editing"] = False
                    st.session_state["confirmed_edits"] = True


def add_verified_query(
    question: str, sql: str, is_onboarding_question: bool = False
) -> None:
    """Save verified question and SQL into an in-memory list with additional details."""
    # Verified queries follow the Snowflake definitions.
    verified_query = semantic_model_pb2.VerifiedQuery(
        name=question,
        question=question,
        sql=sql,
        verified_by=st.session_state["user_name"],
        verified_at=int(time.time()),
        use_as_onboarding_question=is_onboarding_question,
    )
    st.session_state.semantic_model.verified_queries.append(verified_query)
    st.success(
        "Verified Query Added! You can go back to validate your YAML again and upload; or keep adding more verified queries."
    )
    st.rerun()


def display_content(
    conn: SnowflakeConnection,
    content: List[Dict[str, Any]],
    request_id: Optional[str],
    message_index: Optional[int] = None,
) -> None:
    """Displays a content item for a message. For generated SQL, allow user to add to verified queries directly or edit then add."""
    message_index = message_index or len(st.session_state.messages)
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

                mark_as_onboarding = st.checkbox(
                    "Mark as onboarding question",
                    key=f"onboarding_idx_{message_index}",
                    help="Mark this question as an onboarding verified query.",
                )
                left, right = st.columns(2)
                if right.button(
                    "Save as verified query",
                    key=f"save_idx_{message_index}",
                    use_container_width=True,
                ):
                    sql_no_cte = remove_ltable_cte(
                        sql,
                        table_names=[
                            t.name for t in st.session_state.semantic_model.tables
                        ],
                    )
                    cleaned_sql = sql_no_cte.replace(
                        " /* Generated by Cortex Analyst */", ""
                    )
                    add_verified_query(
                        question, cleaned_sql, is_onboarding_question=mark_as_onboarding
                    )

                if left.button(
                    "Edit",
                    key=f"edits_idx_{message_index}",
                    use_container_width=True,
                ):
                    edit_verified_query(conn, sql, question, message_index)

    # If debug mode is enabled, we render the request ID. Note that request IDs are currently only plumbed
    # through for assistant messages, as we obtain the request ID as part of the Analyst response.
    if request_id and st.session_state.chat_debug:
        st.caption(f"Request ID: {request_id}")


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
                "role": "analyst",
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
            # To get the handy robot icon on assistant messages, the role needs to be "assistant" or "ai".
            # However, the Analyst API uses "analyst" as the role, so we need to convert it at render time.
            render_role = "assistant" if message["role"] == "analyst" else "user"
            with st.chat_message(render_role):
                display_content(
                    conn=_conn,
                    content=message["content"],
                    message_index=message_index,
                    request_id=message.get(
                        "request_id"
                    ),  # Safe get since user messages have no request IDs
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


def clear_evaluation_data() -> None:
    session_states = (
        "eval_table_frame",
        "eval_table_hash",
        "selected_eval_database",
        "selected_eval_schema",
        "selected_eval_table",
        "selected_results_eval_database",
        "selected_results_eval_new_table",
        "selected_results_eval_new_table_no_schema",
        "selected_results_eval_old_table",
        "selected_results_eval_schema",
        "use_existing_table",
    )
    for feature in session_states:
        if feature in st.session_state:
            del st.session_state[feature]


def validate_table_columns(param, evaluation_table_columns):
    pass


@st.dialog("Evaluation Data", width="large")
def evaluation_data_dialog() -> None:
    evaluation_table_columns = ["ID", "QUERY", "GOLD_SQL"]
    st.markdown("Please select evaluation table")
    table_selector_container(
        db_selector={"key": "selected_eval_database", "label": "Eval database"},
        schema_selector={"key": "selected_eval_schema", "label": "Eval schema"},
        table_selector={"key": "selected_eval_table", "label": "Eval table"},
    )
    if st.button("Use Table"):
        if (
            not st.session_state["selected_eval_database"]
            or not st.session_state["selected_eval_schema"]
            or not st.session_state["selected_eval_table"]
        ):
            st.error("Please fill in all fields.")
            return

        if not validate_table_columns(st.session_state["selected_eval_table"], evaluation_table_columns):
            st.error("Table must have columns {evaluation_table_columns} to be used in Evaluation")
            return

        st.session_state["eval_table"] = SnowflakeTable(
            table_database=st.session_state["selected_eval_database"],
            table_schema=st.session_state["selected_eval_schema"],
            table_name=st.session_state["selected_eval_table"],
        )
        st.session_state["eval_table_hash"] = get_table_hash(
            conn=get_snowflake_connection(), table_fqn=st.session_state.eval_table.table_name
        )
        eval_table_frame = fetch_table(
            conn=get_snowflake_connection(), table_fqn=st.session_state.eval_table.table_name
        )
        st.session_state["eval_table_frame"] = eval_table_frame.set_index("ID")

        st.rerun()


    if not eval_results_existing_table:
        schema_selector_container(
            db_selector={"key": "selected_results_eval_database","label":"Results database"},
            schema_selector={"key": "selected_results_eval_schema","label":"Results schema"},)

        original_new_table_name = st.text_input(
            key="selected_results_eval_new_table_no_schema",
            label="Enter the table name to upload evaluation results",
        )
        if st.button("Create Table"):
            if (
                not st.session_state["selected_results_eval_database"]
                or not st.session_state["selected_results_eval_schema"]
                or not new_table_name
            ):
                st.error("Please fill in all fields.")
                return

            if (
                st.session_state["selected_results_eval_database"]
                and st.session_state["selected_results_eval_schema"]
                and validate_table_exist(
                    st.session_state["selected_results_eval_schema"], new_table_name
                )
            ):
                st.error("Table already exists")
                return


            with st.spinner("Creating table..."):
                success = create_table_in_schema(
                    conn=get_snowflake_connection(),
                    schema_name=st.session_state["selected_results_eval_schema"],
                    table_name=new_table_name,
                    columns_schema=[
                        f"{k} {v}" for k, v in results_table_columns.items()
                    ],
                )
                if success:
                    st.success(f"Table {new_table_name} created successfully!")
                else:
                    st.error(f"Failed to create table {new_table_name}")
                    return

            fqn_table_name = ".".join([st.session_state["selected_results_eval_schema"],new_table_name.upper()])

            st.session_state["eval_results_table"] = SnowflakeTable(
                table_database=st.session_state["selected_results_eval_database"],
                table_schema=st.session_state["selected_results_eval_schema"],
                table_name=fqn_table_name,
            )

            st.rerun()

    else:
        table_selector_container(
            db_selector={
                "key": "selected_results_eval_database",
                "label": "Results database",
            },
            schema_selector={
                "key": "selected_results_eval_schema",
                "label": "Results schema",
            },
            table_selector={
                "key": "selected_results_eval_old_table",
                "label": "Results table",
            },
        )

    st.divider()

    if st.button("Use Tables"):
        st.session_state["selected_results_eval_table"] = st.session_state.get(
            "selected_results_eval_new_table"
        ) or st.session_state.get("selected_results_eval_old_table")

        if (
            not st.session_state["selected_eval_database"]
            or not st.session_state["selected_eval_schema"]
            or not st.session_state["selected_eval_table"]
            or not st.session_state["selected_results_eval_database"]
            or not st.session_state["selected_results_eval_schema"]
            or not st.session_state["selected_results_eval_table"]
        ):
            st.error("Please fill in all fields.")
            return

        if not validate_table_schema(
            table=st.session_state["selected_eval_table"],
            schema=EVALUATION_TABLE_SCHEMA,
        ):
            st.error(f"Evaluation table must have schema {EVALUATION_TABLE_SCHEMA}.")
            return

        if eval_results_existing_table:
            if not validate_table_schema(
                table=st.session_state["selected_results_eval_old_table"],
                schema=RESULTS_TABLE_SCHEMA,
            ):
                st.error(
                    f"Evaluation result table must have schema {RESULTS_TABLE_SCHEMA}."
                )
                return

            if not validate_table_columns(st.session_state["selected_results_eval_table"], tuple(results_table_columns.keys())):
                st.error(f"Table must have columns {list(results_table_columns.keys())}.")
                return

            with st.spinner("Creating table..."):
                success = create_table_in_schema(
                    conn=get_snowflake_connection(),
                    table_fqn=st.session_state["selected_results_eval_new_table"],
                    columns_schema=RESULTS_TABLE_SCHEMA,
                )
                if success:
                    st.success(
                        f'Table {st.session_state["selected_results_eval_new_table"]} created successfully!'
                    )
                else:
                    st.error(
                        f'Failed to create table {st.session_state["selected_results_eval_new_table"]}'
                    )
                    return

        st.session_state["eval_table_hash"] = get_table_hash(
            conn=get_snowflake_connection(),
            table_fqn=st.session_state["selected_eval_table"],
        )
        st.session_state["eval_table_frame"] = fetch_table(
            conn=get_snowflake_connection(),
            table_fqn=st.session_state["selected_eval_table"],
        ).set_index("ID")

        st.rerun()


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
            upload_yaml(file_name)
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
    """
    css_yaml_editor = """
    textarea{
        font-size: 14px;
        color: #2e2e2e;
        font-family:Menlo;
        background-color: #fbfbfb;
    }
    """

    # Style text_area to mirror st.code
    with stylable_container(key="customized_text_area", css_styles=css_yaml_editor):
        content = st.text_area(
            label="yaml_editor",
            label_visibility="collapsed",
            value=yaml_str,
            height=600,
        )
    st.session_state.working_yml = content
    status_container_title = "**Edit**"
    status_container = st.empty()

    def validate_and_update_session_state() -> None:
        # Validate new content
        try:
            validate(
                content,
                conn=get_snowflake_connection(),
            )
            st.session_state["validated"] = True
            update_container(status_container, "success", prefix=status_container_title)
            st.session_state.semantic_model = yaml_to_semantic_model(content)
            st.session_state.last_saved_yaml = content
        except Exception as e:
            st.session_state["validated"] = False
            update_container(status_container, "failed", prefix=status_container_title)
            exception_as_dialog(e)

    button_row = row(5)
    if button_row.button("Validate", use_container_width=True, help=VALIDATE_HELP):
        # Validate new content
        validate_and_update_session_state()

        # Rerun the app if validation was successful.
        # We shouldn't rerun if validation failed as the error popup would immediately dismiss.
        # This must be done outside of the try/except because the generic Exception handling is catching the
        # exception that st.rerun() properly raises to halt execution.
        # This is fixed in later versions of Streamlit, but other refactors to the code are required to upgrade.
        if st.session_state["validated"]:
            st.rerun()

    if content:
        button_row.download_button(
            label="Download",
            data=content,
            file_name="semantic_model.yaml",
            mime="text/yaml",
            use_container_width=True,
            help=UPLOAD_HELP,
        )

    if button_row.button(
        "Upload",
        use_container_width=True,
        help=UPLOAD_HELP,
    ):
        upload_dialog(content)
    if st.session_state.get("partner_setup", False):
        from partner.partner_utils import integrate_partner_semantics

        if button_row.button(
            "Integrate Partner",
            use_container_width=True,
            help=PARTNER_SEMANTIC_HELP,
            disabled=not st.session_state["validated"],
        ):
            integrate_partner_semantics()

    if st.session_state.experimental_features:
        # Preserve a session state variable that maintains whether the join dialog is open.
        # This is necessary because the join dialog calls `st.rerun()` from within, which closes the modal
        # unless its state is being tracked.
        if "join_dialog_open" not in st.session_state:
            st.session_state["join_dialog_open"] = False

        if button_row.button(
            "Join Editor",
            use_container_width=True,
        ):
            with st.spinner("Validating your model..."):
                validate_and_update_session_state()
            st.session_state["join_dialog_open"] = True

        if st.session_state["join_dialog_open"]:
            joins_dialog()

    # Render the validation state (success=True, failed=False, editing=None) in the editor.
    if st.session_state.validated:
        update_container(status_container, "success", prefix=status_container_title)
    elif st.session_state.validated is not None and not st.session_state.validated:
        update_container(status_container, "failed", prefix=status_container_title)
    else:
        update_container(status_container, "editing", prefix=status_container_title)


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

    experimental_features = st.checkbox(
        "Enable joins (optional)",
        help="Checking this box will enable you to add/edit join paths in your semantic model. If enabling this setting, please ensure that you have the proper parameters set on your Snowflake account. Reach out to your account team for access.",
    )

    # # TODOTZ - uncomment this block to use defaults for testing
    # print("USING DEFAULTS FOR TESTING")
    # st.session_state["snowflake_stage"] = SnowflakeStage(
    #     stage_database="TZAYATS",
    #     stage_schema="TZAYATS.TESTING",
    #     stage_name="TZAYATS.TESTING.MY_SEMANTIC_MODELS",
    # )
    # st.session_state["file_name"] = "revenue_timeseries_update.yaml"
    # st.session_state["page"] = GeneratorAppScreen.ITERATION
    # st.session_state["experimental_features"] = experimental_features
    # st.rerun()

    # TODOTZ - comment this block to use defaults for testing
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
        st.session_state["file_name"] = file_name
        st.session_state["page"] = GeneratorAppScreen.ITERATION
        st.session_state["experimental_features"] = experimental_features
        st.rerun()


@st.dialog("Chat Settings", width="small")
def chat_settings_dialog() -> None:
    """
    Dialog that allows user to toggle on/off certain settings about the chat experience.
    """

    debug = st.toggle(
        "Debug mode",
        value=st.session_state.chat_debug,
        help="Enable debug mode to see additional information (e.g. request ID).",
    )

    multiturn = st.toggle(
        "Multiturn",
        value=st.session_state.multiturn,
        help="Enable multiturn mode to allow the chat to remember context. Note that your account must have the correct parameters enabled to use this feature.",
    )

    if st.button("Save"):
        st.session_state.chat_debug = debug
        st.session_state.multiturn = multiturn
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


def evaluation_mode_show() -> None:
    if st.button("Set Evaluation Tables", on_click=clear_evaluation_data):
        evaluation_data_dialog()

    if "validated" in st.session_state and not st.session_state["validated"]:
        st.error("Please validate your semantic model before evaluating.")
        return

    # TODO: find a less awkward way of specifying this.
    if any(
        key not in st.session_state
        for key in ("selected_eval_table", "eval_table_hash", "eval_table_frame")
    ):
        st.error("Please set evaluation tables.")
        return

    else:
        results_table = st.session_state.get(
            "selected_results_eval_old_table"
        ) or st.session_state.get("selected_results_eval_new_table")
        summary_stats = pd.DataFrame(
            [
                ["Evaluation Table", st.session_state["selected_eval_table"]],
                ["Evaluation Result Table", results_table],
                ["Evaluation Table Hash", st.session_state["eval_table_hash"]],
                ["Semantic Model YAML Hash", hash(st.session_state["working_yml"])],
                ["Query Count", len(st.session_state["eval_table_frame"])],
            ],
            columns=["Summary Statistic", "Value"],
        )
        st.dataframe(summary_stats, hide_index=True)

        send_analyst_requests()
        run_sql_queries()
        result_comparisons()


def send_analyst_requests() -> None:
    def _get_content(
        x: dict, item_type: str, key: str, default: str = ""  # type: ignore[type-arg]
    ) -> str:
        result = next(
            (
                item[key]
                for item in x["message"]["content"]
                if item["type"] == item_type
            ),
            default,
        )
        return result

    eval_table_frame: pd.DataFrame = st.session_state["eval_table_frame"]

    total_requests = len(eval_table_frame)
    progress_bar = st.progress(0)
    status_text = st.empty()
    start_time = time.time()
    analyst_results = []

    for i, (id, row) in enumerate(eval_table_frame.iterrows(), start=1):
        status_text.text(f"Sending request {i}/{total_requests} to Analyst...")
        messages = [
            {"role": "user", "content": [{"type": "text", "text": row["QUERY"]}]}
        ]
        semantic_model = proto_to_yaml(st.session_state.semantic_model)
        try:
            response = send_message(
                _conn=get_snowflake_connection(),
                semantic_model=semantic_model,
                messages=messages,  # type: ignore[arg-type]
            )
            response_text = _get_content(response, item_type="text", key="text")
            response_sql = _get_content(response, item_type="sql", key="statement")
            analyst_results.append(
                dict(ID=id, ANALYST_TEXT=response_text, ANALYST_SQL=response_sql)
            )
        except Exception as e:
            import traceback

            st.error(f"Problem with {id}: {e} \n{traceback.format_exc()}")

        progress_bar.progress(i / total_requests)
        time.sleep(0.1)

    elapsed_time = time.time() - start_time
    status_text.text(
        f"All analyst requests received ✅ (Time taken: {elapsed_time:.2f} seconds)"
    )

    analyst_results_frame = pd.DataFrame(analyst_results).set_index("ID")
    st.session_state["analyst_results_frame"] = analyst_results_frame


def run_sql_queries() -> None:
    eval_table_frame: pd.DataFrame = st.session_state["eval_table_frame"]
    analyst_results_frame = st.session_state["analyst_results_frame"]

    total_requests = len(eval_table_frame)
    progress_bar = st.progress(0)
    status_text = st.empty()
    start_time = time.time()

    analyst_results = []
    gold_results = []

    for i, (id, row) in enumerate(eval_table_frame.iterrows(), start=1):
        status_text.text(f"Evaluating Analyst query {i}/{total_requests}...")

        analyst_query = analyst_results_frame.loc[id, "ANALYST_SQL"]
        analyst_result = execute_query(
            conn=get_snowflake_connection(), query=analyst_query
        )
        analyst_results.append(analyst_result)

        gold_query = eval_table_frame.loc[id, "GOLD_SQL"]
        gold_result = execute_query(conn=get_snowflake_connection(), query=gold_query)
        gold_results.append(gold_result)

        progress_bar.progress(i / total_requests)
        time.sleep(0.1)

    st.session_state["query_results_frame"] = pd.DataFrame(
        data=dict(ANALYST_RESULT=analyst_results, GOLD_RESULT=gold_results),
        index=eval_table_frame.index,
    )

    elapsed_time = time.time() - start_time
    status_text.text(
        f"All analyst and gold queries run ✅ (Time taken: {elapsed_time:.2f} seconds)"
    )


def _match_series(analyst_frame: pd.DataFrame, gold_series: pd.Series) -> str | None:
    """Determine which result frame column name matches the gold series.

    Args:
        analyst_frame: the data generated from the LLM constructed user query
        gold_series: a column from the data generated from the gold sql

    Returns:
        if there is a match, the results column name, if not, None
    """
    for analyst_col in analyst_frame:
        assert isinstance(analyst_col, str)
        try:
            pd.testing.assert_series_equal(
                left=analyst_frame[analyst_col],
                right=gold_series,
                check_names=False,
            )
            return analyst_col
        except AssertionError:
            pass

    return None


def _results_contain_gold_data(
    analyst_frame: pd.DataFrame,
    gold_frame: pd.DataFrame,
) -> bool:
    """Determine if result frame contains all the same values as a gold frame.

    Args:
        analyst_frame: the data generated from the LLM constructed user query
        gold_frame: the data generated from a gold sql query

    Returns:
        a boolean indicating if the results contain the gold data
    """
    if analyst_frame.shape[0] != gold_frame.shape[0]:
        return False

    unmatched_result_cols = analyst_frame.columns
    for gold_col in gold_frame:
        matching_col = _match_series(
            analyst_frame=analyst_frame[unmatched_result_cols],
            gold_series=gold_frame[gold_col],
        )
        if matching_col is None:
            return False
        else:
            unmatched_result_cols = unmatched_result_cols.drop(matching_col)

    return True


def _llm_judge(frame: pd.DataFrame) -> pd.DataFrame:
    # create prompt frame series
    table_name = "__LLM_JUDGE_TEMP_TABLE"
    col_name = "LLM_JUDGE_PROMPT"

    prompt_frame = frame.apply(
        axis=1,
        func=lambda row: LLM_JUDTE_PROMPT_TEMPLATE.format(
            input_question=row["QUERY"],
            frame1_str=row["ANALYST_RESULT"].to_string(index=False),
            frame2_str=row["GOLD_RESULT"].to_string(index=False),
        ),
    ).to_frame(name=col_name)
    conn = get_snowflake_connection()
    _ = write_pandas(
        conn=conn,
        df=prompt_frame,
        table_name=table_name,
        auto_create_table=True,
        table_type="temporary",
        overwrite=True,
    )

    query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', {col_name}) AS LLM_JUDGE
    FROM {conn.database}.{conn.schema}.{table_name}
    """
    cursor = conn.cursor()
    cursor.execute(query)
    llm_judge_frame = cursor.fetch_pandas_all()
    llm_judge_frame.index = frame.index

    reason_filter = re.compile(r"REASON\:([\S\s]*?)ANSWER\:")
    answer_filter = re.compile(r"ANSWER\:([\S\s]*?)$")

    def _safe_re_search(x, filter):  # type: ignore[no-untyped-def]
        try:
            return re.search(filter, x).group(1).strip()  # type: ignore[union-attr]
        except Exception as e:
            return f"Could Not Parse LLM Judge Response: {x}"

    llm_judge_frame["EXPLANATION"] = llm_judge_frame["LLM_JUDGE"].apply(
        _safe_re_search, args=(reason_filter,)
    )
    llm_judge_frame["CORRECT"] = (
        llm_judge_frame["LLM_JUDGE"]
        .apply(_safe_re_search, args=(answer_filter,))
        .str.lower()
        .eq("true")
    )
    return llm_judge_frame


def visualize_eval_results(frame: pd.DataFrame) -> None:
    n_questions = len(frame)
    n_correct = frame["CORRECT"].sum()
    accuracy = (n_correct / n_questions) * 100
    st.markdown(
        f"###### Results: {n_correct} out of {n_questions} questions correct with accuracy {accuracy:.2f}%"
    )
    for id, row in frame.iterrows():
        match_emoji = "✅" if row["CORRECT"] else "❌"
        with st.expander(f"Row ID: {id} {match_emoji}"):
            st.write(f"Input Query: {row['QUERY']}")
            st.write(row["ANALYST_TEXT"].replace("\n", " "))

            col1, col2 = st.columns(2)

            with col1:
                st.write("Analyst SQL")
                st.code(row["ANALYST_SQL"], language="sql", wrap_lines=True)

            with col2:
                st.write("Golden SQL")
                st.code(row["GOLD_SQL"], language="sql", wrap_lines=True)

            col1, col2 = st.columns(2)
            with col1:
                if isinstance(row["ANALYST_RESULT"], str):
                    st.error(row["ANALYST_RESULT"])
                else:
                    st.write(row["ANALYST_RESULT"])

            with col2:
                if isinstance(row["GOLD_RESULT"], str):
                    st.error(row["GOLD_RESULT"])
                else:
                    st.write(row["GOLD_RESULT"])

            st.write(f"**Explanation**: {row['EXPLANATION']}")


def result_comparisons() -> None:
    eval_table_frame: pd.DataFrame = st.session_state["eval_table_frame"]
    analyst_results_frame = st.session_state["analyst_results_frame"]
    query_results_frame = st.session_state["query_results_frame"]

    frame = pd.concat(
        [eval_table_frame, analyst_results_frame, query_results_frame], axis=1
    )

    start_time = time.time()
    status_text = st.empty()

    matches = pd.Series(False, index=frame.index)
    explanations = pd.Series("", index=frame.index)
    use_llm_judge = "<use llm judge>"

    status_text.text("Checking for exact matches...")
    for id, row in frame.iterrows():
        analyst_is_frame = isinstance(row["ANALYST_RESULT"], pd.DataFrame)
        gold_is_frame = isinstance(row["GOLD_RESULT"], pd.DataFrame)
        if (not analyst_is_frame) and (not gold_is_frame):
            matches[id] = False
            explanations[id] = dedent(
                f"""
                analyst sql had an error: {row["ANALYST_RESULT"]}
                gold sql had an error: {row["GOLD_RESULT"]}
                """
            )
        elif (not analyst_is_frame) and gold_is_frame:
            matches[id] = False
            explanations[id] = dedent(
                f"""
                analyst sql had an error: {row["ANALYST_RESULT"]}
                """
            )
        elif analyst_is_frame and (not gold_is_frame):
            matches[id] = False
            explanations[id] = dedent(
                f"""
                gold sql had an error: {row["GOLD_RESULT"]}
                """
            )
        else:
            exact_match = _results_contain_gold_data(
                analyst_frame=row["ANALYST_RESULT"], gold_frame=row["GOLD_RESULT"]
            )
            matches[id] = exact_match
            explanations[id] = "Data matches exactly" if exact_match else use_llm_judge

    frame["CORRECT"] = matches
    frame["EXPLANATION"] = explanations

    filtered_frame = frame[explanations == use_llm_judge]

    status_text.text(f"Calling LLM Judge...")
    llm_judge_frame = _llm_judge(frame=filtered_frame)

    for col in ("CORRECT", "EXPLANATION"):
        frame[col] = llm_judge_frame[col].combine_first(frame[col])

    elapsed_time = time.time() - start_time
    status_text.text(
        f"Analyst and Gold Results Compared ✅ (Time taken: {elapsed_time:.2f} seconds)"
    )

    visualize_eval_results(frame)

    frame["TIMESTAMP"] = pd.Timestamp.now()
    frame["EVAL_TABLE"] = st.session_state["selected_eval_table"]
    frame["EVAL_TABLE_HASH"] = st.session_state["eval_table_hash"]
    frame["MODEL_HASH"] = hash(st.session_state["working_yml"])

    frame = frame.reset_index()[list(RESULTS_TABLE_SCHEMA)]
    write_pandas(
        conn=get_snowflake_connection(),
        df=frame,
        table_name=st.session_state["selected_results_eval_table"],
        overwrite=False,
        quote_identifiers=False,
        auto_create_table=not st.session_state["use_existing_table"],
    )
    st.write("Evaluation results stored in the database ✅")




def show() -> None:
    init_session_states()

    if "snowflake_stage" not in st.session_state and "yaml" not in st.session_state:
        # If the user is jumping straight into the iteration flow and not coming from the builder flow,
        # we need to collect credentials and load YAML from stage.
        # If coming from the builder flow, there's no need to collect this information until the user wants to upload.
        set_up_requirements()
    else:
        home, mode = st.columns(2)
        with home:
            return_home_button()
        with mode:
            st.session_state["app_mode"] = st.selectbox(
                label="App Mode",
                label_visibility="collapsed",
                options=["Chat", "Evaluation", "Preview YAML"],
            )
        if "yaml" not in st.session_state:
            # Only proceed to download the YAML from stage if we don't have one from the builder flow.
            yaml = download_yaml(
                st.session_state.file_name, st.session_state.snowflake_stage.stage_name
            )
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
            app_mode = st.session_state["app_mode"]
            if app_mode == "Preview YAML":
                st.code(
                    st.session_state.working_yml, language="yaml", line_numbers=True
                )
            elif app_mode == "Evaluation":
                evaluation_mode_show()
            elif app_mode == "Chat":
                if st.button("Settings"):
                    chat_settings_dialog()
                # We still initialize an empty connector and pass it down in order to propagate the connector auth token.
                chat_and_edit_vqr(get_snowflake_connection())
            else:
                st.error(f"Unknown App Mode: {app_mode}")
