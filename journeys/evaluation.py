import hashlib
import re
import time
from textwrap import dedent
from typing import Any

import pandas as pd
import snowflake.snowpark._internal.utils as snowpark_utils
import sqlglot
import streamlit as st
import yaml
from loguru import logger
from snowflake.connector.pandas_tools import write_pandas

from app_utils.chat import send_message
from app_utils.shared_utils import (
    get_snowflake_connection,
    schema_selector_container,
    set_sit_query_tag,
    table_selector_container,
    update_last_validated_model,
    validate_table_exist,
    validate_table_schema,
)
from semantic_model_generator.data_processing.proto_utils import proto_to_yaml
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
    "TIMESTAMP": "TIMESTAMP_NTZ",
    "ID": "VARCHAR",
    "QUERY": "VARCHAR",
    "ANALYST_TEXT": "VARCHAR",
    "ANALYST_SQL": "VARCHAR",
    "ANALYST_RESULT": "VARCHAR",
    "GOLD_SQL": "VARCHAR",
    "GOLD_RESULT": "VARCHAR",
    "CORRECT": "BOOLEAN",
    "EXPLANATION": "VARCHAR",
    "MODEL_HASH": "VARCHAR",
    "SEMANTIC_MODEL_STRING": "VARCHAR",
    "EVAL_TABLE": "VARCHAR",
    "EVAL_HASH": "VARCHAR",
    "EVAL_RUN_NAME": "VARCHAR",
}

LLM_JUDGE_PROMPT_TEMPLATE = """\
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


def visualize_eval_results(frame: pd.DataFrame) -> None:
    n_questions = len(frame)
    n_correct = frame["CORRECT"].sum()
    accuracy = (n_correct / n_questions) * 100
    results_placeholder = st.session_state.get("eval_results_placeholder")
    with results_placeholder.container():
        st.markdown(
            f"###### Results: {n_correct} out of {n_questions} questions correct with accuracy {accuracy:.2f}%"
        )
        for row_id, row in frame.iterrows():
            match_emoji = "✅" if row["CORRECT"] else "❌"
            with st.expander(f"Row ID: {row_id} {match_emoji}"):
                st.write(f"Input Query: {row['QUERY']}")
                st.write(row["ANALYST_TEXT"].replace("\n", " "))

                col1, col2 = st.columns(2)

                try:
                    analyst_sql = sqlglot.parse_one(
                        row["ANALYST_SQL"], dialect="snowflake"
                    )
                    analyst_sql = analyst_sql.sql(dialect="snowflake", pretty=True)
                except Exception as e:
                    logger.warning(f"Error parsing analyst SQL: {e} for {row_id}")
                    analyst_sql = row["ANALYST_SQL"]

                try:
                    gold_sql = sqlglot.parse_one(row["GOLD_SQL"], dialect="snowflake")
                    gold_sql = gold_sql.sql(dialect="snowflake", pretty=True)
                except Exception as e:
                    logger.warning(f"Error parsing gold SQL: {e} for {row_id}")
                    gold_sql = row["GOLD_SQL"]

                with col1:
                    st.write("Analyst SQL")
                    st.code(analyst_sql, language="sql")

                with col2:
                    st.write("Golden SQL")
                    st.code(gold_sql, language="sql")

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


def _llm_judge(frame: pd.DataFrame, max_frame_size=200) -> pd.DataFrame:

    if frame.empty:
        return pd.DataFrame({"EXPLANATION": [], "CORRECT": []})

    # create prompt frame series
    col_name = "LLM_JUDGE_PROMPT"

    prompt_frame = frame.apply(
        axis=1,
        func=lambda x: LLM_JUDGE_PROMPT_TEMPLATE.format(
            input_question=x["QUERY"],
            frame1_str=x["ANALYST_RESULT"][:max_frame_size].to_string(index=False),
            frame2_str=x["GOLD_RESULT"][:max_frame_size].to_string(index=False),
        ),
    ).to_frame(name=col_name)
    session = st.session_state["session"]
    table_name = snowpark_utils.random_name_for_temp_object(
        snowpark_utils.TempObjectType.TABLE
    )
    conn = get_snowflake_connection()
    snowpark_df = session.create_dataframe(prompt_frame)
    snowpark_df.write.mode("overwrite").save_as_table(
        table_name, table_type="temporary"
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
            return f"Could Not Parse LLM Judge Response: {x} with error: {e}"

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
    for row_id, row in frame.iterrows():
        analyst_is_frame = isinstance(row["ANALYST_RESULT"], pd.DataFrame)
        gold_is_frame = isinstance(row["GOLD_RESULT"], pd.DataFrame)
        if (not analyst_is_frame) and (not gold_is_frame):
            matches[row_id] = False
            explanations[row_id] = dedent(
                f"""
                analyst sql had an error: {row["ANALYST_RESULT"]}
                gold sql had an error: {row["GOLD_RESULT"]}
                """
            )
        elif (not analyst_is_frame) and gold_is_frame:
            matches[row_id] = False
            explanations[row_id] = dedent(
                f"""
                analyst sql had an error: {row["ANALYST_RESULT"]}
                """
            )
        elif analyst_is_frame and (not gold_is_frame):
            matches[row_id] = False
            explanations[row_id] = dedent(
                f"""
                gold sql had an error: {row["GOLD_RESULT"]}
                """
            )
        else:
            exact_match = _results_contain_gold_data(
                analyst_frame=row["ANALYST_RESULT"],
                gold_frame=row["GOLD_RESULT"],
            )
            matches[row_id] = exact_match
            explanations[row_id] = (
                "Data matches exactly" if exact_match else use_llm_judge
            )

    frame["CORRECT"] = matches
    frame["EXPLANATION"] = explanations

    filtered_frame = frame[explanations == use_llm_judge]

    status_text.text("Calling LLM Judge...")
    llm_judge_frame = _llm_judge(frame=filtered_frame)

    for col in ("CORRECT", "EXPLANATION"):
        frame[col] = llm_judge_frame[col].combine_first(frame[col])

    elapsed_time = time.time() - start_time
    status_text.text(
        f"Analyst and Gold Results Compared ✅ (Time taken: {elapsed_time:.2f} seconds)"
    )
    # compute accuracy
    st.session_state["eval_accuracy"] = (frame["CORRECT"].sum() / len(frame)) * 100
    st.session_state["total_eval_frame"] = frame


def write_eval_results(frame: pd.DataFrame) -> None:
    frame_to_write = frame.copy()
    frame_to_write["TIMESTAMP"] = st.session_state["eval_timestamp"]
    frame_to_write["EVAL_HASH"] = st.session_state["eval_hash"]
    frame_to_write["EVAL_RUN_NAME"] = st.session_state["eval_run_name"]
    frame_to_write["EVAL_TABLE"] = st.session_state["eval_table"]
    frame_to_write["EVAL_TABLE_HASH"] = st.session_state["eval_table_hash"]
    frame_to_write["MODEL_HASH"] = st.session_state["semantic_model_hash"]

    # Save results to frame as string
    frame_to_write["ANALYST_RESULT"] = frame["ANALYST_RESULT"].apply(
        lambda x: x.to_json(orient="records") if isinstance(x, pd.DataFrame) else x
    )
    frame_to_write["GOLD_RESULT"] = frame["GOLD_RESULT"].apply(
        lambda x: x.to_json(orient="records") if isinstance(x, pd.DataFrame) else x
    )
    frame_to_write["SEMANTIC_MODEL_STRING"] = st.session_state["working_yml"]

    frame_to_write = frame_to_write.reset_index()[list(RESULTS_TABLE_SCHEMA)]
    write_pandas(
        conn=get_snowflake_connection(),
        df=frame_to_write,
        table_name=st.session_state["results_eval_table"],
        overwrite=False,
        quote_identifiers=False,
        auto_create_table=False,
    )
    st.write("Evaluation results stored in the database ✅")


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


def run_sql_queries() -> None:
    eval_table_frame: pd.DataFrame = st.session_state["eval_table_frame"]
    analyst_results_frame = st.session_state["analyst_results_frame"]

    total_requests = len(eval_table_frame)
    progress_bar = st.progress(0)
    status_text = st.empty()
    start_time = time.time()

    analyst_results = []
    gold_results = []

    for i, (row_id, row) in enumerate(eval_table_frame.iterrows(), start=1):
        status_text.text(f"Evaluating Analyst query {i}/{total_requests}...")

        analyst_query = analyst_results_frame.loc[row_id, "ANALYST_SQL"]
        analyst_result = execute_query(
            conn=get_snowflake_connection(), query=analyst_query
        )
        analyst_results.append(analyst_result)

        gold_query = eval_table_frame.loc[row_id, "GOLD_SQL"]
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

    for i, (row_id, row) in enumerate(eval_table_frame.iterrows(), start=1):
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
                dict(ID=row_id, ANALYST_TEXT=response_text, ANALYST_SQL=response_sql)
            )
        except Exception as e:
            import traceback

            st.error(f"Problem with {row_id}: {e} \n{traceback.format_exc()}")

        progress_bar.progress(i / total_requests)
        time.sleep(0.1)

    elapsed_time = time.time() - start_time
    status_text.text(
        f"All analyst requests received ✅ (Time taken: {elapsed_time:.2f} seconds)"
    )

    analyst_results_frame = pd.DataFrame(analyst_results).set_index("ID")
    st.session_state["analyst_results_frame"] = analyst_results_frame


@st.experimental_dialog("Evaluation Tables", width="large")
def evaluation_data_dialog() -> None:
    st.markdown("Please select an evaluation table.")
    st.markdown("The evaluation table should have the following schema:")
    eval_table_schema_explained = pd.DataFrame(
        [
            ["ID", "VARCHAR", "Unique identifier for each row"],
            ["QUERY", "VARCHAR", "The query to be evaluated"],
            ["GOLD_SQL", "VARCHAR", "The expected SQL for the query"],
        ],
        columns=["Column", "Type", "Description"],
    )
    st.dataframe(eval_table_schema_explained, hide_index=True)
    table_selector_container(
        db_selector={"key": "selected_eval_database", "label": "Evaluation database"},
        schema_selector={"key": "selected_eval_schema", "label": "Evaluation schema"},
        table_selector={"key": "selected_eval_table", "label": "Evaluation table"},
    )

    st.divider()

    st.markdown("Please select a results table.")
    eval_results_existing_table = st.checkbox(
        "Use existing table", key="use_existing_eval_results_table"
    )

    if not eval_results_existing_table:
        schema_selector_container(
            db_selector={
                "key": "selected_results_eval_database",
                "label": "Results database",
            },
            schema_selector={
                "key": "selected_results_eval_schema",
                "label": "Results schema",
            },
        )

        original_new_table_name = st.text_input(
            key="selected_results_eval_new_table_no_schema",
            label="Enter the table name to upload evaluation results.",
        )
        if original_new_table_name:
            schema_name = st.session_state.get("selected_results_eval_schema")
            updated_new_table_name = f"{schema_name}.{original_new_table_name}".upper()
            st.session_state["selected_results_eval_new_table"] = updated_new_table_name
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

        else:
            if validate_table_exist(
                schema=st.session_state["selected_results_eval_schema"],
                table_name=st.session_state[
                    "selected_results_eval_new_table_no_schema"
                ],
            ):
                st.error("Results table already exists")
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

        st.session_state["eval_table"] = st.session_state["selected_eval_table"]
        st.session_state["results_eval_table"] = st.session_state[
            "selected_results_eval_table"
        ]
        clear_evaluation_data()

        st.rerun()


def clear_evaluation_selection() -> None:
    session_states = (
        "selected_eval_database",
        "selected_eval_schema",
        "selected_eval_table",
        "selected_results_eval_database",
        "selected_results_eval_new_table",
        "selected_results_eval_new_table_no_schema",
        "selected_results_eval_old_table",
        "selected_results_eval_schema",
        "use_existing_eval_results_table",
        "selected_eval_run_name",
    )
    for feature in session_states:
        if feature in st.session_state:
            del st.session_state[feature]


def clear_evaluation_data() -> None:
    session_states = (
        "total_eval_frame",
        "eval_accuracy",
        "analyst_results_frame",
        "query_results_frame",
        "eval_run_name",
        "eval_timestamp",
        "eval_hash",
    )
    for feature in session_states:
        if feature in st.session_state:
            del st.session_state[feature]


def evaluation_mode_show() -> None:

    if st.button("Select Evaluation Tables", on_click=clear_evaluation_selection):
        evaluation_data_dialog()

    st.write(
        "Welcome!🧪 In the evaluation mode you can evaluate your semantic model using pairs of golden queries/questions and their expected SQL statements. These pairs should be captured in an **Evaluation Table**. Accuracy metrics will be shown and the results will be stored in an **Evaluation Results Table**."
    )
    st.text_input(
        "Evaluation Run Name",
        key="selected_eval_run_name",
        value=st.session_state.get("selected_eval_run_name", ""),
    )

    # TODO: find a less awkward way of specifying this.
    if any(key not in st.session_state for key in ("eval_table", "results_eval_table")):
        st.error("Please select evaluation tables.")
        return

    summary_stats = pd.DataFrame(
        [
            ["Evaluation Table", st.session_state["eval_table"]],
            ["Evaluation Result Table", st.session_state["results_eval_table"]],
            ["Query Count", len(st.session_state["eval_table_frame"])],
        ],
        columns=["Summary Statistic", "Value"],
    )
    st.markdown("#### Evaluation Data Summary")
    st.dataframe(summary_stats, hide_index=True)
    if st.button("Run Evaluation"):
        run_evaluation()

    if "total_eval_frame" in st.session_state:
        current_hash = generate_hash(st.session_state["working_yml"])
        model_changed_test = current_hash != st.session_state["semantic_model_hash"]

        evolution_run_summary = pd.DataFrame(
            [
                ["Evaluation Run Name", st.session_state["eval_run_name"]],
                ["Evaluation Table Hash", st.session_state["eval_table_hash"]],
                ["Semantic Model Hash", st.session_state["semantic_model_hash"]],
                ["Evaluation Run Hash", st.session_state["eval_hash"]],
                ["Timestamp", st.session_state["eval_timestamp"]],
                ["Accuracy", f"{st.session_state['eval_accuracy']:.2f}%"],
            ],
            columns=["Summary Statistic", "Value"],
        )
        if model_changed_test:
            st.warning("Model has changed since last evaluation run.")
            st.markdown("#### Previous Evaluation Run Summary")
        else:
            st.markdown("#### Current Evaluation Run Summary")
        st.dataframe(evolution_run_summary, hide_index=True)
        st.session_state["eval_results_placeholder"] = st.empty()
        visualize_eval_results(st.session_state["total_eval_frame"])


def run_evaluation() -> None:
    set_sit_query_tag(
        get_snowflake_connection(),
        vendor="",
        action="evaluation_run",
    )
    current_hash = generate_hash(st.session_state["working_yml"])
    model_changed_test = ("semantic_model_hash" not in st.session_state) or (
        current_hash != st.session_state["semantic_model_hash"]
    )
    placeholder = st.empty()

    if not model_changed_test and "total_eval_frame" in st.session_state:
        placeholder.write("Model has not changed since last evaluation run.")
        return

    if not st.session_state.validated or model_changed_test:
        placeholder.write("Validating model...")
        try:
            # try loading the yaml
            _ = yaml.safe_load(st.session_state["working_yml"])
            # try validating the yaml using analyst
            validate(st.session_state["working_yml"], get_snowflake_connection())
            st.session_state.validated = True
            update_last_validated_model()
        except Exception as e:
            placeholder.error(f"Could not validate model ❌ with error: {e}")
            return
    placeholder.write("Model validated ✅")
    clear_evaluation_data()
    st.session_state["eval_run_name"] = st.session_state["selected_eval_run_name"]
    st.session_state["semantic_model_hash"] = current_hash
    if st.session_state["eval_run_name"] == "":
        st.write("Running evaluation ...")
    else:
        st.write(
            f"Running evaluation for name: {st.session_state['eval_run_name']} ..."
        )
    if "eval_results_placeholder" in st.session_state:
        results_placeholder = st.session_state["eval_results_placeholder"]
        results_placeholder.empty()
    st.session_state["eval_timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    st.session_state["eval_hash"] = generate_hash(st.session_state["eval_timestamp"])
    send_analyst_requests()
    run_sql_queries()
    result_comparisons()
    write_eval_results(st.session_state["total_eval_frame"])
    st.write("Evaluation complete ✅")


@st.cache_resource(show_spinner=False)
def generate_hash(input_object: Any) -> str:
    output_hash = ""
    try:
        text = str(input_object)
        output_hash = hashlib.md5(text.encode()).hexdigest()
    except Exception as e:
        logger.error(f"Error generating hash: {e}")
        output_hash = str(hash(input_object))
    return output_hash
