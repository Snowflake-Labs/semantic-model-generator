import streamlit as st

from admin_apps.journeys.iteration import get_connector
from admin_apps.shared_utils import GeneratorAppScreen
from semantic_model_generator.generate_model import generate_model_str_from_snowflake
from semantic_model_generator.snowflake_utils.snowflake_connector import (
    fetch_table_names,
)

connector = get_connector()


@st.cache_resource(show_spinner=False)
def get_available_tables() -> list[str]:
    """
    Simple wrapper around fetch_table_names to cache the results.

    Returns: list of fully qualified table names
    """

    with connector.connect(db_name="") as conn:
        return fetch_table_names(conn)


@st.experimental_dialog("Selecting your tables", width="large")
def table_selector_dialog() -> None:
    """
    Renders a dialog box for the user to input the tables they want to use in their semantic model.
    """

    st.write(
        "Please fill out the following fields to start building your semantic model."
    )
    with st.form("table_selector_form"):
        model_name = st.text_input("Semantic Model Name")
        sample_values = st.selectbox(
            "Number of sample values", list(range(1, 11)), index=0
        )
        st.markdown("")

        if "available_tables" not in st.session_state:
            with st.spinner("Loading table definitions..."):
                st.session_state["available_tables"] = get_available_tables()

        tables = st.multiselect(
            label="Tables",
            options=st.session_state["available_tables"],
            placeholder="Select the tables you'd like to include in your semantic model.",
        )
        st.markdown("<div style='margin: 240px;'></div>", unsafe_allow_html=True)
        submit = st.form_submit_button(
            "Submit", use_container_width=True, type="primary"
        )
        if submit:
            if not tables:
                st.error("Please select at least one table to proceed.")
            else:
                with st.spinner("Generating model..."):
                    yaml_str = generate_model_str_from_snowflake(
                        base_tables=tables,
                        snowflake_account=st.session_state["account_name"],
                        semantic_model_name=model_name,
                        n_sample_values=sample_values,  # type: ignore
                    )

                    # Set the YAML session state so that the iteration app has access to the generated contents,
                    # then proceed to the iteration screen.
                    st.session_state["yaml"] = yaml_str
                    st.session_state["page"] = GeneratorAppScreen.ITERATION
                    st.rerun()


def show() -> None:
    table_selector_dialog()
