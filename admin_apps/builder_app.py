import streamlit as st

from admin_apps.shared_utils import GeneratorAppScreen
from semantic_model_generator.generate_model import generate_model_str_from_snowflake


@st.experimental_dialog("Selecting your tables", width="large")
def table_selector_dialog() -> None:
    """
    Renders a dialog box for the user to input the tables they want to use in their semantic model.
    """

    st.write(
        "Please fill out the following fields to start building your semantic model."
    )
    with st.form(key="Builder form"):
        model_name = st.text_input("Semantic Model Name")
        sample_values = st.selectbox(
            "Number of sample values", list(range(1, 11)), index=0
        )
        tables = st.text_input(
            "Fully qualified table names",
            help="Please separate table names with commas.",
        )

        submit = st.form_submit_button("Submit")
        if submit:
            with st.spinner("Generating model..."):
                yaml_str = generate_model_str_from_snowflake(
                    base_tables=[table.strip() for table in tables.split(",")],
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
