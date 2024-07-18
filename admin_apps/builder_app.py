import streamlit as st

from semantic_model_generator.generate_model import (
    generate_and_validate_model_from_snowflake,
)


@st.experimental_dialog("Selecting your tables", width="large")
def table_selector_dialog() -> None:
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
            # Set common state variables with iteration app, in order to prevent dupe inputs.
            with st.spinner("Generating model..."):
                yaml_str = generate_and_validate_model_from_snowflake(
                    base_tables=[table.strip() for table in tables.split(",")],
                    snowflake_account=st.session_state["account_name"],
                    semantic_model_name=model_name,
                    n_sample_values=sample_values,  # type: ignore
                )

                st.session_state["yaml"] = yaml_str
                st.session_state["page"] = "iteration"
                st.rerun()


def show() -> None:
    table_selector_dialog()
