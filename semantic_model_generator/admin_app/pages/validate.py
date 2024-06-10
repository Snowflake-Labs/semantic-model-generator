import streamlit as st
from app_utils import semantic_model_exists, validate_and_upload_tmp_yaml

from semantic_model_generator.data_processing.proto_utils import proto_to_yaml


def show_and_validate_yaml() -> None:
    """
    Renders a page to display the yaml form of the semantic model. Allow click at "validate" button for validation.
    Note: "validate" do not include validation for verified queries. TODO (renee) add verification for verified queries into OSS.
    Technically, if users added verified queries from the chat interface, there should be no concerns.
    However, if users updated any tables/columns definitions after adding verified queries, it may cause an issue.
    """
    yaml = proto_to_yaml(st.session_state.semantic_model)

    with st.container(height=500):
        st.caption("YAML content:")
        st.code(
            yaml,
            language="yaml",
            line_numbers=True,
        )

    if st.button("Validate", key="validate_model", use_container_width=True):
        if "snowflake_stage" not in st.session_state:
            st.warning("Please enter stage path in the main page!")
        validate_and_upload_tmp_yaml()


if semantic_model_exists():
    show_and_validate_yaml()
else:
    st.error("No model found.")
