import streamlit as st
from semantic_model_generator.data_processing.proto_utils import proto_to_yaml
from shared_utils import semantic_model_exists, validate_and_upload_tmp_yaml, model_is_validated

# In case the model is already validated,
# the next page should be available already.
st.session_state["next_is_unlocked"] = model_is_validated()


def show_and_validate_yaml() -> None:
    """
    Renders a page to display the yaml form of the semantic model. Allow click at "validate" button for validation.
    Note: "validate" do not include validation for verified queries. TODO (renee) add verification for verified queries into OSS.
    Technically, if users added verified queries from the chat interface, there should be no concerns.
    However, if users updated any tables/columns definitions after adding verified queries, it may cause an issue.
    """
    yaml = proto_to_yaml(st.session_state.semantic_model)

    with st.container(height=400, border=False):
        st.code(
            yaml,
            language="yaml",
            line_numbers=True,
        )

    left, right = st.columns((2, 10), vertical_alignment="center")
    status = right.empty()
    if left.button("Validate", key="validate_model"):
        if "snowflake_stage" not in st.session_state:
            right.warning("Please enter stage path in the main page!")
            st.stop()
        status.info("Processing...")
        with status:
            validate_and_upload_tmp_yaml()

""" In this step, you can preview your YAML and check if it's valid."""

if semantic_model_exists():
    show_and_validate_yaml()
else:
    st.error("No model found.")
