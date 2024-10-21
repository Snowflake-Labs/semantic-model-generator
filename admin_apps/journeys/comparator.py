import streamlit as st
from admin_apps.shared_utils import GeneratorAppScreen
from admin_apps.shared_utils import return_home_button
from admin_apps.shared_utils import download_yaml_fqn
from semantic_model_generator.data_processing.proto_utils import yaml_to_semantic_model, proto_to_yaml
from streamlit_monaco import st_monaco

MODEL1_PATH = "model1_path"
MODEL1_YAML = "model1_yaml"
MODEL2_PATH = "model2_path"
MODEL2_YAML = "model2_yaml"


def init_session_states() -> None:
    st.session_state["page"] = GeneratorAppScreen.COMPARATOR


def comparator_app() -> None:
    st.write(
        """
        ## Compare two semantic models
        """
    )
    col1, col2 = st.columns(2)
    with col1:
        st.write(f"Model 1 from: `{st.session_state[MODEL1_PATH]}`")
        content1 = st_monaco(
            value=st.session_state[MODEL1_YAML],
            height="400px",
            language="yaml",
        )

    with col2:
        st.write(f"Model 2 from: `{st.session_state[MODEL2_PATH]}`")
        content2 = st_monaco(
            value=st.session_state[MODEL2_YAML],
            height="400px",
            language="yaml",
        )

    # TODO:
    # - Compare the two models
    # - Show the differences
    # - Validation of the models
    # - Check if both models are pointing at the same table
    # - dialog to ask questions
    # - Results of the cortex analyst with both models

    return_home_button()


def is_session_state_initialized() -> bool:
    return all([
        MODEL1_YAML in st.session_state,
        MODEL2_YAML in st.session_state,
        MODEL1_PATH in st.session_state,
        MODEL2_PATH in st.session_state,
    ])


def show() -> None:
    init_session_states()
    if is_session_state_initialized():
        comparator_app()
    else:
        init_dialog()


def read_semantic_model(model_path: str) -> str:
    """Read the semantic model from the given path (local or snowflake stage).
    
    Args:
        model_path (str): The path to the semantic model.

    Returns:
        str: The semantic model as a string.

    Raises:
        FileNotFoundError: If the model is not found.
    """
    if model_path.startswith('@'):
        return download_yaml_fqn(model_path)
    else:
        with open(model_path, "r") as f:
            return f.read()


@st.dialog("Welcome to the Cortex Analyst Annotation Workspace! 📝", width="large")
def init_dialog() -> None:
    init_session_states()

    st.write("Please enter the paths (local or stage) of the two models you would like to compare.")

    model_1_path = st.text_input("Model 1", placeholder="e.g. /local/path/to/model1.yaml")
    model_2_path = st.text_input("Model 2", placeholder="e.g. @DATABASE.SCHEMA.STAGE_NAME/path/to/model2.yaml")

    if st.button("Compare"):
        model_1_yaml = model_2_yaml = None
        try:
            model_1_yaml = read_semantic_model(model_1_path)
        except FileNotFoundError as e:
            st.error(f"Model 1 not found: {e}")
        try:
            model_2_yaml = read_semantic_model(model_2_path)
        except FileNotFoundError as e:
            st.error(f"Model 2 not found: {e}")

        if model_1_yaml and model_2_yaml:
            st.session_state[MODEL1_PATH] = model_1_path
            st.session_state[MODEL1_YAML] = model_1_yaml
            st.session_state[MODEL2_PATH] = model_2_path
            st.session_state[MODEL2_YAML] = model_2_yaml
            st.rerun()

    return_home_button()
