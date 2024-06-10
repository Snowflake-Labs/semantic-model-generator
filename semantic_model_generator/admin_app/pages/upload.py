import streamlit as st
from app_utils import semantic_model_exists, user_upload_yaml

if not semantic_model_exists():
    st.error("No model found.")
    st.stop()
else:
    semantic_model_name = st.session_state.semantic_model.name

if "snowflake_stage" in st.session_state:
    stage = st.session_state.snowflake_stage
    full_stage_name = f"{stage.stage_database}.{stage.stage_schema}.{stage.stage_name}"

    disabled = st.session_state.validated is not True
    if st.button(
        f"Upload semantic model `{semantic_model_name}` to stage `{full_stage_name}`",
        key="upload_model",
        disabled=disabled,
        help="⚠️ **Model is not validated! Go back to 'Validate'**"
        if disabled
        else None,
    ):
        user_upload_yaml()

else:
    st.stop()

if "semantic_model" in st.session_state:
    semantic_model_name = st.session_state.semantic_model.name
    if semantic_model_name == "":
        st.error("No semantic model defined. Go back to **Create**.")
        st.stop()
else:
    st.error("No semantic model defined. Go back to **Create**.")
    st.stop()
