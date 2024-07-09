import time

import streamlit as st
from shared_utils import semantic_model_exists, stage_exists, upload_yaml

# There's no next!
st.session_state["next_is_unlocked"] = False

if semantic_model_exists() and stage_exists():
    """It's time to upload your semantic model to the stage!"""
    with st.form(key="upload", border=False):
        semantic_model_name = st.session_state.semantic_model.name
        semantic_model_name = st.session_state.semantic_model.name
        stage = st.session_state.snowflake_stage
        full_stage_name = (
            f"{stage.stage_database}.{stage.stage_schema}.{stage.stage_name}"
        )

        st.text_input("Stage name", full_stage_name, disabled=True)
        st.text_input("Model name", semantic_model_name, disabled=True)

        default_file_name = semantic_model_name.replace(" ", "_").lower() + ".yaml"
        st.session_state.file_name = st.text_input("File name", value=default_file_name)

        left, right = st.columns((1, 5.5), vertical_alignment="center")
        status = right.empty()
        if left.form_submit_button("Upload"):
            if not st.session_state.file_name.endswith(".yaml"):
                st.session_state.file_name += ".yaml"
            status.info(f"Uploading {st.session_state.file_name}...")
            time.sleep(1)
            upload_yaml(st.session_state.file_name)
            status.success("Successfully uploaded!")
else:
    st.warning(
        """Semantic model or stage are missing. Go back and make sure all steps
               are correctly done."""
    )
