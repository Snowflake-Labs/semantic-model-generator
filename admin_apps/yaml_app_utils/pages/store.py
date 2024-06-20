import time

import streamlit as st

from shared_utils import stage_exists, SnowflakeStage

# In case the stage was filled,
# the next page should be available already.
st.session_state["next_is_unlocked"] = stage_exists()

st.markdown(
    """Please point to an existing Snowflake stage for storing your semantic model. Note that your current role must have write access to the stage."""
)


with st.form(key="storage", border=False):
    # col1, col2, col3 = st.columns(3)
    # with col1:
    stage_database = st.text_input("Database", key="stage_database_key", value="SNOWFLAKE_SEMANTIC_CONTEXT")
    # with col2:
    stage_schema = st.text_input("Schema", key="stage_schema_key", value="DEFINITIONS")
    # with col3:
    stage_name = st.text_input("Stage", key="stage_name_key", value="TEST")

    left, right = st.columns((1, 5), vertical_alignment="center")
    save = left.form_submit_button("Set stage")

# Display the saved values from session state
if save:

    status = right.empty()
    status.info("Processing...")

    if not (
        len(stage_database) > 0 and len(stage_schema) > 0 and len(stage_name) > 0
    ):
        st.error("Database, schema and stage names can not be empty.")
        st.stop()

    st.session_state.snowflake_stage = SnowflakeStage(
        stage_database, stage_schema, stage_name
    )


    time.sleep(1)
    status.success("Successfully saved stage.")
    st.session_state["next_is_unlocked"] = True
    time.sleep(1)
    st.rerun()
