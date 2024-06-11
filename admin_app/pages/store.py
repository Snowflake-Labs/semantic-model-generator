import time
from dataclasses import dataclass

import streamlit as st


@dataclass
class SnowflakeStage:
    stage_database: str
    stage_schema: str
    stage_name: str


""" ### Set destination stage """
st.markdown(
    """Please point to an **existing Snowflake stage** for storing your semantic model.

Note that your current role must have write access to the stage."""
)

with st.form(key="storage", border=True):
    col1, col2, col3 = st.columns(3)
    with col1:
        stage_database = st.text_input("Database", key="stage_database_key")
    with col2:
        stage_schema = st.text_input("Schema", key="stage_schema_key")
    with col3:
        stage_name = st.text_input("Stage", key="stage_name_key")

    save = st.form_submit_button("Set stage")

# Display the saved values from session state
if save:
    with st.status("Setting stage...", expanded=True) as status:
        st.markdown("Checking that names are valid... 🔎")
        if not (
            len(stage_database) > 0 and len(stage_schema) > 0 and len(stage_name) > 0
        ):
            st.error("Database, schema and stage names can not be empty.")
            status.update(state="error", expanded=True)
            st.stop()

        time.sleep(1.5)

        st.markdown("Saving values for session... 💾")
        st.session_state.snowflake_stage = SnowflakeStage(
            stage_database, stage_schema, stage_name
        )
        time.sleep(1.5)

        status.update(
            label="**Stage successfully set!** Applying changes...",
            state="complete",
            expanded=False,
        )

        time.sleep(1.5)
        st.rerun()
