import os

import streamlit as st

from admin_apps import builder_app, iteration_app


def onboarding_dialog() -> None:
    st.write(
        "Welcome to the Semantic Model Generator! Here, we can help you create or edit a semantic model. What would you like to do?"
    )
    if st.button("Create a new semantic model"):
        st.session_state["page"] = "builder"
        st.rerun()
    elif st.button("Edit an existing semantic model"):
        st.session_state["page"] = "iteration"
        st.rerun()


def set_up_common_requirements() -> None:
    """
    Populates common state between builder and iteration apps, e.g. the snowflake account and host to use.
    """

    st.session_state["account_name"] = os.environ.get("SNOWFLAKE_ACCOUNT_LOCATOR")
    st.session_state["host_name"] = os.environ.get("SNOWFLAKE_HOST")
    st.session_state["user_name"] = os.environ.get("SNOWFLAKE_USER")


set_up_common_requirements()

if "page" not in st.session_state:
    st.session_state["page"] = "onboarding"
if st.session_state["page"] == "builder":
    builder_app.show()
if st.session_state["page"] == "iteration":
    iteration_app.show()
else:
    onboarding_dialog()
