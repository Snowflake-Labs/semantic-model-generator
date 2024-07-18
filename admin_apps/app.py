import os

import streamlit as st

from admin_apps.shared_utils import GeneratorAppScreen

# set_page_config must be run as the first Streamlit command on the page, before any other streamlit imports.
st.set_page_config(layout="wide", page_icon="💬", page_title="Semantic Model Generator")


if __name__ == "__main__":
    from admin_apps import builder_app, iteration_app

    def onboarding_dialog() -> None:
        st.write(
            "Welcome to the Semantic Model Generator! Here, we can help you create or edit a semantic model. What would you like to do?"
        )
        if st.button("Create a new semantic model"):
            st.session_state["page"] = GeneratorAppScreen.BUILDER
            st.rerun()
        elif st.button("Edit an existing semantic model"):
            st.session_state["page"] = GeneratorAppScreen.ITERATION
            st.rerun()

    # Populating common state between builder and iteration apps.
    st.session_state["account_name"] = os.environ.get("SNOWFLAKE_ACCOUNT_LOCATOR")
    st.session_state["host_name"] = os.environ.get("SNOWFLAKE_HOST")
    st.session_state["user_name"] = os.environ.get("SNOWFLAKE_USER")

    if "page" not in st.session_state:
        st.session_state["page"] = GeneratorAppScreen.ONBOARDING

    if st.session_state["page"] == GeneratorAppScreen.BUILDER:
        builder_app.show()
    if st.session_state["page"] == GeneratorAppScreen.ITERATION:
        iteration_app.show()
    else:
        onboarding_dialog()
