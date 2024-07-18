import os

import streamlit as st

from admin_apps.shared_utils import GeneratorAppScreen

# set_page_config must be run as the first Streamlit command on the page, before any other streamlit imports.
st.set_page_config(layout="wide", page_icon="💬", page_title="Semantic Model Generator")


if __name__ == "__main__":
    from admin_apps import builder_app, iteration_app

    def onboarding_dialog() -> None:
        """
        Renders the initial screen where users can choose to create a new semantic model or edit an existing one.
        """
        st.markdown(
            """
                <div style="text-align: center;">
                    <h1>Welcome to the Snowflake Semantic Model Generator! ❄️</h1>
                    <p>Let's get started. What would you like to do?</p>
                </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("<div style='margin: 20px;'></div>", unsafe_allow_html=True)

        _, center, _ = st.columns([1, 2, 1])
        with center:
            if st.button(
                "**🛠 Create a new semantic model**",
                use_container_width=True,
                type="primary",
            ):
                builder_app.show()
            st.write("")
            if st.button(
                "**✏️ Edit an existing semantic model**",
                use_container_width=True,
                type="primary",
            ):
                iteration_app.show()

    # Populating common state between builder and iteration apps.
    st.session_state["account_name"] = os.environ.get("SNOWFLAKE_ACCOUNT_LOCATOR")
    st.session_state["host_name"] = os.environ.get("SNOWFLAKE_HOST")
    st.session_state["user_name"] = os.environ.get("SNOWFLAKE_USER")

    # When the app first loads, show the onboarding screen.
    if "page" not in st.session_state:
        st.session_state["page"] = GeneratorAppScreen.ONBOARDING

    # Depending on the page state, we either show the onboarding menu or the chat app flow.
    # The builder flow is simply an intermediate dialog before the iteration flow.
    if st.session_state["page"] == GeneratorAppScreen.ITERATION:
        iteration_app.show()
    else:
        onboarding_dialog()
