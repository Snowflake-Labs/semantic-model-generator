import streamlit as st
from snowflake.connector import DatabaseError
from snowflake.connector.connection import SnowflakeConnection

# set_page_config must be run as the first Streamlit command on the page, before any other streamlit imports.
st.set_page_config(layout="wide", page_icon="💬", page_title="Semantic Model Generator")

from app_utils.shared_utils import (  # noqa: E402
    GeneratorAppScreen,
    get_snowflake_connection,
    set_account_name,
    set_host_name,
    set_sit_query_tag,
    set_snowpark_session,
    set_streamlit_location,
    set_user_name,
)
from semantic_model_generator.snowflake_utils.env_vars import (  # noqa: E402
    SNOWFLAKE_ACCOUNT_LOCATOR,
    SNOWFLAKE_HOST,
    SNOWFLAKE_USER,
)


@st.experimental_dialog(title="Connection Error")
def failed_connection_popup() -> None:
    """
    Renders a dialog box detailing that the credentials provided could not be used to connect to Snowflake.
    """
    st.markdown(
        """It looks like the credentials provided could not be used to connect to the account."""
    )
    st.stop()


def verify_environment_setup() -> SnowflakeConnection:
    """
    Ensures that the correct environment variables are set before proceeding.
    """

    # Instantiate the Snowflake connection that gets reused throughout the app.
    try:
        with st.spinner(
            "Validating your connection to Snowflake. If you are using MFA, please check your authenticator app for a push notification."
        ):
            return get_snowflake_connection()
    except DatabaseError:
        failed_connection_popup()


if __name__ == "__main__":
    from journeys import builder, iteration, partner

    st.session_state["sis"] = set_streamlit_location()

    def onboarding_dialog() -> None:
        """
        Renders the initial screen where users can choose to create a new semantic model or edit an existing one.
        """

        # Direct to specific page based instead of default onboarding if user comes from successful partner setup
        st.markdown(
            """
                <div style="text-align: center;">
                    <h1>Welcome to the Snowflake Semantic Model Generator! ❄️</h1>
                    <p>⚠️  Heads up! The Streamlit app is no longer supported for semantic model creation.
                    <p>👉 Please use the Snowsight UI in Snowflake to create and update semantic models — it’s newer and works better! </p>
                    <p>✅ Once your model is created in Snowsight, come back here to run evaluations, which still work best in this app.</p>
                </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("<div style='margin: 60px;'></div>", unsafe_allow_html=True)

        _, center, _ = st.columns([1, 2, 1])
        with center:
            if st.button(
                "**[⚠️ Deprecated]🛠 Create a new semantic model**",
                use_container_width=True,
                type="primary",
            ):
                builder.show()
            st.markdown("")
            if st.button(
                "**✏️ Edit an existing semantic model**",
                use_container_width=True,
                type="primary",
            ):
                iteration.show()
            st.markdown("")
            if st.button(
                "**[⚠️ Deprecated]📦 Start with partner semantic model**",
                use_container_width=True,
                type="primary",
            ):
                set_sit_query_tag(
                    get_snowflake_connection(),
                    vendor="",
                    action="start",
                )
                partner.show()

    conn = verify_environment_setup()
    set_snowpark_session(conn)

    # Populating common state between builder and iteration apps.
    set_account_name(conn, SNOWFLAKE_ACCOUNT_LOCATOR)
    set_host_name(conn, SNOWFLAKE_HOST)
    set_user_name(conn, SNOWFLAKE_USER)

    # When the app first loads, show the onboarding screen.
    if "page" not in st.session_state:
        st.session_state["page"] = GeneratorAppScreen.ONBOARDING

    # Depending on the page state, we either show the onboarding menu or the chat app flow.
    # The builder flow is simply an intermediate dialog before the iteration flow.
    if st.session_state["page"] == GeneratorAppScreen.ITERATION:
        iteration.show()
    else:
        onboarding_dialog()
