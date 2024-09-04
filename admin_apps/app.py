import streamlit as st
from snowflake.connector import DatabaseError

# set_page_config must be run as the first Streamlit command on the page, before any other streamlit imports.
st.set_page_config(layout="wide", page_icon="💬", page_title="Semantic Model Generator")

from admin_apps.shared_utils import (  # noqa: E402
    GeneratorAppScreen,
    get_snowflake_connection,
    set_sit_query_tag,
)
from semantic_model_generator.snowflake_utils.env_vars import (  # noqa: E402
    SNOWFLAKE_ACCOUNT_LOCATOR,
    SNOWFLAKE_HOST,
    SNOWFLAKE_USER,
    assert_required_env_vars,
)


@st.dialog(title="Setup")
def env_setup_popup(missing_env_vars: list[str]) -> None:
    """
    Renders a dialog box to prompt the user to set the required environment variables.
    Args:
        missing_env_vars: A list of missing environment variables.
    """
    formatted_missing_env_vars = "\n".join(f"- **{s}**" for s in missing_env_vars)
    st.markdown(
        f"""Oops! It looks like the following required environment variables are missing: \n{formatted_missing_env_vars}\n\n
Please follow the [setup instructions](https://github.com/Snowflake-Labs/semantic-model-generator?tab=readme-ov-file#setup) to properly configure your environment. Restart this app after you've set the required environment variables."""
    )
    st.stop()


@st.dialog(title="Connection Error")
def failed_connection_popup() -> None:
    """
    Renders a dialog box detailing that the credentials provided could not be used to connect to Snowflake.
    """
    st.markdown(
        f"""It looks like the credentials provided for `{SNOWFLAKE_USER}` could not be used to connect to the account `{SNOWFLAKE_ACCOUNT_LOCATOR}` at host `{SNOWFLAKE_HOST}`. Please verify your credentials in the environment variables and try again."""
    )
    st.stop()


def verify_environment_setup() -> None:
    """
    Ensures that the correct environment variables are set before proceeding.
    """
    missing_env_vars = assert_required_env_vars()
    if missing_env_vars:
        env_setup_popup(missing_env_vars)

    # Instantiate the Snowflake connection that gets reused throughout the app.
    try:
        with st.spinner(
            "Validating your connection to Snowflake. If you are using MFA, please check your authenticator app for a push notification."
        ):
            get_snowflake_connection()
    except DatabaseError:
        failed_connection_popup()


if __name__ == "__main__":
    from admin_apps.journeys import builder, iteration, partner

    def onboarding_dialog() -> None:
        """
        Renders the initial screen where users can choose to create a new semantic model or edit an existing one.
        """

        # Direct to specific page based instead of default onboarding if user comes from successful partner setup
        if (
            st.session_state.get("partner_setup", False)
            and st.session_state.get("partner_tool", None) == "looker"
        ):
            builder.show()
        st.markdown(
            """
                <div style="text-align: center;">
                    <h1>Welcome to the Snowflake Semantic Model Generator! ❄️</h1>
                    <p>Let's get started. What would you like to do?</p>
                </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("<div style='margin: 60px;'></div>", unsafe_allow_html=True)

        _, center, _ = st.columns([1, 2, 1])
        with center:
            if st.button(
                "**🛠 Create a new semantic model**",
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
                "**:package: Start with partner semantic model**",
                use_container_width=True,
                type="primary",
            ):
                set_sit_query_tag(
                    get_snowflake_connection(),
                    vendor="",
                    action="start",
                )
                partner.show()

    verify_environment_setup()

    # Populating common state between builder and iteration apps.
    st.session_state["account_name"] = SNOWFLAKE_ACCOUNT_LOCATOR
    st.session_state["host_name"] = SNOWFLAKE_HOST
    st.session_state["user_name"] = SNOWFLAKE_USER

    # When the app first loads, show the onboarding screen.
    if "page" not in st.session_state:
        st.session_state["page"] = GeneratorAppScreen.ONBOARDING

    # Depending on the page state, we either show the onboarding menu or the chat app flow.
    # The builder flow is simply an intermediate dialog before the iteration flow.
    if st.session_state["page"] == GeneratorAppScreen.ITERATION:
        iteration.show()
    else:
        onboarding_dialog()
