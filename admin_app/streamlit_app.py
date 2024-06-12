import streamlit as st
from app_utils import semantic_model_exists, show_yaml_in_dialog

st.set_page_config(layout="wide")

# Import Material Icons manually so we can use
# them beyond page titles in st.markdown or so.
MATERIAL_ICONS_URL = "https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0"
material_icons_css = f'<link rel="stylesheet" href="{MATERIAL_ICONS_URL}" />'
st.markdown(material_icons_css, unsafe_allow_html=True)

# Add a logo on the top-left corner of the app
LOGO_URL_LARGE = "https://upload.wikimedia.org/wikipedia/commons/thumb/f/ff/Snowflake_Logo.svg/2560px-Snowflake_Logo.svg.png"
LOGO_URL_SMALL = (
    "https://logos-world.net/wp-content/uploads/2022/11/Snowflake-Symbol.png"
)
st.logo(
    image=LOGO_URL_LARGE,
    link="https://www.snowflake.com/en/data-cloud/cortex/",
    icon_image=LOGO_URL_SMALL,
)

# Specify app navigation
selected_page: st.Page = st.navigation(
    [
        st.Page(
            page="pages/about.py",
            title="About",
            icon=":material/info:",
            default=True,
        ),
        st.Page(
            page="pages/store.py",
            title="Store",
            icon=":material/folder:",
        ),
        st.Page(
            page="pages/create.py",
            title="Create",
            icon=":material/add:",
        ),
        st.Page(
            page="pages/edit.py",
            title="Edit",
            icon=":material/edit:",
        ),
        st.Page(
            page="pages/validate.py",
            title="Validate",
            icon=":material/check:",
        ),
        st.Page(
            page="pages/upload.py",
            title="Upload",
            icon=":material/upload:",
        ),
        st.Page(
            page="pages/chat.py",
            title="Chat",
            icon=":material/chat:",
        ),
    ],
)

# Add metadata about the active semantic model
# being edited in the sidebar.
with st.sidebar:
    st.caption(
        ":gray[**Active model**]",
        help="When you start editing your model, it will appear here",
    )

    if semantic_model_exists():
        model = st.session_state.semantic_model
        st.write(f"- Name: `{model.name}`")
        if st.session_state.validated:
            st.write("- Status: :green[Validated]")
        else:
            st.write("- Status: :red[Not validated]")

        if "snowflake_stage" in st.session_state:
            stage = st.session_state.snowflake_stage
            st.write(
                f"- Stage: `{stage.stage_database}.{stage.stage_schema}.{stage.stage_name}`"
            )
    else:
        st.caption("No active model yet.")

    st.caption(":gray[**Generated YAML**]")

    left, right = st.columns(2)
    with left:
        st.button(
            "Preview",
            disabled=not semantic_model_exists(),
            use_container_width=True,
            on_click=show_yaml_in_dialog,
        )

    with right:
        st.download_button(
            "Download",
            data=b"mock content",  # TODO: Fix!
            file_name="semantic_model.yaml",
            mime="text/plain",
            disabled=not semantic_model_exists(),
            use_container_width=True,
        )

    st.caption(":gray[**Debug**]", help="That should be dropped! Just helpful locally")
    st.expander("Session state").write(st.session_state)


# Show app title (common to all pages)
st.title("Semantic Model Builder")
st.write("Your companion app to manage Snowflake semantic models.")
st.divider()

# Show page title
icon_name = selected_page.icon.split("/")[1].rstrip(":")
icon_html = f"""<span class="material-symbols-outlined">{icon_name}</span>"""
title = f"""<h2> {icon_html} {selected_page.title}</h2>"""
st.markdown(title, unsafe_allow_html=True)

# Run selected page
selected_page.run()
