from __future__ import annotations
import streamlit as st
from streamlit_extras.stylable_container import stylable_container

from shared_utils import model_is_validated, semantic_model_exists, show_yaml_in_dialog, AppMetadata
from builder_app_utils.navigation import (
    NAVIGATION,
    PAGES_DIRECTORY,
    CUSTOM_STYLE,
    CUSTOM_STYLE_ANIMATION,
    get_previous_and_next_pages,
    get_selected_page_index,
    get_spec,
)

st.set_page_config(layout="centered")

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

selected_page: st.Page = st.navigation(
    NAVIGATION,
    position="hidden",  # We hide the default navigation since we make a custom topnav one
)


with st.sidebar:

    # Show app title (common to all pages)
    st.title("Semantic model builder")
    st.write("Your companion app to build a Snowflake semantic model.")

    st.caption(
        ":gray[**Active settings**]",
        help="When you start editing your model, settings will appear here",
    )

    AppMetadata().show_as_dataframe()
    if model_is_validated():
        st.write("Model is :green[validated]")
    else:
        st.write("Model is :red[not yet validated]")

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


previous_page, next_page = get_previous_and_next_pages(selected_page)


with stylable_container(
    key="navigation",
    css_styles="{padding: calc(1em - 1px);}",
):

    previous_button_container, _, next_button_container = st.columns(
        spec=get_spec(previous_page, next_page),
        vertical_alignment="center",
    )

# Create a bordered container with all the page-level UI
with st.container(border=True):

    # Show page formatted title, with a Material icon
    icon_name = selected_page.icon.split("/")[1].rstrip(":")
    icon_html = f"""<span class="material-symbols-outlined">{icon_name}</span>"""
    title = f"""<h4> {icon_html}  {selected_page.title}</h4>"""
    st.markdown(title, unsafe_allow_html=True)

    # Show a progress bar
    selected_page_index = get_selected_page_index(selected_page)
    progress_value = (selected_page_index + 1) / len(NAVIGATION)
    st.progress(value=progress_value)

    # Run the selected page
    with st.container(border=False):
        selected_page.run()


with previous_button_container:
    if previous_page:
        url_path = previous_page.url_path or "getting_started"
        st.page_link(
            label=f" ←  " + f"{previous_page.title}",
            page=f"{PAGES_DIRECTORY}/{url_path}.py",
            use_container_width=True,
        )

with next_button_container:
    if next_page:
        label = f"{next_page.title}"
        if "next_is_unlocked" not in st.session_state:
            disabled = False
        else:
            disabled = not st.session_state.next_is_unlocked

        with stylable_container(
            key="next-page-container",
            css_styles=CUSTOM_STYLE_ANIMATION if not disabled else CUSTOM_STYLE,
        ):

            st.page_link(
                label=" →  " + label,
                page=f"{PAGES_DIRECTORY}/{next_page.url_path}.py",
                icon=None,
                use_container_width=True,
                disabled=disabled,
                help="Some action needed!" if disabled else None,
            )
    else:
        st.container(border=False).write("Final step! 🎉")