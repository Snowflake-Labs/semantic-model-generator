from __future__ import annotations
import streamlit as st

# Specify app navigation utils
PAGES_DIRECTORY = "yaml_app_utils/pages"
NAVIGATION = [
    st.Page(
        page=f"{PAGES_DIRECTORY}/getting_started.py",
        title="Getting started",
        icon=":material/outlined_flag:",
        default=True,
    ),
    st.Page(
        page=f"{PAGES_DIRECTORY}/store.py",
        title="Store",
        icon=":material/source:",
    ),
    st.Page(
        page=f"{PAGES_DIRECTORY}/create.py",
        title="Create",
        icon=":material/add:",
    ),
    st.Page(
        page=f"{PAGES_DIRECTORY}/edit.py",
        title="Edit",
        icon=":material/edit:",
    ),
    st.Page(
        page=f"{PAGES_DIRECTORY}/validate.py",
        title="Validate",
        icon=":material/rule:",
    ),
    st.Page(
        page=f"{PAGES_DIRECTORY}/upload.py",
        title="Upload",
        icon=":material/upload:",
    ),
]

CUSTOM_STYLE = """
a[data-testid="stPageLink-NavLink"] > span {
    margin-left: auto;
}

"""

CUSTOM_STYLE_ANIMATION = """
a[data-testid="stPageLink-NavLink"] > span {
    margin-left: auto;
    animation: signal 3s;
}

@keyframes signal {
  from {color: blue;}
  to {color: default;}
}
"""

def get_selected_page_index(
    selected_page: st.Page, navigation: list[st.Page] = NAVIGATION
) -> int:
    """
    Get page index

    Args:
        selected_page (st.Page): Page we want the index from
        navigation (list[st.Page]): Sorted list of pages

    Returns:
        int: Index of selected page in navigation
    """
    return [
        index
        for (index, page) in enumerate(navigation)
        if selected_page.title == page.title
    ][0]


def get_previous_and_next_pages(
    selected_page: st.Page,
    navigation: list[st.Page] = NAVIGATION,
) -> tuple[st.Page | None, st.Page | None]:
    """
    Get the previous and next page objects for a given page.
    This assumes the pages are sorted sequentially inside
    the input `navigation`.

    If page is first or last, then next and previous pages
    will be marked as None.

    Args:
        selected_page (st.Page): Page we want to get the previous
            and next from.
        navigation (list[st.Page]): Sorted list of pages

    Returns:
        tuple[st.Page | None, st.Page | None]: Previous and next pages.
    """

    selected_page_index = get_selected_page_index(selected_page)

    if selected_page_index == 0:
        previous_page = None
    else:
        previous_page = navigation[selected_page_index - 1]

    if selected_page_index == len(navigation) - 1:
        next_page = None
    else:
        next_page = navigation[selected_page_index + 1]

    return (previous_page, next_page)


def get_spec(previous_page: st.Page | None, next_page: st.Page | None) -> tuple:
    """
    Get a spec for the st.columns to work nicely depending on the labels
    for the previous (left) and next (right) buttons.

    Args:
        previous_page (st.Page | None): Previous page
        next_page (st.Page | None): Next page

    Returns:
        tuple: spec that is used inside of st.columns
    """
    if previous_page and next_page:
        return (len(previous_page.title), 20, len(next_page.title))
    else:
        return (5, 20, 5)