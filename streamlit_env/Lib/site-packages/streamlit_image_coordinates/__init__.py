from __future__ import annotations

import base64
from io import BytesIO
from pathlib import Path

import numpy as np
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image
from streamlit.elements.image import UseColumnWith

# Tell streamlit that there is a component called streamlit_image_coordinates,
# and that the code to display that component is in the "frontend" folder
frontend_dir = (Path(__file__).parent / "frontend").absolute()
_component_func = components.declare_component(
    "streamlit_image_coordinates", path=str(frontend_dir)
)


# Create the python function that will be called
def streamlit_image_coordinates(
    source: str | Path | np.ndarray | object,
    height: int | None = None,
    width: int | None = None,
    key: str | None = None,
    use_column_width: UseColumnWith | str | None = None,
    click_and_drag: bool = False,
):
    """
    Take an image source and return the coordinates of the image clicked

    Parameters
    ----------
    source : str | Path | object
        The image source
    height : int | None
        The height of the image. If None, the height will be the original height
    width : int | None
        The width of the image. If None, the width will be the original width
    use_column_width : "auto", "always", "never", or bool
        If "auto", set the image's width to its natural size,
        but do not exceed the width of the column.
        If "always" or True, set the image's width to the column width.
        If "never" or False, set the image's width to its natural size.
        Note: if set, `use_column_width` takes precedence over the `width` parameter.
    click_and_drag: bool
        If true, the event is not sent until the user releases the mouse. The
        mouse down event is returned as x1, y1 and the mouse up event is returned
        as x2, y2. Note that x2 and y2 may be outside the image.
    """

    if isinstance(source, (Path, str)):
        if not str(source).startswith("http"):
            content = Path(source).read_bytes()
            src = "data:image/png;base64," + base64.b64encode(content).decode("utf-8")
        else:
            src = str(source)
    elif hasattr(source, "save"):
        buffered = BytesIO()
        source.save(buffered, format="PNG")  # type: ignore
        src = "data:image/png;base64,"
        src += base64.b64encode(buffered.getvalue()).decode("utf-8")  # type: ignore
    elif isinstance(source, np.ndarray):
        image = Image.fromarray(source)
        buffered = BytesIO()
        image.save(buffered, format="PNG")  # type: ignore
        src = "data:image/png;base64,"
        src += base64.b64encode(buffered.getvalue()).decode("utf-8")  # type: ignore
    else:
        raise ValueError(
            "Must pass a string, Path, numpy array or object with a save method"
        )

    return _component_func(
        src=src,
        height=height,
        width=width,
        use_column_width=use_column_width,
        key=key,
        click_and_drag=click_and_drag,
    )


def main():
    st.set_page_config(
        page_title="Streamlit Image Coordinates",
        page_icon="🎯",
        layout="wide",
    )
    col1, col2, col3 = st.columns(3)

    with col1:
        st.write("## Url example")

        with st.echo():
            value = streamlit_image_coordinates(
                "https://placekitten.com/200/300",
                key="url",
            )

            st.write(value)

    with col2:
        st.write("## Local image example")

        with st.echo():
            value = streamlit_image_coordinates(
                "kitty.jpeg",
                key="local",
            )

            st.write(value)

    with col3:
        st.write("## Custom size example")

        with st.echo():
            value = streamlit_image_coordinates(
                "kitty.jpeg",
                width=250,
                key="local2",
            )

            st.write(value)


if __name__ == "__main__":
    main()
