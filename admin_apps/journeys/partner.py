import streamlit as st
from PIL import Image

from admin_apps.partner.partner_utils import (
    configure_partner_semantic,
)

@st.dialog("Partner Semantic Support", width="large")
def partner_semantic_setup() -> None:
    st.write(
        """
        Have an existing semantic layer in a partner tool that's integrated with Snowflake? 
        See the below instructions for integrating your partner semantic specs into Cortex Analyst's semantic file.
        """
    )
    configure_partner_semantic()


def show() -> None:
    partner_semantic_setup()
    
