import streamlit as st
from app_utils import display_semantic_model, import_yaml, init_session_states

""" Go ahead and create your semantic model! You can also load it from an existing .YAML"""

init_session_states()

left, right = st.columns((4, 3))

with left:
    st.write("### From scratch")
    with st.container(border=True, height=300):
        display_semantic_model()
with right:
    st.write("### Load existing")
    with st.container(border=True, height=300):
        import_yaml()
