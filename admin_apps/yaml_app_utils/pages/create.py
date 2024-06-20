import streamlit as st
from shared_utils import display_semantic_model, import_yaml, init_session_states, semantic_model_exists

init_session_states()

# In case the semantic model was built already,
# the next page should be available already.
st.session_state["next_is_unlocked"] = semantic_model_exists()

""" Create your semantic model from scratch or by uploading an existing YAML file."""

creation_type = st.selectbox("Create model", ["From scratch", "Load existing YAML"], label_visibility="collapsed")

if creation_type == "From scratch":
    display_semantic_model()

elif creation_type == "Load existing YAML":
    import_yaml()