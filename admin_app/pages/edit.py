import streamlit as st
from app_utils import edit_semantic_model, semantic_model_exists

if semantic_model_exists():
    edit_semantic_model()
else:
    st.error("No model found.")
