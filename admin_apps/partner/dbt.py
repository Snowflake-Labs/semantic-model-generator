import streamlit as st

from admin_apps.shared_utils import (
    extract_key_values,
    load_yaml_file,
)


# Partner semantic support instructions
DBT_IMAGE = 'admin_apps/images/dbt-signature_tm_black.png'
DBT_INSTRUCTIONS = """
We extract metadata from your **DBT** semantic yaml file(s) and merge it with a generated Cortex Analyst semantic file.

**Note**: The DBT semantic layer must be sourced from tables/views in Snowflake.
> Steps:
> 1) Upload your dbt semantic (yaml/yml) file(s) below. 
> 2) Select **🛠 Create a new semantic model** to generate a new Cortex Analyst semantic file for Snowflake tables or **✏️ Edit an existing semantic model**.
> 3) Validate the output in the UI.
> 4) Once you've validated the semantic file, click **Partner Semantic** to merge DBT and Cortex Analyst semantic files.  
"""


def upload_dbt_semantic() -> None:
    """
    Upload semantic file(s) for dbt from local source.
    """
    uploaded_files = st.file_uploader(
            f'Upload {st.session_state["partner_tool"]} semantic yaml file(s)',
            type=["yaml", "yml"],
            accept_multiple_files=True,
            key="myfile",
        )
    if uploaded_files:
        partner_semantic = extract_key_values(
            load_yaml_file(uploaded_files), "semantic_models"
        )
        if not partner_semantic:
            st.error(
                "Upload file does not contain required semantic_models section."
            )
        else:
            st.session_state["partner_semantic"] = partner_semantic
            # st.session_state["uploaded_semantic_files"] = [
            #     i.name for i in uploaded_files
            # ]
            # Where logical fields are captured in semantic file
            st.session_state['field_section_names'] = ["dimensions", "measures", "entities"]
            # Field-level metadata common to both cortex and partner
            st.session_state['common_fields'] = ["name", "description"]
        if st.button("Continue", type="primary"):
            st.session_state['partner_setup'] = True
            st.rerun()
    else:
        st.session_state["partner_semantic"] = None


def determine_field_section_dbt(
    section_cortex: str,
    section_partner: str,
    field_details_cortex: dict[str, str],
    field_details_partner: dict[str, str],
) -> tuple[str, str | None]:
    """
    Derives intended section and data type of field in cortex analyst model.

    Function assumes dbt as partner.
    """
    if section_cortex and field_details_cortex:
        try:
            data_type = field_details_cortex.get("data_type", None)
        except TypeError:
            data_type = "TEXT"
        return (section_cortex, data_type)
    else:  # No matching cortex field found; field is partner is a novel logical field
        if section_partner == "entities":
            section_cortex = "dimensions"
            data_type = "TEXT"
        elif section_partner == "measures":
            section_cortex = "measures"
            data_type = "NUMBER"
        else:  # field_details_partner == 'dimensions'
            try:
                if field_details_partner.get("type") == "time":
                    section_cortex = "time_dimensions"
                    data_type = "DATE"
            except TypeError:
                section_cortex = "dimensions"
                data_type = "TEXT"
            else:
                section_cortex = "dimensions"
                data_type = "TEXT"
        return (section_cortex, data_type)
