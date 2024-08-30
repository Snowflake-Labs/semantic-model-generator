from typing import Any
import json
import time
import yaml

# from snowflake.connector import SnowflakeConnection
import streamlit as st
import numpy as np
import pandas as pd

from admin_apps.shared_utils import (
    render_image,
)

from admin_apps.partner.looker import (
    set_looker_semantic,
)

from admin_apps.partner.dbt import (
    upload_dbt_semantic,
    DBTSemanticModel,

)

from admin_apps.partner.cortex import (
    CortexSemanticTable,
)

from admin_apps.partner.looker import (
    LookerSemanticTable,
)

from semantic_model_generator.data_processing.proto_utils import (
    yaml_to_semantic_model,
)


def extract_expressions_from_sections(
    data_dict: dict[str, Any], section_names: list[str]
) -> dict[str, dict[str, Any]]:
    
    """
    Extracts data in section_names from a dictionary into a nested dictionary:
    """

    def extract_dbt_field_key(obj: dict[str, Any]) -> str | Any:
        # return obj.get("expr", obj["name"]).lower()
        return obj.get("expr", obj["name"]).upper()

    d = {}
    for i in section_names:
        if st.session_state["selected_partner"] == 'dbt':
            d[i] = {extract_dbt_field_key(obj): obj for obj in data_dict.get(i, [])}

    return d


def set_partner_instructions() -> None:
    
    """
    Sets instructions and partner logo in session_state based on selected partner.
    """
    
    if st.session_state.get("partner_tool", None):
        if st.session_state["partner_tool"] == "dbt":
            from admin_apps.partner.dbt import DBT_IMAGE, DBT_INSTRUCTIONS

            instructions = DBT_INSTRUCTIONS
            image = DBT_IMAGE
            image_size = (72, 32)
        elif st.session_state["partner_tool"] == "looker":
            from admin_apps.partner.looker import LOOKER_IMAGE, LOOKER_INSTRUCTIONS

            instructions = LOOKER_INSTRUCTIONS
            image = LOOKER_IMAGE
            image_size = (72, 72)
        st.session_state["partner_instructions"] = instructions
        st.session_state["partner_image"] = image
        st.session_state["partner_image_size"] = image_size


def configure_partner_semantic() -> None:
    
    """
    Upload semantic files from local source.
    """
    
    from admin_apps.journeys import builder

    partners = [None, "dbt", "looker"]

    partner_tool = st.selectbox(
        "Select the partner tool",
        partners,
        index = None,
        key="partner_tool",
        on_change=set_partner_instructions()
    )
    if st.session_state.get("partner_tool", None):
        with st.expander(f"{st.session_state.get('partner_tool', '').title()} Instructions", expanded=True):
            render_image(st.session_state['partner_image'], st.session_state['partner_image_size'])
            st.write(st.session_state['partner_instructions'])
    
    # Previous dialog box widget values will reset when overlayed
    if st.session_state.get('partner_tool', None):
        st.session_state['selected_partner'] = st.session_state['partner_tool']
    # if st.session_state.get('partner_setup', False):
    #     st.session_state['partner_setup'] = st.session_state['partner_setup']

    if st.session_state["partner_tool"] == "dbt":
        upload_dbt_semantic()
    if st.session_state["partner_tool"] == "looker":
        set_looker_semantic()


class PartnerCompareRow:
    def __init__(self, row_data: pd.Series) -> None:  # type: ignore
        self.row_data = row_data
        self.key = row_data["field_key"]
        self.cortex_metadata = (
            self.row_data["field_details_cortex"]
            if self.row_data["field_details_cortex"]
            else {}
        )
        self.partner_metadata = (
            self.row_data["field_details_partner"]
            if self.row_data["field_details_partner"]
            else {}
        )

    def render_row(self) -> None | dict[str, Any]:  # type: ignore
        toggle_options = ["merged", "cortex", "partner", "remove"]
        metadata = {}

        # Create metadata based for each field given merging or singular semantic file useage of the field
        # Merge will merge the 2 based on user-selected preference
        if self.cortex_metadata and self.partner_metadata:
            metadata["merged"] = self.cortex_metadata.copy()
            if st.session_state["partner_metadata_preference"] == "Partner":
                metadata['merged'] = self.cortex_metadata | self.partner_metadata
            else:
                metadata['merged'] = self.partner_metadata | self.cortex_metadata

        else:
            metadata["merged"] = {}
        metadata["partner"] = self.partner_metadata if self.partner_metadata else {}
        metadata["cortex"] = self.cortex_metadata if self.cortex_metadata else {}
        metadata["remove"] = {}

        if metadata["merged"]:
            toggle_default = "merged"
        elif metadata["partner"]:
            if st.session_state["keep_extra_partner"]:
                toggle_default = "partner"
            else:
                toggle_default = "remove"
        elif metadata["cortex"]:
            if st.session_state["keep_extra_cortex"]:
                toggle_default = "cortex"
            else:
                toggle_default = "remove"
        else:
            toggle_default = "remove"

        key_col, detail_col = st.columns((0.5, 1))
        with key_col:
            st.write(self.key)
            # We want to disable non-options but always keep remove option
            revised_options = [
                i for i in toggle_options if metadata[i] or i == "remove"
            ]
            detail_selection: str = st.radio(
                "Keep",  # type: ignore
                index=revised_options.index(toggle_default),
                options=revised_options,
                key=f"row_{self.key}",
                format_func=lambda x: x.capitalize(),
                label_visibility="collapsed",
            )
        with detail_col:
            if metadata[detail_selection]:
                st.json(
                    {
                        k: v
                        for k, v in metadata[detail_selection].items()
                        if isinstance(v, str)
                    }
                )
            else:
                st.write("NA")
        st.divider()
        # Extract the selected metadata if not set to remove
        if detail_selection != "remove":
            selected_metadata: dict[str, Any] = metadata[detail_selection]
            # Add expr to selected metadata if it's not included which is the case for dbt
            selected_metadata["expr"] = self.key
            return selected_metadata
    

def compare_sections(section_cortex: str, section_partner: str) -> str:
    
    """
    Returns cortex section if not None, else partner section
    """

    if section_cortex:
        return section_cortex
    else:
        return section_partner
    

def compare_data_types(details_cortex: dict[str, Any], details_partner: dict[str, Any]) -> str:
    
    """
    Returns cortex datatype if not None, else partner section
    """

    cortex_data_type = None
    partner_data_type = None

    if isinstance(details_cortex, dict):
        cortex_data_type = details_cortex.get("data_type", None)
    if isinstance(details_partner, dict):
        partner_data_type = details_partner.get("data_type", None)

    if cortex_data_type:
        return cortex_data_type
    elif partner_data_type:
        return partner_data_type
    else:
        return 'TEXT'


@st.dialog("Integrate partner tool semantic specs", width="large")
def integrate_partner_semantics() -> None:
    
    st.write(
        "Specify how to merge semantic metadata from partner tools with Cortex Analyst's semantic model."
    )

    COMPARE_SEMANTICS_HELP = """Which semantic file should be checked first for necessary metadata.
    Where metadata is missing, the other semantic file will be checked."""

    INTEGRATE_HELP = """Merge the selected Snowflake and Partner tables' semantics together."""

    SAVE_HELP = """Save the merges to the Cortex Analyst semantic model for validation and iteration."""

    KEEP_CORTEX_HELP = """Retain fields that are found in Cortex Analyst semantic model
    but not in Partner semantic model."""

    KEEP_PARTNER_HELP = """Retain fields that are found in Partner semantic model
    but not in Cortex Analyst semantic model."""


    # if st.session_state.get("partner_semantic", None):
    if st.session_state.get('partner_setup', False):
        if st.session_state.get("selected_partner", None) == 'looker':
            LookerSemanticTable.create_cortex_table_list()
        elif st.session_state.get("selected_partner", None) == 'dbt':
            pass
        else:
            st.error("Selected partner tool not available.")

        partner_tables = [model.get_name() for model in st.session_state["partner_semantic"]]
        # elif st.session_state.get("selected_partner", None) == 'looker':
            # partner_tables = [model.get_name() for model in st.session_state["partner_semantic"]]
            # Write cortex semantic tables as classes in a list to session state
        CortexSemanticTable.create_cortex_table_list()
        cortex_tables = [table.get_name() for table in st.session_state["cortex_comparison_tables"]]

        # partner_tables = [model.get_name() for model in st.session_state["partner_semantic"]]
        # partner_tables = extract_key_values(st.session_state["partner_semantic"], "name")

        st.write("Select which logical tables/views to compare and merge.")
        c1, c2 = st.columns(2)
        with c1:
            semantic_cortex_tbl: str = st.selectbox("Snowflake", cortex_tables)  # type: ignore
        with c2:
            semantic_partner_tbl: str = st.selectbox("Partner", partner_tables)  # type: ignore

        st.session_state["partner_metadata_preference"] = st.selectbox(
            "For fields shared in both sources, which source should be checked first for common metadata?",
            ["Partner", "Cortex"],
            index=0,
            help=COMPARE_SEMANTICS_HELP,
        )
        orphan_label, orphan_col1, orphan_col2 = st.columns(
            3, vertical_alignment="center", gap="small"
        )
        with orphan_label:
            st.write("Retain unmatched fields:")
        with orphan_col1:
            st.session_state["keep_extra_cortex"] = st.toggle(
                "Cortex", value=True, help=KEEP_CORTEX_HELP
            )
        with orphan_col2:
            st.session_state["keep_extra_partner"] = st.toggle(
                "Partner", value=True, help=KEEP_PARTNER_HELP
            )
        with st.expander("Advanced configuration", expanded=False):
            st.caption("Only shared metadata information displayed")
            # Create dataframe of each semantic file's fields with mergeable keys
            if st.session_state.get("selected_partner", None) == 'looker':
                # THIS IS WHERE THE ERROR IS OCCURING
                partner_fields_df = LookerSemanticTable.retrieve_df_by_name(semantic_partner_tbl)
            if st.session_state.get("selected_partner", None) == 'dbt':
                partner_fields_df = DBTSemanticModel.retrieve_df_by_name(semantic_partner_tbl)

            # partner_fields_df = DBTSemanticModel.retrieve_df_by_name(semantic_partner_tbl)
            cortex_fields_df = CortexSemanticTable.retrieve_df_by_name(semantic_cortex_tbl)
            combined_fields_df = cortex_fields_df.merge(
                partner_fields_df,
                on="field_key",
                how="outer",
                suffixes=("_cortex", "_partner"),
            ).replace(np.nan, None)

            # Convert json strings to dict for easier extraction later
            for col in ["field_details_cortex", "field_details_partner"]:
                combined_fields_df[col] = combined_fields_df[col].apply(
                    lambda x: (
                        json.loads(x)
                        if not pd.isnull(x) and not isinstance(x, dict)
                        else x
                    )
                )

            # Create containers and store them in a dictionary
            containers = {
                "dimensions": st.container(),
                "measures": st.container(),
                "time_dimensions": st.container(),
            }

            # Assign labels to the containers
            for key in containers.keys():
                containers[key].write(f"**{key.replace('_', ' ').title()}**")

            # Initialize sections as empty lists
            sections: dict[str, list[dict[str, Any]]] = {
                key: [] for key in containers.keys()
            }
            for k, v in combined_fields_df.iterrows():
                # Get destination section and intended data type for cortex analyst semantic file
                # If the key is found from the generator, use it. Otherwise, use the partner-specific logic.
                target_section = compare_sections(v["section_cortex"], v["section_partner"])
                target_data_type = compare_data_types(v["field_details_cortex"], v["field_details_partner"])
                with containers[target_section]:
                    selected_metadata = PartnerCompareRow(v).render_row()
                    if selected_metadata:
                        selected_metadata["data_type"] = target_data_type
                        sections[target_section].append(selected_metadata)

            integrate_col, commit_col, _ = st.columns((1, 1, 5), gap="small")
            with integrate_col:
                merge_button = st.button(
                    "Merge",
                    help=INTEGRATE_HELP,
                    use_container_width=True
                )
            with commit_col:
                reset_button = st.button(
                    "Save",
                    help=SAVE_HELP,
                    use_container_width=True,
                )

            if merge_button:
                # Update fields in cortex semantic model
                for i, tbl in enumerate(st.session_state['cortex_comparison_tables']):
                    if tbl.get_name() == semantic_cortex_tbl:
                        for k in sections.keys():
                            st.session_state['current_yaml_as_dict']['tables'][i][k] = sections[k]

                try:
                    st.session_state["yaml"] = yaml.dump(st.session_state['current_yaml_as_dict'],
                                                         sort_keys=False)
                    st.session_state["semantic_model"] = yaml_to_semantic_model(
                        st.session_state["yaml"]
                    )
                    merge_msg = st.success("Merging...")
                    time.sleep(1)
                    merge_msg.empty()
                except Exception as e:
                    st.error(f"Integration failed: {e}")

            if reset_button:
                st.success(
                    "Integration complete! Please validate your semantic model before uploading."
                )
                time.sleep(1.5)
                st.rerun()  # Lazy alternative to resetting all configurations
    else:
        st.error("Partner semantic not setup.")