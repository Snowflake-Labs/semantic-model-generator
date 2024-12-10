import json
import time
from enum import Enum
from typing import Any, Union

import numpy as np
import pandas as pd
import streamlit as st
import yaml

from app_utils.shared_utils import (
    get_snowflake_connection,
    render_image,
    set_sit_query_tag,
)
from partner.cortex import CortexSemanticTable
from partner.dbt import DBTSemanticModel, upload_dbt_semantic
from semantic_model_generator.data_processing.proto_utils import yaml_to_semantic_model


class PartnerTool(Enum):
    DBT_SQL_MODEL = "dbt - SQL Model"
    DBT_SEMANTIC_MODEL = "dbt - Semantic Model"
    LOOKER_EXPLORE = "Looker - Explore"


def set_partner_instructions() -> None:
    """
    Sets instructions and partner logo in session_state based on selected partner.
    Returns: None
    """

    if st.session_state.get("partner_tool", None):
        if st.session_state["partner_tool"] == PartnerTool.DBT_SQL_MODEL.value:
            from partner.dbt import DBT_IMAGE, DBT_MODEL_INSTRUCTIONS

            instructions = DBT_MODEL_INSTRUCTIONS
            image = DBT_IMAGE
            image_size = (72, 32)
        elif st.session_state["partner_tool"] == PartnerTool.DBT_SEMANTIC_MODEL.value:
            from partner.dbt import DBT_IMAGE, DBT_SEMANTIC_INSTRUCTIONS

            instructions = DBT_SEMANTIC_INSTRUCTIONS
            image = DBT_IMAGE
            image_size = (72, 32)
        elif st.session_state["partner_tool"] == PartnerTool.LOOKER_EXPLORE.value:
            from partner.looker import LOOKER_IMAGE, LOOKER_INSTRUCTIONS

            instructions = LOOKER_INSTRUCTIONS
            image = LOOKER_IMAGE
            image_size = (72, 72)
        st.session_state["partner_instructions"] = instructions
        st.session_state["partner_image"] = image
        st.session_state["partner_image_size"] = image_size


def configure_partner_semantic() -> None:
    """
    Upload semantic files from local source.
    Returns: None
    """

    partners = [tool.value for tool in PartnerTool]

    st.selectbox(
        "Select the partner tool",
        partners,
        index=None,
        key="partner_tool",
        on_change=set_partner_instructions(),  # type: ignore
    )
    if st.session_state.get("partner_tool", None):
        with st.expander(
            "Instructions",
            expanded=True,
        ):
            render_image(
                st.session_state["partner_image"],
                st.session_state["partner_image_size"],
            )
            st.write(st.session_state["partner_instructions"])

    # Previous dialog box widget values will reset when overlayed
    if st.session_state.get("partner_tool", None):
        st.session_state["selected_partner"] = st.session_state["partner_tool"]

    if st.session_state["partner_tool"] == PartnerTool.DBT_SEMANTIC_MODEL.value:
        upload_dbt_semantic()
    if st.session_state["partner_tool"] == PartnerTool.LOOKER_EXPLORE.value:
        from partner.looker import set_looker_semantic

        set_looker_semantic()
    if st.session_state["partner_tool"] == PartnerTool.DBT_SQL_MODEL.value:
        st.session_state["partner_setup"] = False


class PartnerCompareRow:
    """
    Renders matched and unmatched cortex and partner fields for comparison.
    """

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

    def render_row(self) -> Union[None, dict[str, Any]]:  # type: ignore
        toggle_options = ["merged", "cortex", "partner", "remove"]
        metadata = {}

        # Create metadata based for each field given merging or singular semantic file useage of the field
        # Merge will merge the 2 based on user-selected preference
        if self.cortex_metadata and self.partner_metadata:
            metadata["merged"] = self.cortex_metadata.copy()
            if st.session_state["partner_metadata_preference"] == "Partner":
                metadata["merged"] = {
                    k: v for k, v in self.cortex_metadata.items() if v
                } | {k: v for k, v in self.partner_metadata.items() if v}
            else:
                metadata["merged"] = {
                    k: v for k, v in self.partner_metadata.items() if v
                } | {k: v for k, v in self.cortex_metadata.items() if v}

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
                # Only printing string valued keys for now
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
    Compares section_cortex and section_parnter returning the former if available.
    Otherwise, returns the latter.

    Args:
        section_cortex (str): The Cortex section of the Cortex field if found.
        section_cortex (str): The Cortex section of the Partner field if found.

    Returns:
        str: Cortex section name.
    """

    if section_cortex:
        return section_cortex
    else:
        return section_partner


def compare_data_types(
    details_cortex: dict[str, Any], details_partner: dict[str, Any]
) -> Any:
    """
    Returns intended cortex datatype comparing cortex and partner datatype values.

    Args:
        details_cortex (dict[str, Any]): Dictionary of Cortex field metadata.
        details_partner (dict[str, Any]): Dictionary of Parnter's Cortex field metadata.

    Returns:
        str: Cortex data_type.
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
        return "TEXT"


@st.experimental_dialog("Integrate partner tool semantic specs", width="large")
def integrate_partner_semantics() -> None:
    """
    Runs UI module for comparing Cortex and Partner fields for integration.

    Returns:
        None
    """

    st.write(
        "Specify how to merge semantic metadata from your selected partner tool with Cortex Analyst's semantic model."
    )

    st.write(f"Partner: **{st.session_state.get('selected_partner', None)}**")

    COMPARE_SEMANTICS_HELP = """Which semantic file should be checked first for necessary metadata.
    Where metadata is missing, the other semantic file will be checked."""

    INTEGRATE_HELP = (
        """Merge the selected Snowflake and Partner tables' semantics together."""
    )

    SAVE_HELP = """Save the merges to the Cortex Analyst semantic model for validation and iteration."""

    KEEP_CORTEX_HELP = """Retain fields that are found in Cortex Analyst semantic model
    but not in Partner semantic model."""

    KEEP_PARTNER_HELP = """Retain fields that are found in Partner semantic model
    but not in Cortex Analyst semantic model."""

    if st.session_state.get("partner_setup", False):
        # Execute pre-processing behind the scenes based on vendor tool
        CortexSemanticTable.create_cortex_table_list()

        if (
            st.session_state.get("selected_partner", None)
            == PartnerTool.LOOKER_EXPLORE.value
        ):
            from partner.looker import LookerSemanticTable

            LookerSemanticTable.create_cortex_table_list()
        elif (
            st.session_state.get("selected_partner", None)
            == PartnerTool.DBT_SEMANTIC_MODEL.value
        ):
            pass
        else:
            st.error("Selected partner tool not available.")

        # Create table selections for comparison
        partner_tables = [
            model.get_name() for model in st.session_state["partner_semantic"]
        ]
        cortex_tables = [
            table.get_name() for table in st.session_state["cortex_comparison_tables"]
        ]

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
        orphan_label, orphan_col1, orphan_col2 = st.columns(3, gap="small")
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
            # Create dataframe of each semantic file's fields with mergeable keys
            st.caption("Only shared metadata information displayed")
            cortex_fields_df = CortexSemanticTable.retrieve_df_by_name(
                semantic_cortex_tbl
            )

            if (
                st.session_state.get("selected_partner", None)
                == PartnerTool.LOOKER_EXPLORE.value
            ):
                from partner.looker import LookerSemanticTable

                partner_fields_df = LookerSemanticTable.retrieve_df_by_name(
                    semantic_partner_tbl
                )
            if (
                st.session_state.get("selected_partner", None)
                == PartnerTool.DBT_SEMANTIC_MODEL.value
            ):
                partner_fields_df = DBTSemanticModel.retrieve_df_by_name(
                    semantic_partner_tbl
                )

            combined_fields_df = cortex_fields_df.merge(
                partner_fields_df,
                on="field_key",
                how="outer",
                suffixes=("_cortex", "_partner"),
            ).replace(
                np.nan, None
            )  # Will be comparing values to None in UI logic

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
                target_section = compare_sections(
                    v["section_cortex"], v["section_partner"]
                )
                target_data_type = compare_data_types(
                    v["field_details_cortex"], v["field_details_partner"]
                )
                with containers[target_section]:
                    selected_metadata = PartnerCompareRow(v).render_row()
                    if selected_metadata:
                        selected_metadata["data_type"] = target_data_type
                        sections[target_section].append(selected_metadata)

        integrate_col, commit_col, _ = st.columns((1, 1, 5), gap="small")
        with integrate_col:
            merge_button = st.button(
                "Merge", help=INTEGRATE_HELP, use_container_width=True
            )
        with commit_col:
            reset_button = st.button(
                "Save",
                help=SAVE_HELP,
                use_container_width=True,
            )

        if merge_button:
            set_sit_query_tag(
                get_snowflake_connection(),
                vendor=st.session_state["selected_partner"],
                action="merge",
            )
            # Update fields in cortex semantic model
            for i, tbl in enumerate(st.session_state["cortex_comparison_tables"]):
                if tbl.get_name() == semantic_cortex_tbl:
                    for k in sections.keys():
                        st.session_state["current_yaml_as_dict"]["tables"][i][k] = (
                            sections[k]
                        )

            try:
                st.session_state["yaml"] = yaml.dump(
                    st.session_state["current_yaml_as_dict"], sort_keys=False
                )
                st.session_state["semantic_model"] = yaml_to_semantic_model(
                    st.session_state["yaml"]
                )
                merge_msg = st.success("Merging...")
                time.sleep(1)
                merge_msg.empty()
            except Exception as e:
                st.error(f"Integration failed: {e}")

        if reset_button:
            set_sit_query_tag(
                get_snowflake_connection(),
                vendor=st.session_state["selected_partner"],
                action="integration_complete",
            )
            st.success(
                "Integration complete! Please validate your semantic model before uploading."
            )
            time.sleep(1.5)
            st.rerun()  # Lazy alternative to resetting all configurations
    else:
        st.error("Partner semantic not setup.")
