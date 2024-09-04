from typing import Any, Optional

import pandas as pd
import streamlit as st
import yaml

from admin_apps.shared_utils import get_snowflake_connection, set_sit_query_tag

# Partner semantic support instructions
DBT_IMAGE = "admin_apps/images/dbt-signature_tm_black.png"
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

    Returns: None
    """

    uploaded_files = st.file_uploader(
        f'Upload {st.session_state["partner_tool"]} semantic yaml file(s)',
        type=["yaml", "yml"],
        accept_multiple_files=True,
        key="myfile",
    )
    if uploaded_files:
        partner_semantic: list[None | DBTSemanticModel] = []
        for file in uploaded_files:
            partner_semantic.extend(read_dbt_yaml(file))  # type: ignore

        if not partner_semantic:
            st.error("Upload file(s) do not contain required semantic_models section.")
        else:
            st.session_state["partner_semantic"] = partner_semantic
        if st.button("Continue", type="primary"):
            st.session_state["partner_setup"] = True
            set_sit_query_tag(
                get_snowflake_connection(),
                vendor="dbt",
                action="setup_complete",
            )
            st.rerun()
    else:
        st.session_state["partner_semantic"] = None


class DBTEntity:
    """
    Class for dbt entity-type field.
    """

    def __init__(self, entity: dict[str, Any]):

        self.entity: dict[str, Any] = entity
        self.name: str = entity["name"]
        self.type: str = entity.get("type", None)
        self.expr: str = entity.get("expr", self.name)
        self.description: None | str = entity.get("description", None)
        self.cortex_map = {
            "name": self.name,
            "description": self.description,
            "expr": self.expr,
            "data_type": self.get_cortex_type(),
        }

    def get_data(self) -> dict[str, Any]:
        return self.entity

    def get_cortex_type(self) -> str:
        return "TEXT"

    def get_cortex_section(self) -> str:
        return "dimensions"

    def get_key(self) -> str:
        return self.expr.upper()

    def get_cortex_details(self) -> dict[str, Any]:
        return_details = {}
        for k, v in self.cortex_map.items():
            if v is not None:
                return_details[k] = v
        return return_details

    def get_cortex_comparison_dict(self) -> dict[str, Any]:
        return {
            "field_key": self.get_key(),
            "section": self.get_cortex_section(),
            "field_details": self.get_cortex_details(),
        }


class DBTMeasure(DBTEntity):
    """
    Class for dbt measure-type field.
    """

    def __init__(self, entity: dict[str, Any]):
        super().__init__(entity)
        self.agg: None | str = entity.get("agg", None)
        self.cortex_map = {
            "name": self.name,
            "description": self.description,
            "expr": self.expr,
            "data_type": self.get_cortex_type(),
            "default_aggregation": self.agg,
        }

    def get_cortex_type(self) -> str:
        return "NUMBER"

    def get_cortex_section(self) -> str:
        return "measures"


class DBTDimension(DBTEntity):
    """
    Class for dbt dimension-type field.
    """

    def get_cortex_type(self) -> str:
        if self.type == "time":
            return "DATETIME"
        else:
            return "TEXT"

    def get_cortex_section(self) -> str:
        if self.type == "time":
            return "time_dimensions"
        else:
            return "dimensions"


class DBTSemanticModel:
    """
    Class for single DBT semantic model.
    """

    def __init__(self, data: dict[str, Any]):
        self.data: dict[str, Any] = data
        self.name: str = data["name"]
        self.description: None | str = data.get("description", None)
        self.entities: None | list[dict[str, Any]] = data["entities"]
        self.dimensions: None | list[dict[str, Any]] = data["dimensions"]
        self.measures: None | list[dict[str, Any]] = data["measures"]

    def get_data(self) -> dict[str, Any]:
        return self.data

    def get_name(self) -> str:
        return self.name

    def get_description(self) -> Optional[str]:
        return self.description

    def get_cortex_fields(self) -> list[dict[str, Any]]:
        cortex_fields = []
        if self.entities:
            for entity in self.entities:
                cortex_fields.append(DBTEntity(entity).get_cortex_comparison_dict())
        if self.measures:
            for measure in self.measures:
                cortex_fields.append(DBTMeasure(measure).get_cortex_comparison_dict())
        if self.dimensions:
            for dimension in self.dimensions:
                cortex_fields.append(
                    DBTDimension(dimension).get_cortex_comparison_dict()
                )

        return cortex_fields

    def create_comparison_df(self) -> pd.DataFrame:
        cortex_fields = self.get_cortex_fields()
        return pd.DataFrame(cortex_fields)

    @staticmethod
    def retrieve_df_by_name(name: str) -> pd.DataFrame:
        for model in st.session_state["partner_semantic"]:
            if model.get_name() == name:
                return model.create_comparison_df()


def read_dbt_yaml(file_path: str) -> list[DBTSemanticModel]:
    """
    Reads file uploads and extracts dbt semantic files in list.
    Args:
        file_path (str): Local file path uploaded by user.

    Returns: None | list[DBTSemanticModel]
    """

    data = yaml.safe_load(file_path)
    dbt_semantic_models = []
    if "semantic_models" in data:
        # dbt_semantic_models = []
        for semantic_model in data["semantic_models"]:
            dbt_semantic_models.append(DBTSemanticModel(semantic_model))
    else:
        st.warning(f"{file_path} does not contain semantic_models section. Skipping.")
    return dbt_semantic_models
