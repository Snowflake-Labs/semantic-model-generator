import streamlit as st
from shared_utils import edit_semantic_model, semantic_model_exists

st.session_state["next_is_unlocked"] = True

if semantic_model_exists():
    edit_semantic_model()
else:
    st.error("No model found.")

# st.caption("Prototypes:")
# import pandas as pd
# from streamlit_extras.stylable_container import stylable_container
# from typing import Literal

# BORDERLESS_BUTTON = """
# button {
#     border: 0px;
#     float: right;
# }

# button:hover {
#     background-color: initial !important;
#     color: initial !important;
# }

# """

# def borderless_button(args, **kwargs):
#     with stylable_container(key=kwargs.get("key"), css_styles=BORDERLESS_BUTTON):
#         return st.button(args, **kwargs)

# def grid(
#     num_columns: int,
#     num_cells: int,
#     gap: Literal["small", "medium", "large"] = "medium",
# ):
#     """
#     Get a list of Streamlit containers so as to fill in
#     a grid as given by (num_columns) and (num_cells)
#     inputs.

#     Usage:
#     >>> cells = grid(3, 8)
#         for cell in cells:
#             cell.write("Hey there!")

#     Args:
#         num_columns (int): Number of columns
#         num_cells (int): Number of cells in the grid
#         gap (Literal["small", "medium", "large"]): Gap between columns. Defaults to "medium".

#     Returns:
#         list[DeltaGenerator]: List of Streamlit containers
#     """

#     def flatten(list_instance: list) -> list:
#         return [item for sublist in list_instance for item in sublist]

#     return flatten([st.columns(num_columns, gap=gap) for _ in range(num_cells // num_columns + 1)])

# st.caption("    ↓  Check here to edit a table")

# data = pd.DataFrame({
#     "Name": ["orders", "customers"],
#     "Database": ["autosql_dataset_dbt_jaffle_shop", "db"],
#     "Schema": ["data", "schema"],
#     "Table": ["orders", "customers"],
#     "Description": ["Order overview data mart, offering key details for each order including if it's a customer's first order and a food vs. drink item breakdown. One row per order.", "Items contatined in each order. The grain of the table is one row per order item."],
#     "Synonyms": [[], []],
# })

# # selection = st.dataframe(
# #     data=data,
# #     hide_index=True,
# #     on_select="rerun",
# #     selection_mode="single-row",
# # )

# # if selected_row := selection["selection"]["rows"]:
# #     st.write(selected_row)
# #     st.data_editor(data.iloc[selected_row])

# icon_html = """<span class="material-symbols-outlined" style="font-size: 18px">
# {}
# </span>"""

# @st.experimental_dialog("Edit table")
# def edit_table(table_name):
#     display_table(table_name)

# from app_utils import display_table, add_new_table

# cells = grid(2, len(data) + 1)
# for cell, row in zip(cells[:-1], data.itertuples()):
#     with cell:
#         with st.container(border=True, height=400,):
#             with st.container(border=False, height=200,):
#                 left, right = st.columns((4, 2), vertical_alignment="center")
#                 with right:
#                     if borderless_button("Edit", key=row.Name):
#                         edit_table(row.Table)
#                 with left:
#                     st.markdown(f"<h3 style='margin-bottom:-20px;'> {icon_html.format('table_view')} {row.Name}</h3>", unsafe_allow_html=True)
#                 st.caption(f"{row.Database}.{row.Schema}.{row.Table}", unsafe_allow_html=True)
#                 st.markdown(row.Description)
#             # borderless_button("Edit", key=row.Name, use_container_width=True)
#             st.dataframe({"Dimensions": [0], "Measures": [2], "Time dimensions": [3]})
#             st.progress(text="Number of items with description", value=12/30)
#             # left, middle, right = st.columns(3)
#             # left.metric("Dimension(s)", 0)
#             # middle.metric("Measure(s)", 2)
#             # right.metric("Time dimension(s)", 0)

#             # left.dataframe({"Dimensions": ["Foo", "Bar"]}, hide_index=True, use_container_width=True)
#             # with left:
#             #     borderless_button("Edit", key=row.Name + "-dimensions", use_container_width=True)
#             # middle.dataframe({"Measures": ["Foo", "Bar"]}, hide_index=True, use_container_width=True)
#             # with middle:
#             #     borderless_button("Edit", key=row.Name + "-measures", use_container_width=True)
#             # right.dataframe({"Time dimensions": ["Foo", "Bar"]}, hide_index=True, use_container_width=True)
#             # with right:
#             #     borderless_button("Edit", key=row.Name + "-time-dimensions", use_container_width=True)

# with cells[2]:
#     with st.container(border=True):
#         borderless_button(" +   Create new table", key="new-table", use_container_width=True, on_click=add_new_table)