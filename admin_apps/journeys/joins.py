import streamlit as st

from semantic_model_generator.protos import semantic_model_pb2


@st.dialog("Join Builder", width="large")
def joins_dialog() -> None:

    if "builder_joins" not in st.session_state:
        st.session_state.builder_joins = st.session_state.semantic_model.relationships

    # For each relationship, render a relationship builder
    for idx, relationship in enumerate(st.session_state.builder_joins):
        with st.expander(f"Join {idx}"):
            try:
                default_left_table = [
                    table.name for table in st.session_state.semantic_model.tables
                ].index(relationship.left_table)
                default_right_table = [
                    table.name for table in st.session_state.semantic_model.tables
                ].index(relationship.right_table)
            except ValueError:
                default_left_table = 0
                default_right_table = 0
            left_table = st.selectbox(
                "Left Table",
                options=[
                    table.name for table in st.session_state.semantic_model.tables
                ],
                index=default_left_table,
                key=f"left_table_{idx}",
            )

            right_table = st.selectbox(
                "Right Table",
                options=[
                    table.name for table in st.session_state.semantic_model.tables
                ],
                index=default_right_table,
                key=f"right_table_{idx}",
            )

            join_type = st.selectbox(
                "Join Type",
                options=["inner", "left_outer"],
                index=relationship.join_type,
                key=f"join_type_{idx}",
            )

            relationship_type = st.selectbox(
                "Relationship Type",
                options=[
                    "one_to_one",
                    "many_to_one",
                ],
                index=relationship.relationship_type,
                key=f"relationship_type_{idx}",
            )

            for col_idx, join_cols in enumerate(relationship.relationship_columns):
                left_table_object = next(
                    (
                        table
                        for table in st.session_state.semantic_model.tables
                        if table.name == left_table
                    )
                )

                right_table_object = next(
                    (
                        table
                        for table in st.session_state.semantic_model.tables
                        if table.name == right_table
                    )
                )
                try:
                    left_columns = []
                    left_columns.extend(left_table_object.columns)
                    left_columns.extend(left_table_object.dimensions)
                    left_columns.extend(left_table_object.time_dimensions)
                    left_columns.extend(left_table_object.measures)

                    right_columns = []
                    right_columns.extend(right_table_object.columns)
                    right_columns.extend(right_table_object.dimensions)
                    right_columns.extend(right_table_object.time_dimensions)
                    right_columns.extend(right_table_object.measures)

                    default_left_col = [col.name for col in left_columns].index(
                        join_cols.left_column
                    )
                    default_right_col = [col.name for col in right_columns].index(
                        join_cols.right_column
                    )
                except ValueError:
                    default_left_col = 0
                    default_right_col = 0
                left_col = st.selectbox(
                    "Left Column",
                    options=[col.name for col in left_columns],
                    index=default_left_col,
                    key=f"left_col_{idx}_{col_idx}",
                )
                right_col = st.selectbox(
                    "Right Column",
                    options=[col.name for col in right_columns],
                    index=default_right_col,
                    key=f"right_col_{idx}_{col_idx}",
                )

            if st.button("Add join keys", key=f"add_join_keys_{idx}"):
                relationship.relationship_columns.append(
                    semantic_model_pb2.RelationKey(
                        left_column="",
                        right_column="",
                    )
                )

    # If the user clicks "Add join", add a new join to the relationships list
    if st.button("Add join"):
        st.session_state.semantic_model.relationships.append(
            semantic_model_pb2.Relationship(
                left_table="",
                right_table="",
                join_type=semantic_model_pb2.JoinType.inner,
                relationship_type=semantic_model_pb2.RelationshipType.one_to_one,
                relationship_columns=[],
            )
        )

    # If the user clicks "Save", save the relationships list to the session state
    if st.button("Save"):
        st.session_state.semantic_model.relationships = st.session_state.builder_joins
        st.rerun()
