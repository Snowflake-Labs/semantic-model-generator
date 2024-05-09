from semantic_model_generator.protos.semantic_model_pb2 import Table


def validate_contains_datatype_for_each_col(table: Table) -> None:
    # Ensure every col for every table has 'data_type' present.
    for dim_col in table.dimensions:
        if (
            dim_col.data_type is None or len(dim_col.data_type.strip()) == 0
        ):  # account for spaces
            raise ValueError(
                f"Your Semantic Model contains a col {dim_col.name} that does not have the `data_type` field. Please add."
            )
    for measure_col in table.measures:
        if (
            measure_col.data_type is None or len(measure_col.data_type.strip()) == 0
        ):  # account for spaces
            raise ValueError(
                f"Your Semantic Model contains a col {measure_col.name} that does not have the `data_type` field. Please add."
            )
    for time_dim_col in table.time_dimensions:
        if (
            time_dim_col.data_type is None or len(time_dim_col.data_type.strip()) == 0
        ):  # account for spaces
            raise ValueError(
                f"Your Semantic Model contains a col {time_dim_col.name} that does not have the `data_type` field. Please add."
            )
