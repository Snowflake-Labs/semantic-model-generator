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


def validate_sample_values_are_quoted(yaml_str: str) -> None:
    """
    Validate that all sample_values in the provided YAML data are wrapped in quotes.

    """
    inside_sample_values = False
    for line in yaml_str.split("\n"):
        line = line.strip()
        if len(line) == 0:
            continue

        if "sample_values" in line:
            inside_sample_values = True
            continue
        # Check if we are still in the list of sample values, or if we moved to another block element or a new table.
        if inside_sample_values and (line[0] != "-" or "- name:" in line):  # reset
            inside_sample_values = False
            continue
        if inside_sample_values:
            # ensure all quoted.
            # count single and double quotes.
            if line.count("'") != 2 and line.count('"') != 2:
                raise ValueError(
                    f"You need to have all sample_values: surrounded by quotes. Please fix the value {line}."
                )
