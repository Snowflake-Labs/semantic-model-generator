import jsonargparse
from loguru import logger

from semantic_model_generator.data_processing.proto_utils import yaml_to_semantic_model


def validate(yaml_path: str) -> None:
    """
    For now, validate just ensures that the yaml is correctly formatted and we can parse into our protos.

    TODO: ensure that all expressions are valid.
    """
    with open(yaml_path) as f:
        yaml_str = f.read()
    _ = yaml_to_semantic_model(yaml_str)
    logger.info(f"Successfully validated {yaml_path}")


if __name__ == "__main__":
    jsonargparse.CLI(
        validate,
        as_positional=False,
    )
