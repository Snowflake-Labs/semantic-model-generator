import os

import pytest
from strictyaml import YAMLValidationError

from semantic_model_generator.data_processing.proto_utils import yaml_to_semantic_model


def test_success():
    this_dir = os.path.dirname(os.path.realpath(__file__))
    yaml_path = os.path.join(this_dir, "samples/jaffle_shop.yaml")
    with open(yaml_path) as f:
        yaml_str = f.read()
        assert yaml_to_semantic_model(yaml_str) is not None


def test_missing_required_field():
    with pytest.raises(
        YAMLValidationError, match=r"aaa.*required key.*data_type.*not found.*"
    ):
        this_dir = os.path.dirname(os.path.realpath(__file__))
        yaml_path = os.path.join(this_dir, "samples/jaffle_shop_missing_required.yaml")
        with open(yaml_path) as f:
            yaml_str = f.read()
            yaml_to_semantic_model(yaml_str)


def test_wrong_sample_value_type():
    # strictyaml auto converts any other type to string to comply with the schema
    this_dir = os.path.dirname(os.path.realpath(__file__))
    yaml_path = os.path.join(this_dir, "samples/jaffle_shop_date_sample.yaml")
    with open(yaml_path) as f:
        yaml_str = f.read()
        assert yaml_to_semantic_model(yaml_str) is not None
