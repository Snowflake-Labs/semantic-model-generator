from typing import TypeVar

from google.protobuf.message import Message
from loguru import logger

from semantic_model_generator.data_processing.proto_utils import proto_to_yaml

ProtoMsg = TypeVar("ProtoMsg", bound=Message)
_MODEL_CONTEXT_LENGTH_TOKENS = 6500  # We use 6.5k, with 1.2k for instructions, so that we can reserve 500 for response tokens (average is 300).
_MODEL_CONTEXT_INSTR_TOKEN = 20  # buffer for instr tokens


def validate_context_length(model: ProtoMsg, throw_error: bool = False) -> None:
    """
    Validate the token limit for the model with space for the prompt.

    yaml_model: The yaml semantic model
    throw_error: Should this function throw an error or just a warning.
    """

    model.ClearField("verified_queries")
    yaml_str = proto_to_yaml(model)
    # Pass in the str version of the semantic context yaml.
    # This isn't exactly how many tokens the model will be, but should roughly be correct.
    TOTAL_TOKENS_LIMIT = _MODEL_CONTEXT_LENGTH_TOKENS + _MODEL_CONTEXT_INSTR_TOKEN
    CHARS_PER_TOKEN = 4  # as per https://help.openai.com/en/articles/4936856-what-are-tokens-and-how-to-count-them
    if len(yaml_str) // CHARS_PER_TOKEN > TOTAL_TOKENS_LIMIT:
        if throw_error:
            raise ValueError(
                f"Your semantic model is too large. Passed size is {len(yaml_str)} characters. We need you to remove {((len(yaml_str) // CHARS_PER_TOKEN)-TOTAL_TOKENS_LIMIT ) *CHARS_PER_TOKEN } characters in your semantic model. Please check: \n (1) If you have long descriptions that can be truncated. \n (2) If you can remove some columns that are not used within your tables. \n (3) If you have extra tables you do not need. \n (4) If you can remove sample values."
            )
        else:
            logger.warning(
                f"WARNING 🚨: The Semantic model is too large. \n Passed size is {len(yaml_str)} characters. We need you to remove {((len(yaml_str) // CHARS_PER_TOKEN)-TOTAL_TOKENS_LIMIT ) *CHARS_PER_TOKEN } characters in your semantic model. Please check: \n (1) If you have long descriptions that can be truncated. \n (2) If you can remove some columns that are not used within your tables. \n (3) If you have extra tables you do not need. \n (4) If you can remove sample values. \n Once you've finished updating, please validate your semantic model."
            )
