from typing import Any, TypeVar

from google.protobuf.message import Message
from loguru import logger

from semantic_model_generator.data_processing.proto_utils import proto_to_yaml

ProtoMsg = TypeVar("ProtoMsg", bound=Message)

# Max total tokens is 8200.
# We reserve 500 tokens for response (average response is 300 tokens).
# So the prompt token limit is 7700.
# We reserve 1220 tokens for model instructions, separate from the semantic model.
# Thus, the semantic model will get about 6480 tokens,
# with some more discounting for retrieved literals.
_TOTAL_PROMPT_TOKEN_LIMIT = 7700
_BASE_INSTRUCTION_TOKEN_LENGTH = 1220
#  Estimated 10 tokens per literals since each literal is presented as a filter expression
#  (i.e. table.column = 'literal').
#  Currently 10 literals are retrieved per search.
_TOKENS_PER_LITERAL = 10
_NUM_LITERAL_RETRIEVALS = 10

# As per https://help.openai.com/en/articles/4936856-what-are-tokens-and-how-to-count-them
_CHARS_PER_TOKEN = 4

# Max number of sample values we include in the semantic model representation.
_MAX_SAMPLE_VALUES = 3


def _get_field(msg: ProtoMsg, field_name: str) -> Any:
    fields = [value for fd, value in msg.ListFields() if fd.name == field_name]
    if not fields:
        return None
    return fields[0]


def _truncate_sample_values(model: ProtoMsg) -> None:
    tables = _get_field(model, "tables")
    if not tables:
        return
    for table in tables:
        dimensions = _get_field(table, "dimensions")
        measures = _get_field(table, "measures")
        if dimensions:
            for dimension in dimensions:
                sample_values = _get_field(dimension, "sample_values")
                if sample_values:
                    del sample_values[_MAX_SAMPLE_VALUES:]
        if measures:
            for measure in measures:
                sample_values = _get_field(measure, "sample_values")
                if sample_values:
                    del sample_values[_MAX_SAMPLE_VALUES:]


def _count_search_services(model: ProtoMsg) -> int:
    cnt = 0
    tables = _get_field(model, "tables")
    if not tables:
        return 0

    for table in tables:
        dimensions = _get_field(table, "dimensions")
        if not dimensions:
            continue
        for dimension in dimensions:
            if _get_field(dimension, "cortex_search_service_name"):
                cnt += 1
    return cnt


def validate_context_length(model: ProtoMsg, throw_error: bool = False) -> None:
    """
    Validate the token limit for the model with space for the prompt.

    yaml_model: The yaml semantic model
    throw_error: Should this function throw an error or just a warning.
    """

    model.ClearField("verified_queries")
    _truncate_sample_values(model)
    num_search_services = _count_search_services(model)

    yaml_str = proto_to_yaml(model)
    # Pass in the str version of the semantic context yaml.
    # This isn't exactly how many tokens the model will be, but should roughly be correct.
    literals_buffer = (
        _TOKENS_PER_LITERAL * _NUM_LITERAL_RETRIEVALS * (1 + num_search_services)
    )
    approx_instruction_length = _BASE_INSTRUCTION_TOKEN_LENGTH + literals_buffer
    model_tokens_limit = _TOTAL_PROMPT_TOKEN_LIMIT - approx_instruction_length
    model_tokens = len(yaml_str) // _CHARS_PER_TOKEN
    if model_tokens > model_tokens_limit:
        tokens_to_remove = model_tokens - model_tokens_limit
        chars_to_remove = tokens_to_remove * _CHARS_PER_TOKEN
        if throw_error:
            raise ValueError(
                f"Your semantic model is too large. "
                f"Passed size is {len(yaml_str)} characters. "
                f"We need you to remove {chars_to_remove} characters in your semantic model. "
                f"Please check: \n"
                f" (1) If you have long descriptions that can be truncated. \n"
                f" (2) If you can remove some columns that are not used within your tables. \n"
                f" (3) If you have extra tables you do not need."
            )
        else:
            logger.warning(
                f"WARNING 🚨: The Semantic model is too large. \n"
                f"Passed size is {len(yaml_str)} characters. "
                f"We need you to remove {chars_to_remove} characters in your semantic model. "
                f"Please check: \n"
                f" (1) If you have long descriptions that can be truncated. \n"
                f" (2) If you can remove some columns that are not used within your tables. \n"
                f" (3) If you have extra tables you do not need. \n"
                f" Once you've finished updating, please validate your semantic model."
            )
