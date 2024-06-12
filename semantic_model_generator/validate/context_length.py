import copy

from semantic_model_generator.data_processing.proto_utils import (
    proto_to_yaml,
    yaml_to_semantic_model,
)

_MODEL_CONTEXT_LENGTH_TOKENS = 6500  # We use 6k, with 1.2k for prompt, so that we can reserve 500 for response tokens (average is about 300).
_MODEL_CONTEXT_INSTR_TOKEN = 20  # buffer for instr tokens


def validate_context_length(yaml_str: str, remove_vqr: bool = True) -> None:
    """
    Validate the token limit for the model with space for the prompt.

    yaml_str: The yaml semantic model
    remove_vqr: Whether to remove the VQR from total token count. As these are indexed separately, default to True.
    """
    # Convert back to a model to remove sections that don't count against token limit (i.e. VQR).
    yaml_str_copy = copy.deepcopy(yaml_str)
    if remove_vqr:
        model = yaml_to_semantic_model(yaml_str_copy)
        model.ClearField("verified_queries")
        yaml_str_copy = proto_to_yaml(model)

    # Pass in the str version of the semantic context yaml.
    # This isn't exactly how many tokens the model will be, but should roughly be correct.
    TOTAL_TOKENS_LIMIT = _MODEL_CONTEXT_LENGTH_TOKENS + _MODEL_CONTEXT_INSTR_TOKEN
    CHARS_PER_TOKEN = 4  # as per https://help.openai.com/en/articles/4936856-what-are-tokens-and-how-to-count-them
    if len(yaml_str_copy) // CHARS_PER_TOKEN > TOTAL_TOKENS_LIMIT:
        raise ValueError(
            f"Your semantic model is too large. Passed size is {len(yaml_str_copy)} characters. We need you to remove {((len(yaml_str_copy) // CHARS_PER_TOKEN)-TOTAL_TOKENS_LIMIT ) *CHARS_PER_TOKEN } characters in your semantic model. Please check: \n (1) If you have long descriptions that can be truncated. \n (2) If you can remove some columns that are not used within your tables. \n (3) If you have extra tables you do not need. \n (4) If you can remove sample values."
        )
