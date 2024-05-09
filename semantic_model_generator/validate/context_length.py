_MODEL_CONTEXT_LENGTH = 7000  # We use 7k so that we can reserve 1k for response tokens.
_MODEL_CONTEXT_INSTR_TOKEN = 20 # buffer for instr tokens

def validate_context_length(yaml_str: str) -> None:
    # Pass in the str version of the semantic context yaml.
    # This isn't exactly how many tokens the model will be, but should roughly be correct.
    CHARS_PER_TOKEN = 4  # as per https://help.openai.com/en/articles/4936856-what-are-tokens-and-how-to-count-them
    if len(yaml_str) // CHARS_PER_TOKEN > _MODEL_CONTEXT_LENGTH+ _MODEL_CONTEXT_INSTR_TOKEN:
        raise ValueError(
            f"Your semantic model is too large. Passed size is {len(yaml_str)} characters. We need you to remove {((len(yaml_str) // CHARS_PER_TOKEN)-_MODEL_CONTEXT_LENGTH + _MODEL_CONTEXT_INSTR_TOKEN  ) *CHARS_PER_TOKEN } characters in your semantic model. Please check: \n (1) If you have long descriptions that can be truncated. \n (2) If you can remove some columns that are not used within your tables. \n (3) If you have extra tables you do not need. \n (4) If you can remove sample values."
        )
