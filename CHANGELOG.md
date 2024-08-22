# Changelog

You must follow the format of `## [VERSION-NUMBER]` for the GitHub workflow to pick up the text.

## [0.1.33] - 2024-08-07

### Updates

- Throw an error during validation if a user adds duplicate verified queries to their semantic model.

## [0.1.32] - 2024-07-30

### Updates

- Bump context length validation limit.
- Fix union type hints for support with Python <3.10.

## [0.1.31] - 2024-07-29

### Updates

- Include new `secure-local-storage` extra package for `snowflake-python-connector` dependency.

## [0.1.30] - 2024-07-12

### Updates

- Restrict Python version to < 3.12 in order to avoid issues with pyarrow dependency.

## [0.1.29] - 2024-07-10

### Updates

- Allow single sign on auth.

## [0.1.28] - 2024-07-09

### Updates

- Allow auto-generation of descriptions for semantic models.

## [0.1.27] - 2024-07-03

### Updates

- Fix VQR validation for measures with aggregation calculation.
- Update pulling sample value by dimension vs. measures; fix length validation logic.

## [0.1.26] - 2024-07-02

### Updates

- Semantic model size validation allows for many more sample values.
  This corresponds with a release of the Cortex Analyst that does dynamic sample value retrieval by default.

## [0.1.25] - 2024-06-18

### Updates

- Plumb through column and table comments
- Skip host name match verification for now

## [0.1.24] - 2024-06-17

### Updates

- Consolidate validations to use the same set of utils
- Handle the validation for expr with aggregations properly

## [0.1.23] - 2024-06-13

### Updates

- Remove VQR from context length calculation.
- Add toggle for number of sample values.

## [0.1.22] - 2024-06-11

### Updates

- Fix small streamlit app components to be compatible with python 3.8

## [0.1.21] - 2024-06-10

### Updates

- Add validation for verified queries;
- Add streamlit admin app for semantic model generation, validation and verified query flow.

## [0.1.20] - 2024-05-31

### Updates

- Fix for validation CLI and README

## [0.1.19] - 2024-05-31

### Updates

- Fix protobuf version to be compatible with streamlit
- Small refactor in validation file

## [0.1.18] - 2024-05-31

### Updates

- Add proto definition for verified queries; also add proto for Column (for backward compatibility only)

## [0.1.17] - 2024-05-21

### Updates

- Allow flow style in yaml validation

## [0.1.16] - 2024-05-15

### Updates

- Remove validation of context length to after save.
- Uppercase db/schema/table(s)

## [0.1.15] - 2024-05-14

### Updates

- Use strictyaml to validate the semantic model yaml matches the expected schema and has all required fields

## [0.1.14] - 2024-05-13

### Updates

- Fix aggregations
- Context limit

## [0.1.13] - 2024-05-08

### Updates

- Object types not supported in generation or validation.

## [0.1.12] - 2024-05-03

### Updates

- Naming
- Validate no expressions in cols in yaml

## [0.1.11] - 2024-05-01

### Updates

- Save path location

## [0.1.10] - 2024-05-01

### Updates

- Save path location

## [0.1.9] - 2024-04-29

### Updates

- Add additional validation for mismatched quotes. Test incorrect enums.

## [0.1.8] - 2024-04-23

### Updates

- run select against given cols in semantic model for validation

## [0.1.7] - 2024-04-18

### Updates

- Parse yaml model into protos, validate cols and col naming

## [0.1.6] - 2024-04-16

### Updates

- First yaml validation included.

## [0.1.5] - 2024-04-15d

### Updates

- Downgrade pyarrow

## [0.1.4] - 2024-04-15c

### Updates

- Spacing typo

## [0.1.3] - 2024-04-15b

### Updates

- Fix 3.8 typing
- Some function renaming
- Support all Snowflake datatypes

## [0.1.2] - 2024-04-15

### Updates

- Downgrade to python 3.8 and resolve typing issues with optional.
- Fix FQN parts for pydantic errors.
- Update README to be less restrictive for installs.

## [0.1.1] - 2024-04-09

### Released

- Verify release workflow works as intended

## [0.1.0] - 2024-04-08

### Released

- Initial release of the project.
