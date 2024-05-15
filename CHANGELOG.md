# Changelog

You must follow the format of `## [VERSION-NUMBER]` for the GitHub workflow to pick up the text.

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
