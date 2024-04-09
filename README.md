# semantic-model-generator

The `Semantic Model Generator` is used to generate a semantic model for use in your Snowflake account.

## Setup

This project uses `poetry` to manage dependencies and we recommend `pyenv` for python version management.

1. `make setup`

Next, export your credentials as environment variables. Note, `host` is optional depending on your Snowflake deployment.

```bash
export SNOWFLAKE_ROLE = "<your-snowflake-role>"
export SNOWFLAKE_WAREHOUSE =  "<your-snowflake-warehouse>"
export SNOWFLAKE_USER =  "<your-snowflake-user>"
export SNOWFLAKE_PASSWORD = "<your-snowflake-password>"
export SNOWFLAKE_HOST = "<your-snowflake-host>"
```

## Usage

You may generate a semantic model for a given list of fully qualified tables following the `{database}.{schema}.{table}` format.

All generated semantic models by default are saved under `semantic_model_generator/output_models`.

**Important**: After generation, your YAML files will have a series of lines with `# <FILL-OUT>`. Please take the time to fill these out with your business context. In addition, if there are columns included that are not useful for your internal teams, please remove them from the semantic model.


```bash
python -m semantic_model_generator.main \
    --fqn_tables "['<your-database-name-1>.<your-schema-name-1>.<your-table-name-1>','<your-database-name-2>.<your-schema-name-2>.<your-table-name-2>']" \
    --snowflake_account="<your-snowflake-account>"
```

## Release

In order to push a new build and release, follow the below steps.

1. Checkout a new branch from main. Please name this branch `release-YYYY-MM-DD`. 
2. Bump the poetry and github tags depending on if this is a patch, minor, or major version update:
    * `export TYPE=patch make update-version`
    * `export TYPE=minor make update-version`
    * `export TYPE=major make update-version`
3. Update the `CHANGELOG.md` adding a relevant header for your version number along with a description of the changes made.
4. Commit the updated `pyproject.toml` and `CHANGELOG.md` and push.
5. Merge your branch.
6. Push the updated tags to trigger the release workflow with `make release`.

