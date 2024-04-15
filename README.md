# semantic-model-generator

The `Semantic Model Generator` is used to generate a semantic model for use in your Snowflake account.

## Setup

This project uses `poetry` to manage dependencies and we recommend `pyenv` for python version management.

1. `make setup`
* Make setup will install poetry if needed.
* For optional dependencies, brew and pyenv, you can install `make install-pyenv` and `make install-homebrew` but this is not required. 

Next, export your credentials as environment variables. Note, `host` is optional depending on your Snowflake deployment.

2. On a Mac: 
```bash
export SNOWFLAKE_ROLE="<your-snowflake-role>"
export SNOWFLAKE_WAREHOUSE="<your-snowflake-warehouse>"
export SNOWFLAKE_USER="<your-snowflake-user>"
export SNOWFLAKE_PASSWORD="<your-snowflake-password>"
export SNOWFLAKE_HOST="<your-snowflake-host>"
```

3. On a PC:
```bash
set SNOWFLAKE_ROLE=<your-snowflake-role>
set SNOWFLAKE_WAREHOUSE=<your-snowflake-warehouse>
set SNOWFLAKE_USER=<your-snowflake-user>
set SNOWFLAKE_PASSWORD=<your-snowflake-password>
set SNOWFLAKE_HOST=<your-snowflake-host>
```


## Usage

### Generation

You may generate a semantic model for a given list of fully qualified tables following the `{database}.{schema}.{table}` format. Each table in this list should be a physical table or a view present in your database.

All generated semantic models by default are saved under `semantic_model_generator/output_models`.

1. `poetry shell` . This will activate your virtual environment.

2. 
```bash
python -m semantic_model_generator.generate_model \
    --physical_tables  "['<your-database-name-1>.<your-schema-name-1>.<your-physical-table-or-view-name-1>','<your-database-name-2>.<your-schema-name-2>.<your-physical-table-or-view-name-2>']" \
    --semantic_model_name "<a-meaningful-semantic-model-name>" \
    --snowflake_account="<your-snowflake-account>"
```

### Post-Generation

**Important**: After generation, your YAML files will have a series of lines with `# <FILL-OUT>`. Please take the time to fill these out with your business context. 

By default, the generated semantic model will contain all columns from the provided tables/views. However, it's highly encouraged to only keep relevant columns and drop any unwanted columns from the generated semantic model.

In addition, consider adding the following elements to your semantic model:

1. Logical columns for a given table/view that are expressions over physical columns.
    * Example: `col1 - col2` could be the `expr` for a logical column.
2. Synonyms. Any additional synonyms for column names.
3. Filters. Additional filters with their relevant `expr`.

## Release

In order to push a new build and release, follow the steps below. Note, only admins are allowed to push `release/v` tags.

1. Checkout a new branch from main. You should name this branch `release/vYYYY-MM-DD`.
2. Bump the poetry:
    * `poetry version patch`
    * `poetry version minor`
    * `poetry version major`
3. Update the `CHANGELOG.md` adding a relevant header for your version number along with a description of the changes made.
4. Run `make build` to create a new .whl file.
5. After approval, run `make release` which will commit `CHANGELOG.md`, `pyproject.toml`, and the .whl files under `dist` and cut a new release.
6. Merge in your pr.
