# semantic-model-generator

The `Semantic Model Generator` is used to generate a semantic model for use in your Snowflake account.

## Setup

We currently leverage credentials saved as environment variables. Note, `host` is optional depending on your Snowflake deployment.

A. To find your Account Name, [follow these instructions](https://docs.snowflake.com/en/sql-reference/functions/current_account_name), or execute the following sql command in your account.

```sql
SELECT CURRENT_ACCOUNT_NAME()
```

B. To find the host for your account, [follow these instructions](https://docs.snowflake.com/en/user-guide/organizations-connect#connecting-with-a-url).


1. To set these on Mac OS/Linux: 
```bash
export SNOWFLAKE_ROLE="<your-snowflake-role>"
export SNOWFLAKE_WAREHOUSE="<your-snowflake-warehouse>"
export SNOWFLAKE_USER="<your-snowflake-user>"
export SNOWFLAKE_PASSWORD="<your-snowflake-password>"
export SNOWFLAKE_HOST="<your-snowflake-host>"
```

2. To set these on windows:
```bash
set SNOWFLAKE_ROLE=<your-snowflake-role>
set SNOWFLAKE_WAREHOUSE=<your-snowflake-warehouse>
set SNOWFLAKE_USER=<your-snowflake-user>
set SNOWFLAKE_PASSWORD=<your-snowflake-password>
set SNOWFLAKE_HOST=<your-snowflake-host>
```

3. To set these within a python environment:
```python
import os

# Setting environment variables
os.environ['SNOWFLAKE_ROLE'] = '<your-snowflake-role>'
os.environ['SNOWFLAKE_WAREHOUSE'] = '<your-snowflake-warehouse>'
os.environ['SNOWFLAKE_USER'] = '<your-snowflake-user>'
os.environ['SNOWFLAKE_PASSWORD'] = '<your-snowflake-password>'
os.environ['SNOWFLAKE_HOST'] = '<your-snowflake-host>'
```
## Usage

You may generate a semantic model for a given list of fully qualified tables following the `{database}.{schema}.{table}` format. Each table in this list should be a physical table or a view present in your database.

All generated semantic models by default are saved under `semantic_model_generator/output_models`.

### Generation - Python

1. Ensure you have installed the python package. Note, the version below should be the latest version under the `dist/` directory.
```bash
pip install dist/semantic_model_generator-0.1.5-py3-none-any.whl
```
2. Activate python shell
```bash
python
```
3. Generate a semantic model
```python
from semantic_model_generator.generate_model import generate_base_semantic_model_from_snowflake

PHYSICAL_TABLES = ['<your-database-name-1>.<your-schema-name-1>.<your-physical-table-or-view-name-1>','<your-database-name-2>.<your-schema-name-2>.<your-physical-table-or-view-name-2>']
SNOWFLAKE_ACCOUNT = "<your-snowflake-account>"
SEMANTIC_MODEL_NAME = "<a-meaningful-semantic-model-name>"

generate_base_semantic_model_from_snowflake(
    physical_tables=PHYSICAL_TABLES,
    snowflake_account=SNOWFLAKE_ACCOUNT,
    semantic_model_name=SEMANTIC_MODEL_NAME
)
```


### Generation - CLI
Unlike the python route above, using the CLI assumes that you will manage your environment using `poetry` and `pyenv` for python versions.
This has only been tested on Mas OS/Linux.

1. If you need brew, `make install-homebrew`.
2. If you need pyenv, `make install-pyenv` and `make install-python-3.8`.
3. `make setup` Make setup will install poetry if needed.


This is the script version run on the command line.
1. `poetry shell` . This will activate your virtual environment.
2. Run on your command line.
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

You should follow the setup commands from usage-cli to install poetry and create your environment.

1. Checkout a new branch from main. You should name this branch `release/vYYYY-MM-DD`.
2. Bump the poetry:
    * `poetry version patch`
    * `poetry version minor`
    * `poetry version major`
3. Update the `CHANGELOG.md` adding a relevant header for your version number along with a description of the changes made.
4. Run `make build` to create a new .whl file.
5. Push your files for approval.
6. After approval, run `make release` which will cut a new release and attach the .whl file.
7. Merge in your pr.
