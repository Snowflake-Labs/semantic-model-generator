# semantic-model-generator

The `Semantic Model Generator` is used to generate a semantic model for use in your Snowflake account.

You can generate semantic models through our Streamlit app, the command line, or directly in your Python code.
Please complete the instructions in [setup](#setup), then proceed to the instructions for your desired approach.

If you want to see what a semantic model looks like, skip to [Examples](#examples).

## Table of Contents

  * [Table of Contents](#table-of-contents)
  * [Setup](#setup)
  * [Streamlit App](#streamlit-app)
  * [CLI Tool](#cli-tool)
    + [Generation](#generation)
    + [Validation](#validation)
  * [Python](#python)
    + [Generation](#generation-1)
    + [Validation](#validation-1)
  * [Usage](#usage)
    + [Semantic Model Context Length Constraints](#semantic-model-context-length-constraints)
    + [Auto-Generated Descriptions](#auto-generated-descriptions)
    + [Additional Fields to Fill Out](#additional-fields-to-fill-out)
  * [Examples](#examples)
  * [Release](#release)


## Setup

We currently leverage credentials saved as environment variables.

A. To find your Account locator, please execute the following sql command in your account.

```sql
SELECT CURRENT_ACCOUNT_LOCATOR();
```

B. To find the `SNOWFLAKE_HOST` for your
account, [follow these instructions](https://docs.snowflake.com/en/user-guide/organizations-connect#connecting-with-a-url).

* Currently we recommend you to look under the `Account locator (legacy)` method of connection for better compatibility
  on API.
* It typically follows format of: `<accountlocator>.<region>.<cloud>.snowflakecomputing.com`. Ensure that you omit the `https://` prefix.
* `SNOWFLAKE_HOST` is required if you are using the Streamlit app, but may not be required for the CLI tool depending on
  your Snowflake deployment. We would recommend setting it regardless.

We recommend setting these environment variables by creating a `.env` file in the root directory of this repo. See the
examples in [`.env.example`](.env.example) for reference and proper syntax for `.env` files.

However, if you would like to set these variables directly in your shell/Python environment,

1. MacOS/Linux syntax:

```bash
export SNOWFLAKE_ROLE="<your-snowflake-role>"
export SNOWFLAKE_WAREHOUSE="<your-snowflake-warehouse>"
export SNOWFLAKE_USER="<your-snowflake-user>"
export SNOWFLAKE_ACCOUNT_LOCATOR="<your-snowflake-account-locator>"
export SNOWFLAKE_HOST="<your-snowflake-host>"
```

2. Windows syntax:

```bash
set SNOWFLAKE_ROLE=<your-snowflake-role>
set SNOWFLAKE_WAREHOUSE=<your-snowflake-warehouse>
set SNOWFLAKE_USER=<your-snowflake-user>
set SNOWFLAKE_ACCOUNT_LOCATOR=<your-snowflake-account-locator>
set SNOWFLAKE_HOST=<your-snowflake-host>
```

3. Python syntax:

```python
import os

# Setting environment variables
os.environ['SNOWFLAKE_ROLE'] = '<your-snowflake-role>'
os.environ['SNOWFLAKE_WAREHOUSE'] = '<your-snowflake-warehouse>'
os.environ['SNOWFLAKE_USER'] = '<your-snowflake-user>'
os.environ['SNOWFLAKE_ACCOUNT_LOCATOR'] = '<your-snowflake-account-locator>'
os.environ['SNOWFLAKE_HOST'] = '<your-snowflake-host>'
```

Our semantic model generators currently support three types of authentication. If no `SNOWFLAKE_AUTHENTICATOR`
environment variable
is set, the default is `snowflake`, which uses standard username/password support.

1. Username and Password

**Note**: If you have MFA enabled, using this default authenticator should send a push notification to your device.

```bash
# no SNOWFLAKE_AUTHENTICATOR needed
SNOWFLAKE_PASSWORD="<your-snowflake-password>"

# MacOS/Linux
export SNOWFLAKE_PASSWORD="<your-snowflake-password>"

# Windows
set SNOWFLAKE_PASSWORD=<your-snowflake-password>

# Python
os.environ['SNOWFLAKE_PASSWORD'] = '<your-snowflake-password>'
```

2. Username/Password with MFA passcode

Using a passcode from your authenticator app:

```bash
SNOWFLAKE_AUTHENTICATOR="username_password_mfa"
SNOWFLAKE_PASSWORD="<your-snowflake-password>"
SNOWFLAKE_MFA_PASSCODE="<your-snowflake-mfa-passcode>" # if your authenticator app reads "123 456", fill in "123456" (No spaces)

# MacOS/Linux
export SNOWFLAKE_AUTHENTICATOR="username_password_mfa"
export SNOWFLAKE_PASSWORD="<your-snowflake-password>"
export SNOWFLAKE_MFA_PASSCODE="<your-snowflake-mfa-passcode>"

# Windows
set SNOWFLAKE_AUTHENTICATOR=username_password_mfa
set SNOWFLAKE_PASSWORD=<your-snowflake-password>
set SNOWFLAKE_MFA_PASSCODE=<your-snowflake-mfa-passcode>

# Python
os.environ['SNOWFLAKE_AUTHENTICATOR'] = 'username_password_mfa'
os.environ['SNOWFLAKE_PASSWORD'] = '<your-snowflake-password>'
os.environ['SNOWFLAKE_MFA_PASSCODE'] = '<your-snowflake-mfa-passcode>'
```

Using a passcode embedded in the password:

```bash
SNOWFLAKE_AUTHENTICATOR="username_password_mfa"
SNOWFLAKE_PASSWORD="<your-snowflake-password>"
SNOWFLAKE_MFA_PASSCODE_IN_PASSWORD="true"

# MacOS/Linux
export SNOWFLAKE_AUTHENTICATOR="username_password_mfa"
export SNOWFLAKE_PASSWORD="<your-snowflake-password>"
export SNOWFLAKE_MFA_PASSCODE_IN_PASSWORD="true"

# Windows
set SNOWFLAKE_AUTHENTICATOR=username_password_mfa
set SNOWFLAKE_PASSWORD=<your-snowflake-password>
set SNOWFLAKE_MFA_PASSCODE_IN_PASSWORD=true

# Python
os.environ['SNOWFLAKE_AUTHENTICATOR'] = 'username_password_mfa'
os.environ['SNOWFLAKE_PASSWORD'] = '<your-snowflake-password>'
os.environ['SNOWFLAKE_MFA_PASSCODE_IN_PASSWORD'] = 'true'
```

3. Single Sign-On (SSO) with Okta

```bash
# no SNOWFLAKE_PASSWORD needed
SNOWFLAKE_AUTHENTICATOR="externalbrowser"

# MacOS/Linux
export SNOWFLAKE_AUTHENTICATOR="externalbrowser"

# Windows
set SNOWFLAKE_AUTHENTICATOR=externalbrowser

# Python
os.environ['SNOWFLAKE_AUTHENTICATOR'] = 'externalbrowser'
```

## Streamlit App

We offer a convenient Streamlit app that supports creating semantic models from scratch as well as iterating on existing ones uploaded to a Snowflake stage.

To install dependencies for the Streamlit app, run

```bash
make setup_admin_app
```

Once installed, you can run the app using the provided Makefile target, or with your current version of Python manually specified:

```bash
# Make target
make run_admin_app

# directly
python3.11 -m streamlit run admin_apps/app.py
```

## CLI Tool

You may also generate a semantic model directly from the CLI. To do this, first install the CLI tool dependencies, which differ from the Streamlit app's dependencies.

Unlike the Streamlit route above, using the CLI assumes that you will manage your environment using `poetry` and `pyenv` for Python versions.
This has only been tested on MacOS/Linux.

1. If you need brew, run `make install-homebrew`.
2. If you need pyenv, `make install-pyenv` and `make install-python-3.8`.
3. Run `make setup` to install all external dependencies into your Poetry environment. This will also install `poetry` if needed.
4. Spawn a shell in the virtual environment using `poetry shell`. This will activate your virtual environment.


### Generation
You are now ready to generate semantic models via the CLI! The generation command uses the following syntax:

```bash
python -m semantic_model_generator.generate_model \
    --base_tables  "['<your-database-name-1>.<your-schema-name-1>.<your-base-table-or-view-name-1>','<your-database-name-2>.<your-schema-name-2>.<your-base-table-or-view-name-2>']" \
    --semantic_model_name "<a-meaningful-semantic-model-name>" \
    --snowflake_account="<your-snowflake-account>"
```

You may generate a semantic model for a given list of fully qualified tables following the `{database}.{schema}.{table}`
format. Each table in this list should be a physical table or a view present in your database.

All generated semantic models by default are saved either under `semantic_model_generator/output_models` if running from
the root of this project or the current directory you're in.


### Validation
You may also use the CLI tool to validate one of your semantic models. From inside your Poetry shell, run

```bash
python -m semantic_model_generator.validate_model \
    --yaml_path="/path/to/your/model_yaml.yaml \
    --snowflake_account="<your-account-name>"
```

## Python

You may also create/validate your semantic models from directly within your Python code. First, ensure that you have installed the Python package. Note, the version below should be the latest version under the `dist/` directory.

```bash
pip install dist/*.whl
```

### Generation

```python
from semantic_model_generator.generate_model import generate_base_semantic_model_from_snowflake

BASE_TABLES = ['<your-database-name-1>.<your-schema-name-1>.<your-base-table-or-view-name-1>',
               '<your-database-name-2>.<your-schema-name-2>.<your-base-table-or-view-name-2>']
SNOWFLAKE_ACCOUNT = "<your-snowflake-account>"
SEMANTIC_MODEL_NAME = "<a-meaningful-semantic-model-name>"

generate_base_semantic_model_from_snowflake(
    base_tables=BASE_TABLES,
    snowflake_account=SNOWFLAKE_ACCOUNT,
    semantic_model_name=SEMANTIC_MODEL_NAME,
)
```

### Validation

```python
from semantic_model_generator.validate_model import validate_from_local_path

YAML_PATH = "/path/to/your/model_yaml.yaml"
SNOWFLAKE_ACCOUNT = "<your-snowflake-account>"

validate_from_local_path(
    yaml_path=YAML_PATH,
    snowflake_account=SNOWFLAKE_ACCOUNT
)

```


## Usage

### Semantic Model Context Length Constraints
Due to context window as well as quality constraints, we currently limit the size of the generated semantic model to <30,980 tokens (~123,920 characters).

Please note sample values and verified queries is not counted into this token length constraints. You can include as much sample values or verified queries as you'd like with limiting the overall file to <1MB.

### Auto-Generated Descriptions

If your snowflake tables and comments do not have comments, we currently
leverages [cortex LLM function](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions) to
auto-generate description suggestions. Those generation are suffixed with '__' and additional comment to remind you to
confirm/modity the descriptions.

### Additional Fields to Fill Out

**IMPORTANT**: After generation, your YAML files will have a series of lines with `# <FILL-OUT>`. Please take the time
to fill these out with your business context, or else subsequent validation of your model will fail.

By default, the generated semantic model will contain all columns from the provided tables/views. However, it's highly
encouraged to only keep relevant columns and drop any unwanted columns from the generated semantic model.

In addition, consider adding the following elements to your semantic model:

1. Logical columns for a given table/view that are expressions over physical columns.
    * Example: `col1 - col2` could be the `expr` for a logical column.
2. Synonyms. Any additional synonyms for column names.
3. Filters. Additional filters with their relevant `expr`.

### Partner Semantic Support

We continue to add support for partner semantic and metric layers. Our aim is to expedite the creation of Cortex Analyst semantic files using logic and metadata from partner tools.
Please see below for details about current partner support. 

**IMPORTANT**: Use the [Streamlit App](#streamlit-app) to leverage existing partner semantic/metric layers. 

| Tool     | Method  | Requirements  |
| -------- | ------- | ------- |
| DBT      | We extract and translate metadata from [semantic_models](https://docs.getdbt.com/docs/build/semantic-models#semantic-models-components) in uploaded DBT yaml file(s) and merge with a generated Cortex Analyst semantic file table-by-table.    |  DBT models and sources leading up to the semantic model layer(s) must be tables/views in Snowflake.   |
| Looker   |We materialize your Explore dataset in Looker as Snowflake table(s) and generate a Cortex Analyst semantic file. Metadata from your Explore fields can be merged with the generated Cortex Analyst semantic file. |  Looker Views referenced in the Looker Explores must be tables/views in Snowflake. Looker SDK credentials are required. Visit [Looker Authentication SDK Docs](https://cloud.google.com/looker/docs/api-auth#authentication_with_an_sdk) for more information. Install Looker's [API Explorer extension](https://cloud.google.com/looker/docs/api-explorer) from the Looker Marketplace to view API credentials directly.  |



## Examples

If you have an example table in your account with the following DDL statements.

```sql
CREATE TABLE sales.public.sd_data
(
    id    SERIAL PRIMARY KEY,
    dt    DATETIME,
    cat   VARCHAR(255),
    loc   VARCHAR(255),
    cntry VARCHAR(255),
    chn   VARCHAR(50),
    amt   DECIMAL(10, 2),
    unts  INT,
    cst   DECIMAL(10, 2)
);
```

Here is an example semantic model, with data elements automatically generated from this repo and filled out by a user.

```yaml
# Name and description of the semantic model.
name: Sales Data
description: This semantic model can be used for asking questions over the sales data.

# A semantic model can contain one or more tables.
tables:

  # A logical table on top of the 'sd_data' base table.
  - name: sales_data
    description: A logical table capturing daily sales information across different store locations and product categories.

    # The fully qualified name of the base table.
    base_table:
      database: sales
      schema: public
      table: sd_data

    # Dimension columns in the logical table.
    dimensions:
      - name: product_category
        synonyms:
          - "item_category"
          - "product_type"
        description: The category of the product sold.
        expr: cat
        data_type: NUMBER
        unique: false
        sample_values:
          - "501"
          - "544"

      - name: store_country
        description: The country where the sale took place.
        expr: cntry
        data_type: TEXT
        unique: false
        sample_values:
          - "USA"
          - "GBR"

      - name: sales_channel
        synonyms:
          - "channel"
          - "distribution_channel"
        description: The channel through which the sale was made.
        expr: chn
        data_type: TEXT
        unique: false
        sample_values:
          - "FB"
          - "GOOGLE"

    # Time dimension columns in the logical table.
    time_dimensions:
      - name: sale_timestamp
        synonyms:
          - "time_of_sale"
          - "transaction_time"
        description: The time when the sale occurred. In UTC.
        expr: dt
        data_type: TIMESTAMP
        unique: false

    # Measure columns in the logical table.
    measures:
      - name: sales_amount
        synonyms:
          - "revenue"
          - "total_sales"
        description: The total amount of money generated from the sale.
        expr: amt
        data_type: NUMBER
        default_aggregation: sum

      - name: sales_tax
        description: The sales tax paid for this sale.
        expr: amt * 0.0975
        data_type: NUMBER
        default_aggregation: sum

      - name: units_sold
        synonyms:
          - "quantity_sold"
          - "number_of_units"
        description: The number of units sold in the transaction.
        expr: unts
        data_type: NUMBER
        default_aggregation: sum

      - name: cost
        description: The cost of the product sold.
        expr: cst
        data_type: NUMBER
        default_aggregation: sum

      - name: profit
        synonyms:
          - "earnings"
          - "net income"
        description: The profit generated from a sale.
        expr: amt - cst
        data_type: NUMBER
        default_aggregation: sum

    # A table can define commonly used filters over it. These filters can then be referenced in user questions directly.
    filters:
      - name: north_america
        synonyms:
          - "North America"
          - "N.A."
          - "NA"
        description: "A filter to restrict only to north american countries"
        expr: cntry IN ('canada', 'mexico', 'usa')
```

## Release

In order to push a new build and release, follow the steps below. Note, only admins are allowed to push `release/v`
tags.

You should follow the setup commands from usage-cli to install poetry and create your environment.

1. Checkout a new branch from main. You should name this branch `release/vYYYY-MM-DD`.
2. Bump the poetry:
    * `poetry version patch` - increments `0.1.x` to `0.1.(x+1)`
    * `poetry version minor` - increments `0.x.0` to `0.(x+1).0`
    * `poetry version major` - increments `x.0.0` to `(x+1).0.0`
3. Update the `CHANGELOG.md` adding a relevant header for your version number along with a description of the changes
   made.
4. Run `make build` to create a new .whl file.
5. Push your files for approval.
6. After approval, run `make release` which will cut a new release and attach the .whl file.
7. Merge in your pr.

- Note: If you `make release` does not trigger the GH action. Please delete the tag and push again.
