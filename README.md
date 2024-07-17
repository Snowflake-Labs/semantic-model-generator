# semantic-model-generator

The `Semantic Model Generator` is used to generate a semantic model for use in your Snowflake account.

Your workflow should be:
1. [Setup](#setup) to set credentials.
2. [Usage](#usage) to create a model either through Python or command line.
3. [Post Generation](#post-generation) to fill out the rest of your semantic model.
4. [Validating Your Final Semantic Model](#validating-yaml-updates) to ensure any changes you've made are valid.
5. [Admin App](#admin-app) spin up the admin app for easier generation of your semantic model.

Or, if you want to see what a semantic model looks like, skip to [Examples](#examples).

## Setup

We currently leverage credentials saved as environment variables. Note, `host` is optional depending on your Snowflake deployment.

A. To find your Account locator, please execute the following sql command in your account.

```sql
SELECT CURRENT_ACCOUNT_LOCATOR();
```

B. To find the `SNOWFLAKE_HOST` for your account, [follow these instructions](https://docs.snowflake.com/en/user-guide/organizations-connect#connecting-with-a-url).
* Currently we recommend you to look under the `Account locator (legacy)` method of connection for better compatibility on API.
* It typically follows format of: `https://<accountlocator>.<region>.<cloud>.snowflakecomputing.com`


1. To set these on Mac OS/Linux: 
```bash
export SNOWFLAKE_ROLE="<your-snowflake-role>"
export SNOWFLAKE_WAREHOUSE="<your-snowflake-warehouse>"
export SNOWFLAKE_USER="<your-snowflake-user>"
export SNOWFLAKE_PASSWORD="<your-snowflake-password>"
export SNOWFLAKE_ACCOUNT_LOCATOR="<your-snowflake-account-locator>"
export SNOWFLAKE_HOST="<your-snowflake-host>"
```

2. To set these on Windows:
```bash
set SNOWFLAKE_ROLE=<your-snowflake-role>
set SNOWFLAKE_WAREHOUSE=<your-snowflake-warehouse>
set SNOWFLAKE_USER=<your-snowflake-user>
set SNOWFLAKE_PASSWORD=<your-snowflake-password>
set SNOWFLAKE_ACCOUNT_LOCATOR=<your-snowflake-account-locator>
set SNOWFLAKE_HOST=<your-snowflake-host>
```

3. To set these within a Python environment:
```python
import os

# Setting environment variables
os.environ['SNOWFLAKE_ROLE'] = '<your-snowflake-role>'
os.environ['SNOWFLAKE_WAREHOUSE'] = '<your-snowflake-warehouse>'
os.environ['SNOWFLAKE_USER'] = '<your-snowflake-user>'
os.environ['SNOWFLAKE_PASSWORD'] = '<your-snowflake-password>'
os.environ['SNOWFLAKE_ACCOUNT_LOCATOR']='<your-snowflake-account-locator>'
os.environ['SNOWFLAKE_HOST'] = '<your-snowflake-host>'
```

4. If you are using single sign-on authetication, you can leave password empty and set additional environ below:
 ```python
os.environ['SNOWFLAKE_AUTHENTICATOR'] = 'EXTERNALBROWSER'
```

## Usage

You may generate a semantic model for a given list of fully qualified tables following the `{database}.{schema}.{table}` format. Each table in this list should be a physical table or a view present in your database.

If your snowflake tables and comments do not have comments, we currently leverages [cortex LLM function](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions) to auto-generate description suggestions. Those generation are suffixed with '__' and additional comment to remind you to confirm/modity the descriptions.

All generated semantic models by default are saved either under `semantic_model_generator/output_models` if running from the root of this project or the current directory you're in.

### Generation - Python

1. Ensure you have installed the Python package. Note, the version below should be the latest version under the `dist/` directory.
```bash
pip install dist/*.whl
```
2. Activate Python shell
```bash
python
```
3. Generate a semantic model.
```python
from semantic_model_generator.generate_model import generate_base_semantic_model_from_snowflake

BASE_TABLES = ['<your-database-name-1>.<your-schema-name-1>.<your-base-table-or-view-name-1>','<your-database-name-2>.<your-schema-name-2>.<your-base-table-or-view-name-2>']
SNOWFLAKE_ACCOUNT = "<your-snowflake-account>"
SEMANTIC_MODEL_NAME = "<a-meaningful-semantic-model-name>"

generate_base_semantic_model_from_snowflake(
    base_tables=BASE_TABLES,
    snowflake_account=SNOWFLAKE_ACCOUNT,
    semantic_model_name=SEMANTIC_MODEL_NAME,
)
```


### Generation - CLI
Unlike the Python route above, using the CLI assumes that you will manage your environment using `poetry` and `pyenv` for Python versions.
This has only been tested on Mas OS/Linux.

1. If you need brew, `make install-homebrew`.
2. If you need pyenv, `make install-pyenv` and `make install-python-3.8`.
3. `make setup` Make setup will install poetry if needed.


This is the script version run on the command line.
1. `poetry shell` . This will activate your virtual environment.
2. Run on your command line.
```bash
python -m semantic_model_generator.generate_model \
    --base_tables  "['<your-database-name-1>.<your-schema-name-1>.<your-base-table-or-view-name-1>','<your-database-name-2>.<your-schema-name-2>.<your-base-table-or-view-name-2>']" \
    --semantic_model_name "<a-meaningful-semantic-model-name>" \
    --snowflake_account="<your-snowflake-account>"
```

### Post-Generation

#### Additional Fields to Fill Out

**IMPORTANT**: After generation, your YAML files will have a series of lines with `# <FILL-OUT>`. Please take the time to fill these out with your business context, or else subsequent validation of your model will fail. 

By default, the generated semantic model will contain all columns from the provided tables/views. However, it's highly encouraged to only keep relevant columns and drop any unwanted columns from the generated semantic model.

In addition, consider adding the following elements to your semantic model:

1. Logical columns for a given table/view that are expressions over physical columns.
    * Example: `col1 - col2` could be the `expr` for a logical column.
2. Synonyms. Any additional synonyms for column names.
3. Filters. Additional filters with their relevant `expr`.

#### Validating Yaml Updates

After you've edited your semantic model, you can validate this file before uploading.

1. Using Python. Ensure you've installed the package.

```python
from semantic_model_generator.validate_model import validate_from_local_path

YAML_PATH="/path/to/your/model_yaml.yaml"
SNOWFLAKE_ACCOUNT="<your-snowflake-account>"

validate_from_local_path(
    yaml_path=YAML_PATH,
    snowflake_account=SNOWFLAKE_ACCOUNT
)

```

2. Using the command line. Ensure `poetry shell` is activated.

```bash
python -m semantic_model_generator.validate_model \
    --yaml_path="/path/to/your/model_yaml.yaml \
    --snowflake_account="<your-account-name>"
```



## admin-app

To install the admin app dependencies, please run 
```bash
make setup_admin_app
```

from the root repo directory. This will install dependencies from the base wheel as well as admin-app specific dependencies.

## Examples

If you have an example table in your account with the following DDL statements.

```sql
CREATE TABLE sales.public.sd_data (
    id SERIAL PRIMARY KEY,
    dt DATETIME,
    cat VARCHAR(255),
    loc VARCHAR(255),
    cntry VARCHAR(255)
    chn VARCHAR(50),
    amt DECIMAL(10, 2),
    unts INT,
    cst DECIMAL(10, 2)
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

In order to push a new build and release, follow the steps below. Note, only admins are allowed to push `release/v` tags.

You should follow the setup commands from usage-cli to install poetry and create your environment.

1. Checkout a new branch from main. You should name this branch `release/vYYYY-MM-DD`.
2. Bump the poetry:
    * `poetry version patch` - increments `0.1.x` to `0.1.(x+1)`
    * `poetry version minor` - increments `0.x.0` to `0.(x+1).0`
    * `poetry version major` - increments `x.0.0` to `(x+1).0.0`
3. Update the `CHANGELOG.md` adding a relevant header for your version number along with a description of the changes made.
4. Run `make build` to create a new .whl file.
5. Push your files for approval.
6. After approval, run `make release` which will cut a new release and attach the .whl file.
7. Merge in your pr.

- Note: If you `make release` does not trigger the GH action. Please delete the tag and push again.
