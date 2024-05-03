# semantic-model-generator

The `Semantic Model Generator` is used to generate a semantic model for use in your Snowflake account.

Your workflow should be:
1. [Setup](#setup) to set credentials.
2. [Usage](#usage) to create a model either through Python or command line.
3. [Post Generation](#post-generation) to fill out the rest of your semantic model.
4. [Validating Your Final Semantic Model](#validating-yaml-updates) to ensure any changes you've made are valid.

Or, if you want to see what a semantic model looks like, skip to [Examples](#examples).

## Setup

We currently leverage credentials saved as environment variables. Note, `host` is optional depending on your Snowflake deployment.

A. To find your Account Name, [follow these instructions](https://docs.snowflake.com/en/sql-reference/functions/current_account_name), or execute the following sql command in your account.

```sql
SELECT CURRENT_ACCOUNT_NAME();
```

B. To find the host for your account, [follow these instructions](https://docs.snowflake.com/en/user-guide/organizations-connect#connecting-with-a-url).
* Typically, the account name follows the following format: `https://<orgname>-<account_name>.snowflakecomputing.com`


1. To set these on Mac OS/Linux: 
```bash
export SNOWFLAKE_ROLE="<your-snowflake-role>"
export SNOWFLAKE_WAREHOUSE="<your-snowflake-warehouse>"
export SNOWFLAKE_USER="<your-snowflake-user>"
export SNOWFLAKE_PASSWORD="<your-snowflake-password>"
export SNOWFLAKE_HOST="<your-snowflake-host>"
```

2. To set these on Windows:
```bash
set SNOWFLAKE_ROLE=<your-snowflake-role>
set SNOWFLAKE_WAREHOUSE=<your-snowflake-warehouse>
set SNOWFLAKE_USER=<your-snowflake-user>
set SNOWFLAKE_PASSWORD=<your-snowflake-password>
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
os.environ['SNOWFLAKE_HOST'] = '<your-snowflake-host>'
```
## Usage

You may generate a semantic model for a given list of fully qualified tables following the `{database}.{schema}.{table}` format. Each table in this list should be a physical table or a view present in your database.

All generated semantic models by default are saved either under `semantic_model_generator/output_models` if running from the root of this project or the current directory you're in.

### Generation - Python

1. Ensure you have installed the Python package. Note, the version below should be the latest version under the `dist/` directory.
```bash
pip install dist/semantic_model_generator-0.1.11-py3-none-any.whl
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
    semantic_model_name=SEMANTIC_MODEL_NAME
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

**Important**: After generation, your YAML files will have a series of lines with `# <FILL-OUT>`. Please take the time to fill these out with your business context. 

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
from semantic_model_generator.validate_model import validate

YAML_PATH="/path/to/your/model_yaml.yaml"
SNOWFLAKE_ACCOUNT="<your-snowflake-account>"

validate(
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
# Name of the Semantic Model.
name: Sales Data
description: This semantic model can be used for asking questions over the sales data.

# A semantic model can contain one or more tables.
tables:

  # Table 1: A logical table over the 'sd_data' physical table.
  - name: sales_data

    # A description of the logical table.
    description: A logical table capturing daily sales information across different store locations and product categories.

    # The fully qualified name of the underlying physical table.
    base_table:
      database: sales
      schema: public
      table: sd_data

    dimensions:
      - name: product_category
        # Synonyms should be unique across the entire semantic model.
        synonyms: 
            - "item_category"
            - "product_type"
        description: The category of the product sold.
        expr: cat
        unique: false
        data_type: NUMBER
        sample_values:
          - '501'
          - '544'

      - name: store_country
        description: The country where the sale took place.
        expr: cntry
        unique: false
        data_type: TEXT
        sample_values:
          - 'USA'
          - 'GBR'

      - name: sales_channel
        synonyms: 
            - "channel"
            - "distribution_channel"
        description: The channel through which the sale was made.
        expr: chn
        unique: false
        data_type: TEXT
        sample_values:
          - 'FB'
          - 'GOOGLE'

    time_dimensions:
      - name: sale_timestamp
        synonyms: 
            - "time_of_sale"
            - "transaction_time"
        description: The time when the sale occurred. In UTC.
        expr: dt
        unique: false
        data_type: TIMESTAMP
        sample_values:
          - '2016-09-01 07:30:00'
          - '2016-09-01 14:16:00'
          - '2016-09-04 09:20:00'

    measures:
      - name: sales_amount
        synonyms: 
            - "revenue"
            - "total_sales"
        description: The total amount of money generated from the sale.
        expr: amt
        default_aggregation: sum
        data_type: NUMBER
        sample_values:
          - '11.650000'
          - '50.880000'

      - name: sales_tax
        description: The sales tax paid for this sale.
        expr: amt * 0.0975
        default_aggregation: sum
        data_type: NUMBER
        sample_values:
          - '51.650000'
          - '57.800'

      - name: units_sold
        synonyms: 
            - "quantity_sold"
            -  "number_of_units"
        description: The number of units sold in the transaction.
        expr: unts
        default_aggregation: sum
        data_type: NUMBER
        sample_values:
          - '1'
          - '3'

      - name: cost
        description: The cost of the product sold.
        expr: cst
        default_aggregation: sum
        data_type: NUMBER
        sample_values:
          - '10'
          - '33'

      - name: profit
        synonyms: 
            - "earnings"
            - "net income"
        description: The profit generated from a sale.
        expr: amt - cst
        default_aggregation: sum
        data_type: NUMBER
        sample_values:
          - '15'
          - '37'


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
    * `poetry version patch`
    * `poetry version minor`
    * `poetry version major`
3. Update the `CHANGELOG.md` adding a relevant header for your version number along with a description of the changes made.
4. Run `make build` to create a new .whl file. Update the package to install under [Python Generation](#generation-python).
5. Push your files for approval.
6. After approval, run `make release` which will cut a new release and attach the .whl file.
7. Merge in your pr.
