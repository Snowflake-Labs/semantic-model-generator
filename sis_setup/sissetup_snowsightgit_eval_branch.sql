SET (streamlit_warehouse)=(SELECT CURRENT_WAREHOUSE());

CREATE DATABASE IF NOT EXISTS CORTEX_ANALYST_SEMANTICS_EVAL;
USE DATABASE CORTEX_ANALYST_SEMANTICS_EVAL;

-- Create API Integration for Git
CREATE OR REPLACE API INTEGRATION git_api_integration_snowflake_labs
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs')
  ENABLED = TRUE;

-- Create Git Repository
CREATE OR REPLACE GIT REPOSITORY git_snowflake_semantic_model_generator
  API_INTEGRATION = git_api_integration_snowflake_labs
  ORIGIN = 'https://github.com/Snowflake-Labs/semantic-model-generator.git';

ALTER GIT REPOSITORY git_snowflake_semantic_model_generator FETCH;

-- Create Schema to host streamlit app
CREATE SCHEMA IF NOT EXISTS CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR
COMMENT = '{"origin": "sf_sit",
            "name": "skimantics",
            "version": {"major": 2, "minor": 0},
            "attributes": {"deployment": "sis"}}';


-- Create stage for App logic and 3rd party packages
CREATE OR REPLACE STAGE CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE
DIRECTORY = (ENABLE = true)
COMMENT = '{"origin": "sf_sit",
            "name": "skimantics",
            "version": {"major": 2, "minor": 0},
            "attributes": {"deployment": "sis"}}';

-- Copy Files from Git Repository into App Stage
COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE
  FROM @CORTEX_ANALYST_SEMANTICS_EVAL.PUBLIC.git_snowflake_semantic_model_generator/branches/customer-eval/app_utils/
  PATTERN='.*[.]zip';

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/
  FROM @CORTEX_ANALYST_SEMANTICS_EVAL.PUBLIC.git_snowflake_semantic_model_generator/branches/customer-eval/
  FILES = ('environment.yml', 'app.py');

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/
  FROM @CORTEX_ANALYST_SEMANTICS_EVAL.PUBLIC.git_snowflake_semantic_model_generator/branches/customer-eval/semantic_model_generator/
  PATTERN='.*[.]py';
  
RM @CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/tests;
RM @CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/output_models;

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/images/
  FROM @CORTEX_ANALYST_SEMANTICS_EVAL.PUBLIC.git_snowflake_semantic_model_generator/branches/customer-eval/images/
  PATTERN='.*[.]png';

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/journeys/
  FROM @CORTEX_ANALYST_SEMANTICS_EVAL.PUBLIC.git_snowflake_semantic_model_generator/branches/customer-eval/journeys/
  PATTERN='.*[.]py';

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/partner/
  FROM @CORTEX_ANALYST_SEMANTICS_EVAL.PUBLIC.git_snowflake_semantic_model_generator/branches/customer-eval/partner/
  PATTERN='.*[.]py';

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/app_utils/
  FROM @CORTEX_ANALYST_SEMANTICS_EVAL.PUBLIC.git_snowflake_semantic_model_generator/branches/customer-eval/app_utils/
  PATTERN='.*[.]py';

-- Create Streamlit App
CREATE OR REPLACE STREAMLIT CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.SEMANTIC_MODEL_GENERATOR
ROOT_LOCATION = '@CORTEX_ANALYST_SEMANTICS_EVAL.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator'
MAIN_FILE = 'app.py'
TITLE = "Semantic Model Generator (with Eval)"
IMPORTS = ('@cortex_analyst_semantics_eval.semantic_model_generator.streamlit_stage/looker_sdk.zip',
'@cortex_analyst_semantics_eval.semantic_model_generator.streamlit_stage/strictyaml.zip')
QUERY_WAREHOUSE = $streamlit_warehouse
COMMENT = '{"origin": "sf_sit",
            "name": "skimantics",
            "version": {"major": 2, "minor": 0},
            "attributes": {"deployment": "sis"}}';
