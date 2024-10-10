CREATE DATABASE IF NOT EXISTS CORTEX_ANALYST_SEMANTICS
COMMENT = '{"origin": "sf_sit",
            "name": "skimantics",
            "version": {"major": 2, "minor": 0},
            "attributes": {"deployment": "sis"}}';

CREATE SCHEMA IF NOT EXISTS CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR
COMMENT = '{"origin": "sf_sit",
            "name": "skimantics",
            "version": {"major": 2, "minor": 0},
            "attributes": {"deployment": "sis"}}';

-- Create stage for App logic and 3rd party packages
CREATE OR REPLACE STAGE CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE
DIRECTORY = (ENABLE = true)
COMMENT = '{"origin": "sf_sit",
            "name": "skimantics",
            "version": {"major": 2, "minor": 0},
            "attributes": {"deployment": "sis"}}';

-- Upload 3rd party packages
-- Run from sis_setup/ as paths are relative to this directory
PUT file://app_utils/*.zip @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE OVERWRITE = TRUE AUTO_COMPRESS = FALSE;

-- Upload App logic
PUT file://app.py @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://environment.yml @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://semantic_model_generator/*.py @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://semantic_model_generator/data_processing/*.py @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/data_processing/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://semantic_model_generator/protos/*.p* @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/protos/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://semantic_model_generator/snowflake_utils/*.py @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/snowflake_utils/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://semantic_model_generator/validate/*.py @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/validate/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://images/*.png @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/images/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://journeys/*.py @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/journeys/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://partner/*.py @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/partner/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://app_utils/*.py @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/app_utils/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;

-- Create Streamlit
CREATE OR REPLACE STREAMLIT CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.SEMANTIC_MODEL_GENERATOR
ROOT_LOCATION = '@CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator'
MAIN_FILE = 'app.py'
TITLE = "Semantic Model Generator"
IMPORTS = ('@cortex_analyst_semantics.semantic_model_generator.streamlit_stage/looker_sdk.zip',
'@cortex_analyst_semantics.semantic_model_generator.streamlit_stage/strictyaml.zip')
QUERY_WAREHOUSE = <% warehouse %>
COMMENT = '{"origin": "sf_sit",
            "name": "skimantics",
            "version": {"major": 2, "minor": 0},
            "attributes": {"deployment": "sis"}}';