CREATE OR REPLACE PROCEDURE CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.ZIP_SRC_FILES(
    SOURCE_STAGE_PATH STRING,
    TARGET_STAGE_PATH STRING
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER='zip_src_files'
EXECUTE AS CALLER
AS $$
from snowflake.snowpark import Session

def zip_src_files(session: Session,
                   SOURCE_STAGE_PATH: str,
                   TARGET_STAGE_PATH: str) -> str:
    return 'hello, world'
$$;

CREATE OR REPLACE PROCEDURE CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.GENERATE_SEMANTIC_FILE(
    STAGE_NAME STRING,
    MODEL_FILENAME STRING,
    SAMPLE_VALUE INT,
    ALLOW_JOINS BOOLEAN,
    TABLE_LIST ARRAY
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = (
    'pandas==2.2.2',
    'tqdm==4.66.5',
    'loguru==0.5.3',
    'protobuf==3.20.3',
    'pydantic==2.8.2',
    'pyyaml==6.0.1',
    'ruamel.yaml==0.17.21',
    'pyarrow==14.0.2',
    'sqlglot==25.10.0',
    'numpy==1.26.4',
    'python-dotenv==0.21.0',
    'urllib3==2.2.2',
    'types-pyyaml==6.0.12.12',
    'types-protobuf==4.25.0.20240417',
    'snowflake-snowpark-python==1.18.0',
    'cattrs==23.1.2'
)
IMPORTS = ('@CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator.zip',
    '@cortex_analyst_semantics.semantic_model_generator.streamlit_stage/strictyaml.zip'
            )
HANDLER='run_generation'
EXECUTE AS CALLER
AS $$
from snowflake.snowpark import Session

def run_generation(session: Session,
                   STAGE_NAME: str,
                   MODEL_NAME: str,
                   SAMPLE_VALUE: int,
                   ALLOW_JOINS: bool,
                   TABLE_LIST: list[str]) -> str:
    from semantic_model_generator.generate_model import generate_model_str_from_snowflake

    if not MODEL_NAME:
        raise ValueError("Please provide a name for your semantic model.")
    elif not TABLE_LIST:
        raise ValueError("Please select at least one table to proceed.")
    else:
        yaml_str = generate_model_str_from_snowflake(
            base_tables=TABLE_LIST,
            semantic_model_name=MODEL_NAME,
            n_sample_values=SAMPLE_VALUE,  # type: ignore
            conn=session.connection,
            allow_joins=ALLOW_JOINS,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            tmp_file_path = os.path.join(temp_dir, f"{MODEL_NAME}.yaml")

            with open(tmp_file_path, "w", encoding='utf-8') as temp_file:
                temp_file.write(yaml)

            st.session_state.session.file.put(
                tmp_file_path,
                f"@{STAGE_NAME}",
                auto_compress=False,
                overwrite=True,
            )
$$;

-- Test
-- CALL CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.GENERATE_SEMANTIC_FILE(
--     'CATRANSLATOR.ANALYTICS.DATA',
--     'sproctest',
--     3,
--     False,
--     ['CATRANSLATOR.ANALYTICS.CUSTOMERS']
-- );