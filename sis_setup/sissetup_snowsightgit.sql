SET (streamlit_warehouse)=(SELECT CURRENT_WAREHOUSE());

CREATE DATABASE IF NOT EXISTS CORTEX_ANALYST_SEMANTICS;
USE DATABASE CORTEX_ANALYST_SEMANTICS;

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

-- Copy Files from Git Repository into App Stage
COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE
  FROM @CORTEX_ANALYST_SEMANTICS.PUBLIC.git_snowflake_semantic_model_generator/branches/main/app_utils/
  PATTERN='.*[.]zip';

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/
  FROM @CORTEX_ANALYST_SEMANTICS.PUBLIC.git_snowflake_semantic_model_generator/branches/main/
  FILES = ('environment.yml', 'app.py');

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/
  FROM @CORTEX_ANALYST_SEMANTICS.PUBLIC.git_snowflake_semantic_model_generator/branches/main/semantic_model_generator/
  PATTERN='.*[.]py';
  
RM @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/tests;
RM @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/semantic_model_generator/output_models;

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/images/
  FROM @CORTEX_ANALYST_SEMANTICS.PUBLIC.git_snowflake_semantic_model_generator/branches/main/images/
  PATTERN='.*[.]png';

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/journeys/
  FROM @CORTEX_ANALYST_SEMANTICS.PUBLIC.git_snowflake_semantic_model_generator/branches/main/journeys/
  PATTERN='.*[.]py';

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/partner/
  FROM @CORTEX_ANALYST_SEMANTICS.PUBLIC.git_snowflake_semantic_model_generator/branches/main/partner/
  PATTERN='.*[.]py';

COPY FILES
  INTO @CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator/app_utils/
  FROM @CORTEX_ANALYST_SEMANTICS.PUBLIC.git_snowflake_semantic_model_generator/branches/main/app_utils/
  PATTERN='.*[.]py';

-- Create Streamlit App
CREATE OR REPLACE STREAMLIT CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.SEMANTIC_MODEL_GENERATOR
ROOT_LOCATION = '@CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator'
MAIN_FILE = 'app.py'
TITLE = "Semantic Model Generator"
IMPORTS = ('@cortex_analyst_semantics.semantic_model_generator.streamlit_stage/looker_sdk.zip',
'@cortex_analyst_semantics.semantic_model_generator.streamlit_stage/strictyaml.zip')
QUERY_WAREHOUSE = $streamlit_warehouse
COMMENT = '{"origin": "sf_sit",
            "name": "skimantics",
            "version": {"major": 2, "minor": 0},
            "attributes": {"deployment": "sis"}}';


-- Create Semantic Model Generation Callable
-- Zip src files for callable SPROC for generation
CREATE OR REPLACE PROCEDURE CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.ZIP_SRC_FILES(
    database STRING,
    schema STRING,
    stage STRING,
    source_path STRING,
    target_parent STRING,
    zip_filename STRING
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = (
    'snowflake-snowpark-python==1.18.0'
)
HANDLER='zip_staged_files'
EXECUTE AS CALLER
AS $$
from snowflake.snowpark import Session
from typing import Optional

def get_staged_files(session: Session,
                     database: str,
                     schema: str,
                     stage: str,
                     target_parent: Optional[str] = None,
                     source_path: Optional[str] = None,
                     ) -> dict[str, str]:
    
    query = f"ls @{database}.{schema}.{stage}/{source_path}"
    file_result = session.sql(query).collect()

    file_data = {}
    for row in file_result:
        filename = row['name'].split('/',1)[1] # Remove the stage name from the filename

        # If target_parent is provided, replace the original file pathing with it
        if target_parent:
            filename = filename.replace(source_path, f"{target_parent}")

        full_file_path = f"@{database}.{schema}.{row['name']}"
        file_data[filename] = session.file.get_stream(f"{full_file_path}").read().decode('utf-8')

    return file_data

def create_zip(file_data: dict[str, str]) -> bytes:
    import io
    import zipfile

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED
                         ) as zipf:
        for filename, content in file_data.items():
            zipf.writestr(filename, content)

    zip_bytes = zip_buffer.getvalue()

    return zip_bytes

def upload_zip(session: Session,
               database: str,
               schema: str,
               stage: str,
               zip_file: bytes,
               zip_filename: str,
               ) -> None:
    import io

    session.file.put_stream(
                io.BytesIO(zip_file),
                f"@{database}.{schema}.{stage}/{zip_filename.replace('zip','')}.zip",
                auto_compress=False,
                overwrite=True,
            )
    
def zip_staged_files(session: Session,
                     database: str,
                     schema: str,
                     stage: str,
                     source_path: Optional[str] = None,
                     target_parent: Optional[str] = None,
                     zip_filename: Optional[str] = None,
                     ) -> str:
    
    file_data = get_staged_files(session, database, schema, stage, target_parent, source_path)
    zip_file = create_zip(file_data)

    if zip_filename:
        zip_filename = zip_filename
    elif target_parent is not None:
        zip_filename = target_parent
    elif source_path is not None:
        zip_filename = source_path
    else:
        zip_filename = "zipped_files"

    upload_zip(session, database, schema, stage, zip_file, zip_filename)

    return f"Files zipped and uploaded to {database}.{schema}.{stage}/{zip_filename}.zip."

$$;

CALL CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.ZIP_SRC_FILES(
    'CORTEX_ANALYST_SEMANTICS',
    'SEMANTIC_MODEL_GENERATOR',
    'streamlit_stage',
    'semantic_model_generator/semantic_model_generator',
    'semantic_model_generator',
    'semantic_model_generator'
);

-- Create generation callable
CREATE OR REPLACE PROCEDURE CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.GENERATE_SEMANTIC_FILE(
    STAGE_NAME STRING,
    MODEL_NAME STRING,
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
    'cattrs==23.1.2',
    'filelock'
)
IMPORTS = ('@CORTEX_ANALYST_SEMANTICS.SEMANTIC_MODEL_GENERATOR.STREAMLIT_STAGE/semantic_model_generator.zip',
    '@cortex_analyst_semantics.semantic_model_generator.streamlit_stage/strictyaml.zip'
            )
HANDLER='run_generation'
EXECUTE AS CALLER
AS $$
from snowflake.snowpark import Session

def import_src_zip(zip_name = 'semantic_model_generator.zip'):
    """Unpacks source zip file in stage to enable importing it to mirror source code structure."""

    import os
    import sys
    import zipfile
    from filelock import FileLock

    # Get the location of the import directory. Snowflake sets the import
    # directory location so code can retrieve the location via sys._xoptions.
    IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
    import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

    # Get the path to the ZIP file and set the location to extract to.
    zip_file_path = import_dir + zip_name
    extracted = f'/tmp/{zip_name.replace(".zip", "")}'

    # Extract the contents of the ZIP. This is done under the file lock
    # to ensure that only one worker process unzips the contents.
    with FileLock('/tmp/extract.lock'):
        if not os.path.isdir(extracted):
            with zipfile.ZipFile(zip_file_path, 'r') as myzip:
                myzip.extractall(extracted)

    # Add in front in case there are conflicting module names including original zipped file
    sys.path.insert(0,extracted)

def run_generation(session: Session,
                   STAGE_NAME: str,
                   MODEL_NAME: str,
                   SAMPLE_VALUE: int,
                   ALLOW_JOINS: bool,
                   TABLE_LIST: list[str]) -> str:

    import io

    import_src_zip()
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

        session.file.put_stream(
                io.BytesIO(yaml_str.encode('utf-8')),
               f"@{STAGE_NAME}/{MODEL_NAME}.yaml",
               auto_compress=False,
               overwrite=True,
           )
        return f"Semantic model file {MODEL_NAME}.yaml has been generated and saved to {STAGE_NAME}."
$$;