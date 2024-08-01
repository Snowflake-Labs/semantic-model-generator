from typing import Optional
from snowflake.connector import SnowflakeConnection
from snowflake.snowpark.exceptions import SnowparkSQLException


revise_semantic_prompt = """You are a data analyst tasked with revising a semantic file for your enterprise.
You will receive an initial shell of a semantic file for Cortex Analyst and must update the semantic file using additional metadata files.
The generated Cortex Analyst semantic file MUST adhere to the following documentation:
<docs>
{docs}
</docs>
Follow the rules below.
<rules>
1. Generated descriptions should be concise.
2. Each tablename should correspond to a single logical table in the semantic file. Do not create multiple logical tables for a single tablename.
3. Do not make assumptions about filters. Table samples are not exhaustive of values.
</rules>
<initial_semantic_shell>
{initial_semantic_file}
</initial_semantic_shell>
<other_metadata_files>
{metadata_files}
</other_metadata_files>
Revised Semantic File: 
"""

def run_cortex_complete(conn: SnowflakeConnection,
                        model: str,
                        prompt: str,
                        prompt_args: Optional[dict] = None) -> str:
    
    if prompt_args:
        prompt = prompt.format(**prompt_args).replace("'", "\\'")
    complete_sql = f"SELECT snowflake.cortex.complete('{model}', '{prompt}')"
    response = conn.cursor().execute(complete_sql).fetchone()[0]

    return response

def format_metadata_files(metadata_files: dict) -> str:
    metadata_str = ""
    for fname, metadata in metadata_files.items():
        metadata_str += f"Filename: {metadata['filename']}\n"
        metadata_str += f"Platform: {metadata['platform']}\n"
        metadata_str += f"Contents: {metadata['contents']}\n"
    return metadata_str


def get_cortex_analyst_docs(webpage: str = "https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/semantic-model-spec") -> str:
    # TODO: Slice the webpage to get the relevant documentation 
    import requests
    from bs4 import BeautifulSoup

    r = requests.get(webpage)
    soup = BeautifulSoup(r.text, "html.parser")
    article = soup.find("article")
    sections = article.find_all('section')
    docs = ""
    for section in sections:
         section_id = section.get('id')
         for copybutton in section.find_all(class_='copybutton'):
            copybutton.decompose()
         if section_id in ['key-concepts',
                           'tips-for-creating-a-semantic-model',
                           'specification',
                           'example-yaml']:
            section_text = section.get_text(strip=False)
            docs += f"{section_text}\n"
            docs = docs.replace("\n\n", "\n")
    return docs

def refine_with_other_metadata(conn: SnowflakeConnection,
                               model: str = 'mistral-large',
                               prompt: str = revise_semantic_prompt,
                               prompt_args: Optional[dict] = None) -> str:
    
    error = '' # Used as a flag to enable builder workflow to continue with prior state
    try:
        response = run_cortex_complete(conn, model=model, prompt = prompt, prompt_args=prompt_args)
        return response, error
    except Exception as e:
        error = f'Error encountered: {str(e)}'
        return '', error                                       