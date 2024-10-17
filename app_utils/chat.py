import json
import re
from typing import Dict, Any

import requests
import streamlit as st
from snowflake.connector import SnowflakeConnection

API_ENDPOINT = "https://{HOST}/api/v2/cortex/analyst/message"


@st.cache_data(ttl=60, show_spinner=False)
def send_message(
    _conn: SnowflakeConnection, semantic_model: str, messages: list[dict[str, str]]
) -> Dict[str, Any]:
    """
    Calls the REST API with a list of messages and returns the response.
    Args:
        _conn: SnowflakeConnection, used to grab the token for auth.
        messages: list of chat messages to pass to the Analyst API.
        semantic_model: stringified YAML of the semantic model.

    Returns: The raw ChatMessage response from Analyst.
    """
    request_body = {
        "messages": messages,
        "semantic_model": semantic_model,
    }

    if st.session_state["sis"]:
        import _snowflake

        resp = _snowflake.send_snow_api_request(  # type: ignore
            "POST",
            f"/api/v2/cortex/analyst/message",
            {},
            {},
            request_body,
            {},
            30000,
        )
        if resp.status_code < 400:
            json_resp: Dict[str, Any] = json.loads(resp["content"])
            return json_resp
        else:
            # Remove the link to the github repo from the error message
            error_msg = re.sub(
                r"\s*Please use https://github\.com/Snowflake-Labs/semantic-model-generator.*",
                "",
                resp["content"],
            )
            raise ValueError(error_msg)

    else:
        host = st.session_state.host_name
        resp = requests.post(
            API_ENDPOINT.format(
                HOST=host,
            ),
            json=request_body,
            headers={
                "Authorization": f'Snowflake Token="{_conn.rest.token}"',  # type: ignore[union-attr]
                "Content-Type": "application/json",
            },
        )
        if resp.status_code < 400:
            json_resp: Dict[str, Any] = resp.json()
            return json_resp
        else:
            err_body = json.loads(resp.text)
            # Remove the link to the github repo from the error message
            error_msg = re.sub(
                r"\s*Please use https://github\.com/Snowflake-Labs/semantic-model-generator.*",
                "",
                err_body["message"],
            )
            raise ValueError(error_msg)
