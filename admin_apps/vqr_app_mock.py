from __future__ import annotations
import time
from typing import Literal
import pandas as pd
import streamlit as st
from dataclasses import dataclass

import streamlit as st
from streamlit_monaco import st_monaco

st.set_page_config(page_title="VQR Mock", page_icon="💬")

""" # VQR Demo app """
AVATARS = {"assistant": "📊", "user": "👤"}
DEFAULT_ASSISTANT_MESSAGE = """
Hey! I'm Cortex Analyst. Ask me a question and I'll show you a
query and a resulting dataframe! This app is useful for you to
find relevant queries to your semantic model.
"""

RANDOM_SQL_QUERY = """
SELECT u.user_id, u.username, p.post_title, COUNT(c.comment_id) AS comment_count
FROM users u
JOIN posts p ON u.user_id = p.user_id
LEFT JOIN comments c ON p.post_id = c.post_id
WHERE u.account_status = 'active'
GROUP BY u.user_id, u.username, p.post_title
HAVING COUNT(c.comment_id) > 10
ORDER BY comment_count DESC;
"""


@dataclass
class Message:
    role: Literal["user", "assistant"]
    content: str
    sql: str | None = None
    dataframe: pd.DataFrame | None = None
    edited: bool = False

    @property
    def avatar(self):
        return AVATARS[self.role]

    def show(self):
        st.markdown(self.content)

        if self.sql:
            code = self.sql
            if self.edited:
                code += "\n-- Edited!"
            st.code(code, language="sql")

        if self.dataframe is not None:
            st.dataframe(self.dataframe, use_container_width=True)


@st.experimental_dialog("Edit", width="large")
def edit(message: Message) -> Message | None:
    """You can use this interface to improve the generated
    SQL query from Cortex Analyst and submit it as a validated
    query response instead."""

    with st.form(key="sql-editor", border=False):
        st.caption("**SQL**")
        with st.container(border=True):
            new_sql = st_monaco(
                value=message.sql.strip(),
                language="sql",
            )
            run = st.form_submit_button("Run", use_container_width=True)

    new_dataframe = message.dataframe

    if run:
        with st.spinner("Mocking computations"):
            time.sleep(1)

    st.caption("**Output data**")
    new_dataframe = message.dataframe  # TODO: API response
    st.dataframe(new_dataframe, use_container_width=True)

    if st.button("Save", use_container_width=True, type="primary"):
        # Edit message in history
        message.sql = new_sql
        message.dataframe = new_dataframe
        message.edited = True
        st.session_state.messages[-1] = message

        # Save VQR
        st.toast("Saved successfully")
        st.rerun()

with st.sidebar:
    st.markdown(":gray[**Settings**]")
    """Stage: ..."""
    """Model: ..."""
    """..."""

# Store LLM-generated responses
if "messages" not in st.session_state.keys():
    st.session_state.messages = [
        Message(role="assistant", content=DEFAULT_ASSISTANT_MESSAGE)
    ]

# Display or clear chat messages
for message in st.session_state.messages:
    with st.chat_message(message.role, avatar=message.avatar):
        message.show()


# User-provided prompt
if prompt := st.chat_input():
    user_message = Message(role="user", content=prompt)
    st.session_state.messages.append(Message(role="user", content=prompt))
    with st.chat_message("user", avatar=user_message.avatar):
        st.write(prompt)

# Generate a new assistant response if last message is from user
if st.session_state.messages[-1].role == "user":
    with st.chat_message("assistant", avatar="📊"):
        with st.spinner("Querying the Cortex API..."):
            time.sleep(0.5)
            response = prompt[::-1]
            sql = RANDOM_SQL_QUERY
            dataframe = pd.DataFrame({"foo": [1, 2, 3], "bar": ["cat", "dog", "duck"]})
            message = Message(
                role="assistant", content=response, sql=sql, dataframe=dataframe
            )

        message.show()

    st.session_state.messages.append(message)


if st.session_state.messages[-1].sql:
    with st.container(border=False):
        st.caption("What do you think?")
        left, right, _ = st.columns((1, 1, 3))
        left.button(
            "💾  Save",
            use_container_width=True,
            on_click=st.toast,
            args=("Saved successfully",),
        )
        if right.button("🖊️  Edit", use_container_width=True):
            edit(message)
