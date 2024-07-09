import time

import streamlit as st
from shared_utils import environment_variables_exist, get_environment_variables

# In case the semantic model was built already,
# the next page should be available already.
st.session_state["next_is_unlocked"] = environment_variables_exist()

"""
Want to unlock the amazing potential of Copilot Analyst? You know what
it takes: a semantic model YAML file! Well, this app is exactly made
to help you create, edit, save and export your semantic model. Just
follow the navigation above!

Before we start, make sure you've properly setup your Snowflake connection
environment variables.
"""

if st.button("Check my environment variables"):
    if environment_variables_exist():
        st.success(
            "All environment variables are properly set. You can move on to the next section."
        )
        st.session_state["next_is_unlocked"] = True
        time.sleep(1)
        st.rerun()
    else:
        snowflake_env = get_environment_variables()
        missing_environment_variables: str = "\n\n- ".join(
            list({k for k, v in snowflake_env.items() if v is None})
        )
        st.error(
            f"The following environment variables are missing: \n- {missing_environment_variables}"
        )


# """
# #### How to use this app?
# The app will guide you through 5 steps:
# 1. **Store** to set the stage to save your semantic model first.
# 2. **Create** to either start the model by scratch (put in the name and description of your model);
#     Or upload an existing model to start editing.
# 3. **Edit** to add/edit/delete tables and dimensions, measures you want to include into the model.
#     Please see [Cortex Analyst semantic model specification documentation](https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/semantic-model-spec)
#     to learn more about semantic model specifications.
# 4. **Validate** to view your YAML and validate it.
# 5. **Upload** to finally upload your model to your stage!

# """
