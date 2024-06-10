import streamlit as st

st.markdown(
    """
Want to unlock the amazing potential of Copilot Analyst? You know what
it takes: a semantic model YAML file! Well, this app is exactly made
to help you create, edit, test, and export your semantic model!

Please make sure you've setup your environment variables for snowflake connection before spinning up the app.

How to use this app?
* In `Store` tab, set the stage to save your semantic model first.
* In `Create` tab, you can either start the model by scratch (put in the name and description of your model);
    Or upload an existing model to start editing.
* In `Edit` tab, you can add/edit/delete tables and dimensions, measures you want to include into the model.
    Please see [Cortex Analyst semantic model specification documentation](https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/semantic-model-spec)
    to learn more about semantic model specifications.
* When you finished editing your model, please go to `Validate` tab to view your yaml and validate.
* Once successfully validated, you can either `Upload` it directly, or go to the `Chat` tab to ask frequently asked questions to test on the model,
    and add verified queries to the model.


We'd love to get your feedback. Please reach out at [foobar]
"""
)
