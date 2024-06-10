This app combines semantic model generation, validation, and verified query workflow and aiming to provide jump start to `cortex analyst` users.


TODO:
1. Add inline validation of fields and forms.  E.g. if 'SQL Expression' is a required field
   for adding a dimension then prompt about the missing field in the UI itself.
2. Handle error cases in 'Add Table' workflow.
3. Move the stage requirement to only apply to upload tab, when API endpoint allows passing in the semantic context string, instead of the stage path.
4. Add an option to specify connection parameters in the app, instead of env vars.
5. Move streamlit version to 1.36.0 when available.

Known issues:
1. Sometimes the 'Show YAML' and 'Add Table' buttons don't respond to clicks after the user
   has imported an existing model.
2. The semantic model name doesn't update in the headline immediately after user enters it.
