# Admin apps

This folder contains a Streamlit app designed that is designed to help you manage your semantic models for the Cortex Analyst feature.

<table border="0">
   <tr>
     <td><img src="https://github.com/Snowflake-Labs/semantic-model-generator/assets/103658138/2b5fd664-9a9a-46ed-afbf-cb7638f0ad19" width="700"></td>
     <td><img src="https://github.com/Snowflake-Labs/semantic-model-generator/assets/103658138/d96a4255-9e82-41ba-8a82-dcb87353b667" width="500"></td>
  </tr>
  <tr>
      <td><strong>Chat app</strong> · so you can iterate on your semantic model by trying it live in a chat UI!</td>
      <td><strong>(coming soon) Builder app</strong> · so you can build your semantic model and edit tables, measures and dimensions from a UI</td>
   </tr>
<table>

## Get started

1. Make sure you've installed the requirements from the `pyproject.toml` file in the root of this repository
```bash
poetry install
```

2. Inside the `admin_apps/` directory, run the following command:

```bash
streamlit run chat_app.py  # for the Chat app
```

3. Enjoy!
