# Admin App

This folder contains a Streamlit app designed that is designed to help you create and iterate on your semantic models
for the Cortex Analyst feature.

The app currently supports two types of flows:

<table border="0">
   <tr>
     <td><img src="https://github.com/Snowflake-Labs/semantic-model-generator/assets/103658138/2b5fd664-9a9a-46ed-afbf-cb7638f0ad19" width="700"></td>
     <td><img src="https://github.com/Snowflake-Labs/semantic-model-generator/assets/103658138/d96a4255-9e82-41ba-8a82-dcb87353b667" width="500"></td>
  </tr>
  <tr>
      <td><strong>Iteration</strong> · so you can iterate on your existing semantic model by trying it live in a chat UI!</td>
      <td><strong>Builder</strong> · so you can create and refine a semantic model from scratch!</td>
   </tr>
<table>

## Get started

1. Install all dependencies by running the following command from the root of the repo:

```bash
pip install -e . && pip install -r admin_apps/requirements.txt
```

2. Run the following command from the root repo:

```bash
python -m streamlit run admin_apps/app.py 
```

3. Enjoy! 