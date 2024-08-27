from snowflake.connector import SnowflakeConnection

def fetch_columns_in_table(conn: SnowflakeConnection, table_name: str) -> list[str]:
    """
    Fetches all columns in a Snowflake table table
    Args:
        conn: SnowflakeConnection to run the query
        table_name: The fully-qualified name of the table.

    Returns: a list of qualified schema names (db.schema)

    """
    query = f"show columns in table {table_name};"
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return [result[2] for result in results]

def clean_table_columns(conn: SnowflakeConnection,
                        snowflake_context: str,
                        tablename: str) -> None:
    """Renames table columns to remove alias prefixes and double quotes"""
    import ast
    columns = fetch_columns_in_table(conn, f'{snowflake_context}.{tablename}')

    for col in columns:
        if '.' in col:
            # new_col = ast.literal_eval(col).split('.')[-1]
            new_col = col.split('.')[-1].upper()
        else:
            new_col = col
        query = f'ALTER TABLE {snowflake_context}.{tablename} RENAME COLUMN "{col}" TO {new_col};'
        conn.cursor().execute(query)