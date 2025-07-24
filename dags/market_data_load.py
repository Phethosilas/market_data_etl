
import pandas as pd
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task()
def load_market_data(df: pd.DataFrame, table_name: str = "market_data", connection_id: str = "polygon_api_postgres_conn"):
    """
    Loads a pandas DataFrame into a PostgreSQL database table using Airflow's PostgresHook.
    Creates the table if it doesn't exist, and uses UPSERT to handle duplicates.
    Args:
        df (pd.DataFrame): DataFrame to load.
        table_name (str): Name of the table to load data into.
        connection_id (str): Airflow connection ID for the PostgreSQL database.
    """
    if df is None or df.empty:
        print("No data to load.")
        return
    
    # Rename 'from' column to 'from_date' if present
    if 'from' in df.columns:
        df = df.rename(columns={'from': 'from_date'})
    
    hook = PostgresHook(postgres_conn_id=connection_id)
    
    # Define column types more appropriately
    column_definitions = []
    for col in df.columns:
        if col in ['open', 'high', 'low', 'close', 'afterHours', 'preMarket']:
            column_definitions.append(f'"{col}" NUMERIC')
        elif col in ['volume']:
            column_definitions.append(f'"{col}" BIGINT')
        elif col in ['from_date']:
            column_definitions.append(f'"{col}" DATE')
        else:
            column_definitions.append(f'"{col}" TEXT')
    
    columns_with_types = ', '.join(column_definitions)
    
    # Create table with proper schema
    create_table_sql = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_with_types},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(symbol, from_date)
    );
    '''
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table {table_name} created or verified.")
        
        # Use UPSERT to handle duplicates
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(df.columns))
            columns_str = ', '.join([f'"{col}"' for col in df.columns])
            
            # Create conflict resolution for UPSERT
            update_columns = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col not in ['symbol', 'from_date']])
            
            upsert_sql = f'''
            INSERT INTO {table_name} ({columns_str}) 
            VALUES ({placeholders})
            ON CONFLICT (symbol, from_date) 
            DO UPDATE SET {update_columns}
            '''
            
            cursor.execute(upsert_sql, tuple(row))
        
        conn.commit()
        print(f"Successfully loaded/updated {len(df)} rows into {table_name}.")
        
    except Exception as e:
        conn.rollback()
        print(f"Error loading data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
