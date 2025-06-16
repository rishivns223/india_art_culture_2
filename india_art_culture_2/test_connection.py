import os
import ssl
import streamlit as st
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import DatabaseError, ProgrammingError

# Disable certificate verification (for testing only)
ssl._create_default_https_context = ssl._create_unverified_context

def get_connection() -> SnowflakeConnection:
    """Create and return a Snowflake connection using Streamlit secrets"""
    try:
        conn = snowflake.connector.connect(
            user=st.secrets.snowflake_user,
            password=st.secrets.snowflake_password,
            account=st.secrets.snowflake_account,
            warehouse=st.secrets.snowflake_warehouse,
            database=st.secrets.snowflake_database,
            schema=st.secrets.snowflake_schema,
            region=st.secrets.snowflake_region,
            insecure_mode=True,
            validate_default_parameters=False,
            ocsp_response_cache_filename=None,
            client_session_keep_alive=True,
            login_timeout=60
        )
        print("✅ Successfully connected to Snowflake!")
        return conn
    except (DatabaseError, ProgrammingError) as e:
        print(f"❌ Failed to connect to Snowflake: {str(e)}")
        raise

def test_connection():
    """Test the Snowflake connection"""
    try:
        conn = get_connection()
        
        # Execute a simple query to verify the connection
        cursor = conn.cursor()
        cursor.execute('SELECT CURRENT_VERSION()')
        version = cursor.fetchone()[0]
        print(f"Snowflake Version: {version}")
        
        cursor.close()
        conn.close()
        print("✅ Connection test completed successfully!")
        
    except Exception as e:
        print(f"❌ Connection test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_connection() 