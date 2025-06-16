import streamlit as st
from test_connection import get_connection
import pandas as pd

def test_snowflake_data():
    """Test Snowflake connection and verify data in tables"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # List of tables to check
        tables = [
            'MONUMENTS',
            'GENDER_TOURISM',
            'GEOLOGICAL_SITES',
            'TOURISM_STATS',
            'ART_FORMS',
            'CULTURAL_SITES'
        ]
        
        print("\nChecking Snowflake tables:")
        print("=" * 50)
        
        for table in tables:
            try:
                # Check if table exists
                cursor.execute(f"SHOW TABLES LIKE '{table}'")
                if cursor.fetchone():
                    # Count rows
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    row_count = cursor.fetchone()[0]
                    print(f"✅ {table}: {row_count} rows")
                    
                    # Show sample data
                    cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                    columns = [desc[0] for desc in cursor.description]
                    sample = cursor.fetchone()
                    print(f"   Columns: {', '.join(columns)}")
                    print(f"   Sample: {sample}\n")
                else:
                    print(f"❌ Table {table} does not exist\n")
            except Exception as e:
                print(f"❌ Error checking {table}: {str(e)}\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Connection error: {str(e)}")

if __name__ == "__main__":
    test_snowflake_data() 