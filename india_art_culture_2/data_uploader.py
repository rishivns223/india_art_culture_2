import os
import pandas as pd
from test_connection import get_connection
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def load_data_to_snowflake():
    """Load all CSV files from data/raw into Snowflake tables"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Map CSV files to table names
    file_table_map = {
        'art_forms.csv': 'ART_FORMS',
        'cultural_sites.csv': 'CULTURAL_SITES',
        'tourism_data.csv': 'TOURISM_STATS'
    }
    
    raw_data_path = os.path.join('data', 'raw')
    
    for filename, table in file_table_map.items():
        file_path = os.path.join(raw_data_path, filename)
        if os.path.exists(file_path):
            print(f"\nProcessing {filename}...")
            try:
                # Read CSV file
                df = pd.read_csv(file_path)
                
                # Clean column names (remove spaces, special chars)
                df.columns = [col.strip().upper().replace(' ', '_').replace('-', '_') 
                            for col in df.columns]
                
                # Write to Snowflake
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df,
                    table_name=table,
                    database='INDIA_CULTURAL_TOURISM',
                    schema='PUBLIC',
                    quote_identifiers=False,  # Don't quote identifiers to avoid case sensitivity issues
                    auto_create_table=True,  # Automatically create table
                    overwrite=True  # Replace existing data
                )
                
                if success:
                    print(f"✅ Successfully loaded {nrows} rows into {table}")
                else:
                    print(f"❌ Failed to load {filename}")
                    
            except Exception as e:
                print(f"❌ Error processing {filename}: {str(e)}")
        else:
            print(f"⚠️ File not found: {filename}")
    
    cursor.close()
    conn.close()
    print("\n✅ Data loading process completed!")

if __name__ == "__main__":
    load_data_to_snowflake() 