import requests
import pandas as pd
import os
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def download_dataset(url, filename):
    """
    Download dataset from data.gov.in and save locally
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        data_dir = Path(__file__).parent.parent / 'data' / 'raw'
        data_dir.mkdir(parents=True, exist_ok=True)
        
        filepath = data_dir / filename
        with open(filepath, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {filename} successfully")
        return filepath
    except Exception as e:
        print(f"Error downloading {filename}: {e}")
        return None

def get_snowflake_connection():
    """
    Establish connection with Snowflake
    """
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def load_to_snowflake(df, table_name):
    """
    Load DataFrame to Snowflake table
    """
    conn = get_snowflake_connection()
    if conn:
        try:
            # Convert DataFrame to CSV
            temp_csv = f"temp_{table_name}.csv"
            df.to_csv(temp_csv, index=False)
            
            # Create table and load data
            cursor = conn.cursor()
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} LIKE {temp_csv} INFER_SCHEMA")
            cursor.execute(f"PUT file://{temp_csv} @%{table_name}")
            cursor.execute(f"COPY INTO {table_name}")
            
            # Cleanup
            os.remove(temp_csv)
            print(f"Loaded data into Snowflake table: {table_name}")
        except Exception as e:
            print(f"Error loading data to Snowflake: {e}")
        finally:
            conn.close()

def fetch_tourism_data():
    """
    Fetch tourism data from local CSV or use sample data
    """
    # Sample data for development
    tourism_data = {
        'state': ['Maharashtra', 'Tamil Nadu', 'Uttar Pradesh', 'Karnataka'],
        'domestic_visitors': [100000, 150000, 120000, 90000],
        'foreign_visitors': [20000, 25000, 15000, 18000],
        'month': ['January', 'January', 'January', 'January'],
        'year': [2023, 2023, 2023, 2023]
    }
    return pd.DataFrame(tourism_data)

def fetch_cultural_sites():
    """
    Fetch cultural sites data from local CSV or use sample data
    """
    sites_data = {
        'site_name': ['Taj Mahal', 'Hampi', 'Khajuraho', 'Ajanta Caves'],
        'state': ['Uttar Pradesh', 'Karnataka', 'Madhya Pradesh', 'Maharashtra'],
        'type': ['Monument', 'Archaeological Site', 'Temple', 'Cave'],
        'latitude': [27.1751, 15.3350, 24.8318, 20.5519],
        'longitude': [78.0421, 76.4600, 79.9199, 75.7000]
    }
    return pd.DataFrame(sites_data)

def fetch_art_forms():
    """
    Fetch art forms data from local CSV or use sample data
    """
    art_forms_data = {
        'art_form': ['Bharatanatyam', 'Kathakali', 'Madhubani', 'Gond'],
        'state': ['Tamil Nadu', 'Kerala', 'Bihar', 'Madhya Pradesh'],
        'category': ['Dance', 'Dance', 'Painting', 'Painting'],
        'practitioners': [1000, 500, 2000, 800]
    }
    return pd.DataFrame(art_forms_data)

def save_data(data, filename):
    """
    Save DataFrame to CSV in the data directory
    """
    data_dir = Path(__file__).parent.parent / 'data' / 'processed'
    data_dir.mkdir(parents=True, exist_ok=True)
    filepath = data_dir / filename
    data.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

def fetch_and_save_all_data():
    """
    Fetch all datasets and save them locally and to Snowflake
    """
    # Create directories if they don't exist
    data_dir = Path(__file__).parent.parent / 'data'
    (data_dir / 'raw').mkdir(parents=True, exist_ok=True)
    (data_dir / 'processed').mkdir(parents=True, exist_ok=True)
    
    # Fetch and save tourism data
    tourism_df = fetch_tourism_data()
    save_data(tourism_df, 'tourism_data.csv')
    load_to_snowflake(tourism_df, 'tourism_data')
    
    # Fetch and save cultural sites data
    sites_df = fetch_cultural_sites()
    save_data(sites_df, 'cultural_sites.csv')
    load_to_snowflake(sites_df, 'cultural_sites')
    
    # Fetch and save art forms data
    art_forms_df = fetch_art_forms()
    save_data(art_forms_df, 'art_forms.csv')
    load_to_snowflake(art_forms_df, 'art_forms')

if __name__ == "__main__":
    fetch_and_save_all_data() 