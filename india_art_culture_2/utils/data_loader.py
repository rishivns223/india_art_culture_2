import pandas as pd
import snowflake.connector
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_snowflake_connection():
    """
    Establish connection with Snowflake using environment variables
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

def load_cultural_sites():
    """
    Load cultural sites data from local CSV or Snowflake
    """
    # Placeholder data for development
    sample_data = {
        'site_name': ['Taj Mahal', 'Qutub Minar', 'Konark Sun Temple'],
        'latitude': [27.1751, 28.5244, 19.8876],
        'longitude': [78.0421, 77.1855, 86.0945],
        'type': ['Monument', 'Monument', 'Temple'],
        'state': ['Uttar Pradesh', 'Delhi', 'Odisha']
    }
    return pd.DataFrame(sample_data)

def load_tourism_data():
    """
    Load tourism statistics from local CSV or Snowflake
    """
    # Placeholder data for development
    sample_data = {
        'month': pd.date_range(start='2023-01-01', periods=12, freq='M'),
        'visitors': [150000, 120000, 180000, 200000, 160000, 140000,
                    130000, 145000, 190000, 210000, 180000, 220000],
        'region': ['North'] * 12
    }
    return pd.DataFrame(sample_data)

def load_art_forms_data():
    """
    Load information about traditional art forms
    """
    # Placeholder data for development
    sample_data = {
        'art_form': ['Bharatanatyam', 'Kathak', 'Madhubani', 'Pashmina'],
        'category': ['Classical Dance', 'Classical Dance', 'Folk Art', 'Textile Arts'],
        'region': ['South India', 'North India', 'Bihar', 'Kashmir'],
        'description': ['Classical dance from Tamil Nadu', 'Classical dance from North India',
                       'Traditional painting style', 'Traditional textile craft']
    }
    return pd.DataFrame(sample_data)

def save_to_csv(data, filename):
    """
    Save DataFrame to CSV file
    """
    data_dir = Path(__file__).parent.parent / 'data'
    data_dir.mkdir(exist_ok=True)
    data.to_csv(data_dir / filename, index=False)

def load_from_csv(filename):
    """
    Load DataFrame from CSV file
    """
    data_dir = Path(__file__).parent.parent / 'data'
    file_path = data_dir / filename
    if file_path.exists():
        return pd.read_csv(file_path)
    return None 