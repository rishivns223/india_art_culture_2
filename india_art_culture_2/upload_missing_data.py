import streamlit as st
from test_connection import get_connection
import pandas as pd

def upload_monuments_data():
    """Upload monuments data to Snowflake"""
    try:
        # Load local data
        monuments = pd.read_csv('data/raw/session_244_AU1787_1.1.csv')
        monuments = monuments[monuments['Sl.No'] != 'Total']  # Remove total row
        monuments.columns = ['SL_NO', 'STATE', 'MONUMENTS']
        
        # Clean state names
        state_name_mapping = {
            'N.C.T. Delhi': 'Delhi',
            'Daman & Diu (UT)': 'Daman and Diu',
            'Puducherry (U.T.)': 'Puducherry',
            'Jammu & Kashmir': 'Jammu and Kashmir'
        }
        monuments['STATE'] = monuments['STATE'].replace(state_name_mapping)
        
        # Upload to Snowflake
        conn = get_connection()
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS MONUMENTS (
            SL_NO INTEGER,
            STATE VARCHAR(100),
            MONUMENTS INTEGER
        )
        """)
        
        # Insert data
        for _, row in monuments.iterrows():
            cursor.execute(
                "INSERT INTO MONUMENTS (SL_NO, STATE, MONUMENTS) VALUES (%s, %s, %s)",
                (row['SL_NO'], row['STATE'], row['MONUMENTS'])
            )
        
        conn.commit()
        print("✅ Monuments data uploaded successfully")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error uploading monuments data: {str(e)}")

def upload_gender_tourism_data():
    """Upload gender tourism data to Snowflake"""
    try:
        # Load local data
        df = pd.read_csv('data/raw/India-Tourism-Statistics-2019-Table-2.6.1.csv')
        df.columns = ['YEAR', 'TOTAL_ARRIVALS', 'MALE_PCT', 'FEMALE_PCT', 'NOT_REPORTED_PCT']
        
        # Calculate actual numbers from percentages
        for col in ['MALE_PCT', 'FEMALE_PCT', 'NOT_REPORTED_PCT']:
            df[col.replace('_PCT', '_COUNT')] = (df['TOTAL_ARRIVALS'] * df[col] / 100).round().astype(int)
        
        # Upload to Snowflake
        conn = get_connection()
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS GENDER_TOURISM (
            YEAR INTEGER,
            TOTAL_ARRIVALS INTEGER,
            MALE_PCT FLOAT,
            FEMALE_PCT FLOAT,
            NOT_REPORTED_PCT FLOAT,
            MALE_COUNT INTEGER,
            FEMALE_COUNT INTEGER,
            NOT_REPORTED_COUNT INTEGER
        )
        """)
        
        # Insert data
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO GENDER_TOURISM (
                YEAR, TOTAL_ARRIVALS, MALE_PCT, FEMALE_PCT, NOT_REPORTED_PCT,
                MALE_COUNT, FEMALE_COUNT, NOT_REPORTED_COUNT
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['YEAR'], row['TOTAL_ARRIVALS'], row['MALE_PCT'],
                row['FEMALE_PCT'], row['NOT_REPORTED_PCT'], row['MALE_COUNT'],
                row['FEMALE_COUNT'], row['NOT_REPORTED_COUNT']
            ))
        
        conn.commit()
        print("✅ Gender tourism data uploaded successfully")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error uploading gender tourism data: {str(e)}")

def upload_geological_sites_data():
    """Upload geological sites data to Snowflake"""
    try:
        # Load local data
        df = pd.read_csv('data/raw/rs_session-238_AU1380_1.1.csv', encoding='cp1252')
        df.columns = ['SL_NO', 'STATE', 'SITE_NAME']
        df['STATE'] = df['STATE'].str.title()
        
        # Create site type classification
        def classify_site(name):
            if 'fossil' in name.lower():
                return 'Fossil Site'
            elif any(term in name.lower() for term in ['lava', 'volcanic', 'igneous']):
                return 'Volcanic/Igneous Formation'
            elif any(term in name.lower() for term in ['fault', 'unconformity']):
                return 'Geological Structure'
            elif any(term in name.lower() for term in ['lake', 'cliff', 'island']):
                return 'Natural Formation'
            else:
                return 'Other Geological Site'
        
        df['SITE_TYPE'] = df['SITE_NAME'].apply(classify_site)
        
        # Upload to Snowflake
        conn = get_connection()
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS GEOLOGICAL_SITES (
            SL_NO INTEGER,
            STATE VARCHAR(100),
            SITE_NAME VARCHAR(500),
            SITE_TYPE VARCHAR(100)
        )
        """)
        
        # Insert data
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO GEOLOGICAL_SITES (SL_NO, STATE, SITE_NAME, SITE_TYPE)
            VALUES (%s, %s, %s, %s)
            """, (row['SL_NO'], row['STATE'], row['SITE_NAME'], row['SITE_TYPE']))
        
        conn.commit()
        print("✅ Geological sites data uploaded successfully")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error uploading geological sites data: {str(e)}")

if __name__ == "__main__":
    print("Uploading missing data to Snowflake...")
    upload_monuments_data()
    upload_gender_tourism_data()
    upload_geological_sites_data()
    print("Done!") 