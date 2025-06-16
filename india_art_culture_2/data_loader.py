import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
from typing import Dict, List
import random

class DataLoader:
    def __init__(self):
        self.data_dir = "data"
        self.base_url = "https://data.gov.in/api/datastore/resource.json"
        os.makedirs(self.data_dir, exist_ok=True)
        
    def download_dataset(self, resource_id: str, filename: str) -> str:
        """Download dataset from data.gov.in API and save to CSV"""
        try:
            print(f"Downloading {filename}...")
            url = f"{self.base_url}?resource_id={resource_id}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                if 'records' in data:
                    df = pd.DataFrame(data['records'])
                    filepath = os.path.join(self.data_dir, filename)
                    df.to_csv(filepath, index=False)
                    print(f"✅ Successfully downloaded {filename}")
                    return filepath
                else:
                    print(f"❌ No records found in response for {filename}")
                    return None
            else:
                print(f"❌ Failed to download {filename}: {response.status_code}")
                return None
        except Exception as e:
            print(f"❌ Error downloading {filename}: {str(e)}")
            return None

    def load_tourism_statistics(self):
        """Load tourism statistics datasets"""
        print("\nDownloading Tourism Statistics...")
        # Foreign Tourist Arrivals
        self.download_dataset(
            "foreign-tourist-arrivals-ftas-2001-2023",
            "foreign_tourist_arrivals.csv"
        )
        
        # Domestic Tourism
        self.download_dataset(
            "state-wise-domestic-tourists-visits-2001-2023",
            "domestic_tourism.csv"
        )

    def load_cultural_heritage(self):
        """Load cultural heritage datasets"""
        print("\nDownloading Cultural Heritage Data...")
        # UNESCO World Heritage Sites
        self.download_dataset(
            "unesco-world-heritage-sites-india",
            "unesco_sites.csv"
        )
        
        # Protected Monuments
        self.download_dataset(
            "state-wise-protected-monuments-archaeological-sites",
            "protected_monuments.csv"
        )

    def load_art_forms(self):
        """Load traditional art forms datasets"""
        print("\nDownloading Art Forms Data...")
        # Traditional Art Forms
        self.download_dataset(
            "traditional-art-forms-india",
            "traditional_art_forms.csv"
        )
        
        # Craft Traditions
        self.download_dataset(
            "handicrafts-and-artisans-data",
            "craft_traditions.csv"
        )

    def create_sample_data(self):
        """Create sample data for testing"""
        print("\nCreating sample data...")
        
        # Sample Foreign Tourist Arrivals
        tourist_data = pd.DataFrame({
            'Year': range(2001, 2024),
            'FTAs': [random.randint(5000000, 15000000) for _ in range(23)]
        })
        tourist_data.to_csv(os.path.join(self.data_dir, 'foreign_tourist_arrivals.csv'), index=False)
        
        # Sample Domestic Tourism
        states = ['Maharashtra', 'Tamil Nadu', 'Uttar Pradesh', 'Karnataka', 'Kerala']
        domestic_data = []
        for state in states:
            for year in range(2001, 2024):
                domestic_data.append({
                    'Year': year,
                    'State': state,
                    'Visitors': random.randint(1000000, 5000000)
                })
        pd.DataFrame(domestic_data).to_csv(os.path.join(self.data_dir, 'domestic_tourism.csv'), index=False)
        
        # Sample UNESCO Sites
        unesco_data = pd.DataFrame({
            'Site_Name': ['Taj Mahal', 'Ajanta Caves', 'Konark Sun Temple'],
            'State': ['Uttar Pradesh', 'Maharashtra', 'Odisha'],
            'Latitude': [27.1751, 20.5519, 19.8876],
            'Longitude': [78.0421, 75.7033, 86.0945]
        })
        unesco_data.to_csv(os.path.join(self.data_dir, 'unesco_sites.csv'), index=False)
        
        # Sample Protected Monuments
        monuments_data = pd.DataFrame({
            'State': states,
            'Monument_Count': [random.randint(50, 200) for _ in range(len(states))]
        })
        monuments_data.to_csv(os.path.join(self.data_dir, 'protected_monuments.csv'), index=False)
        
        # Sample Art Forms
        art_categories = ['Classical Dance', 'Classical Music', 'Folk Art', 'Martial Arts']
        art_forms = []
        for category in art_categories:
            for i in range(3):
                art_forms.append({
                    'Category': category,
                    'Name': f'{category} Form {i+1}'
                })
        pd.DataFrame(art_forms).to_csv(os.path.join(self.data_dir, 'traditional_art_forms.csv'), index=False)
        
        # Sample Craft Traditions
        craft_types = ['Textiles', 'Pottery', 'Metal Work', 'Wood Carving']
        crafts = []
        for state in states:
            for craft in craft_types:
                crafts.append({
                    'State': state,
                    'Craft_Type': craft
                })
        pd.DataFrame(crafts).to_csv(os.path.join(self.data_dir, 'craft_traditions.csv'), index=False)
        
        print("✅ Successfully created sample data")

if __name__ == "__main__":
    print("Starting data process...")
    loader = DataLoader()
    print("\n⚠️ Using sample data for development...")
    loader.create_sample_data()
    print("\nData process completed!") 