# Indian Cultural Heritage Explorer

An interactive Streamlit application that showcases India's traditional art forms, cultural experiences, and promotes responsible tourism.

## Features

- Interactive map of cultural heritage sites
- Traditional art forms explorer
- Tourism analytics and trends
- Responsible tourism guide
- Integration with Snowflake for data management

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd india_art_culture
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
Create a `.env` file in the root directory with the following variables:
```
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

## Running the Application

To run the application:
```bash
streamlit run app.py
```

The application will be available at `http://localhost:8501`

## Project Structure

```
india_art_culture/
├── app.py                 # Main Streamlit application
├── requirements.txt       # Python dependencies
├── README.md             # This file
├── data/                 # Data directory for CSV files
└── utils/                # Utility functions
    ├── data_loader.py    # Data loading functions
    └── visualization.py  # Visualization functions
```

## Data Sources

The application uses data from:
- [data.gov.in](https://data.gov.in) for cultural heritage information
- Snowflake database for tourism statistics
- Local CSV files for cached data

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 