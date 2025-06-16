import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt
import os
import json
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Load environment variables
load_dotenv()

# Snowflake connection configuration
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

def check_snowflake_config():
    """Check Snowflake configuration and display status"""
    missing_vars = []
    for key in SNOWFLAKE_CONFIG:
        if not SNOWFLAKE_CONFIG[key]:
            missing_vars.append(key)
    
    if missing_vars:
        st.error(f"Missing Snowflake configuration variables: {', '.join(missing_vars)}")
        st.info("Please ensure these variables are set in your .env file")
        return False
    
    # Show configuration status (excluding password)
    st.write("### Snowflake Configuration Status")
    status_data = {
        'Variable': ['User', 'Account', 'Warehouse', 'Database', 'Schema'],
        'Value': [
            SNOWFLAKE_CONFIG['user'],
            SNOWFLAKE_CONFIG['account'],
            SNOWFLAKE_CONFIG['warehouse'],
            SNOWFLAKE_CONFIG['database'],
            SNOWFLAKE_CONFIG['schema']
        ],
        'Status': ['✅' if val else '❌' for val in [
            SNOWFLAKE_CONFIG['user'],
            SNOWFLAKE_CONFIG['account'],
            SNOWFLAKE_CONFIG['warehouse'],
            SNOWFLAKE_CONFIG['database'],
            SNOWFLAKE_CONFIG['schema']
        ]]
    }
    st.table(pd.DataFrame(status_data))
    return True

@st.cache_resource
def get_snowflake_connection():
    """Create and cache Snowflake connection"""
    try:
        if not check_snowflake_config():
            return None
            
        st.info("Attempting to connect to Snowflake...")
        
        # Handle new Snowflake URL format
        account = SNOWFLAKE_CONFIG['account']
        if 'snowflakecomputing.com' not in account and '.' not in account:
            # If it's not a full URL and doesn't contain a region, assume it's a new format account
            st.info(f"Using modern Snowflake account format: {account}")
            conn = snowflake.connector.connect(
                user=SNOWFLAKE_CONFIG['user'],
                password=SNOWFLAKE_CONFIG['password'],
                account=account,
                warehouse=SNOWFLAKE_CONFIG['warehouse'],
                database=SNOWFLAKE_CONFIG['database'],
                schema=SNOWFLAKE_CONFIG['schema']
            )
            st.success("Successfully connected to Snowflake!")
            return conn
        
        # Legacy format handling remains the same
        if '.' not in account:
            st.warning("Account URL doesn't contain region. Trying common regions...")
            regions = ['us-east-1', 'us-west-2', 'eu-central-1', 'ap-southeast-1']
            for region in regions:
                try:
                    modified_account = f"{account}.{region}"
                    st.info(f"Trying account: {modified_account}")
                    conn = snowflake.connector.connect(
                        user=SNOWFLAKE_CONFIG['user'],
                        password=SNOWFLAKE_CONFIG['password'],
                        account=modified_account,
                        warehouse=SNOWFLAKE_CONFIG['warehouse'],
                        database=SNOWFLAKE_CONFIG['database'],
                        schema=SNOWFLAKE_CONFIG['schema']
                    )
                    st.success(f"Successfully connected using region: {region}")
                    return conn
                except Exception as e:
                    st.warning(f"Failed with region {region}: {str(e)}")
                    continue
            
            st.error("Failed to connect with all common regions")
            return None
        
        # Try normal connection if account contains region
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG['user'],
            password=SNOWFLAKE_CONFIG['password'],
            account=SNOWFLAKE_CONFIG['account'],
            warehouse=SNOWFLAKE_CONFIG['warehouse'],
            database=SNOWFLAKE_CONFIG['database'],
            schema=SNOWFLAKE_CONFIG['schema']
        )
        st.success("Successfully connected to Snowflake!")
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")
        st.info("Falling back to local file storage.")
        return None

def execute_query(query):
    """Execute a query on Snowflake and return results as DataFrame"""
    conn = get_snowflake_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            df = pd.DataFrame(results, columns=columns)
            cur.close()
            return df
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
            return None
    return None

@st.cache_data
def load_all_data():
    """Load all data from Snowflake or local files as fallback"""
    datasets = {
        'art_and_culture': {},
        'tourism_statistics': {},
        'heritage': {},
        'parliament_data': {}
    }
    
    # Try Snowflake first
    conn = get_snowflake_connection()
    
    if conn:
        try:
            # Art and Culture Data
            art_forms_query = "SELECT * FROM ART_FORMS"
            df = execute_query(art_forms_query)
            if df is not None and not df.empty:
                datasets['art_and_culture']['art_forms'] = df
            
            # Festivals Data
            festivals_query = "SELECT * FROM FESTIVALS"
            df = execute_query(festivals_query)
            if df is not None and not df.empty:
                datasets['art_and_culture']['festivals'] = df.to_dict('records')
            
            # Tourism Statistics
            tourism_tables = [
                'TOURISM_STATISTICS_2019_2_1_1',
                'TOURISM_STATISTICS_2019_2_6_1',
                'TOURISM_STATISTICS_2021_2_3_3',
                'TOURISM_DATA',
                'TOURISM_STATISTICS_2018_2_1_1'
            ]
            
            for table in tourism_tables:
                query = f"SELECT * FROM {table}"
                df = execute_query(query)
                if df is not None and not df.empty:
                    key = table.lower()
                    datasets['tourism_statistics'][key] = df
            
            # Heritage Data
            heritage_tables = {
                'cultural_sites': 'CULTURAL_SITES',
                'heritage_cities': 'HERITAGE_CITIES'
            }
            
            for key, table in heritage_tables.items():
                query = f"SELECT * FROM {table}"
                df = execute_query(query)
                if df is not None and not df.empty:
                    datasets['heritage'][key] = df
            
            # Parliament Data
            parliament_tables = [
                'RS_SESSION_246_AU_2259',
                'RS_SESSION_248_AU_1232',
                'RS_SESSION_255_AU_1292',
                'RS_SESSION_259_AU_1898',
                'RS_SESSION_262_AU_497',
                'RS_SESSION_238_AU1380',
                'RS_SESSION_251_AU308',
                'RS_SESSION_251_AU1434',
                'SESSION_244_AU1787'
            ]
            
            for table in parliament_tables:
                query = f"SELECT * FROM {table}"
                df = execute_query(query)
                if df is not None and not df.empty:
                    key = table.lower()
                    datasets['parliament_data'][key] = df
            
            return datasets
            
        except Exception as e:
            st.error(f"Error loading data from Snowflake: {str(e)}")
            st.info("Falling back to local file storage.")
    
    # Fallback to local files if Snowflake fails
    data_dir = "/Users/rdhardubey/india_art_culture/data/raw"
    
    def try_read_csv(file_path):
        """Try different encodings to read CSV file"""
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
        for encoding in encodings:
            try:
                return pd.read_csv(file_path, encoding=encoding)
            except UnicodeDecodeError:
                continue
            except Exception as e:
                st.warning(f"Error reading {os.path.basename(file_path)}: {str(e)}")
                return None
        st.warning(f"Could not read {os.path.basename(file_path)} with any supported encoding")
        return None
    
    try:
        # Art and Culture Data
        art_forms_path = os.path.join(data_dir, 'art_forms.csv')
        if os.path.exists(art_forms_path):
            df = try_read_csv(art_forms_path)
            if df is not None and not df.empty:
                datasets['art_and_culture']['art_forms'] = df
        
        # Festivals
        festival_path = os.path.join(data_dir, 'Festival_of_India.json')
        if os.path.exists(festival_path):
            with open(festival_path, 'r') as f:
                festivals = json.load(f)
                df = pd.DataFrame(festivals)
                datasets['art_and_culture']['festivals'] = df.to_dict('records')
        
        # Tourism Statistics
        tourism_files = [
            'India-Tourism-Statistics-2019-Table-2.1.1.csv',
            'India-Tourism-Statistics-2019-Table-2.6.1.csv',
            'India-Tourism-Statistics-2021-Table-2.3.3.csv',
            'India-Tourism-Statistics-2021-Table-2.3.3(1).csv',
            'tourism_data.csv',
            'Tourism_In_India_Statistics_2018-Table_2.1.1_1.csv'
        ]
        
        for file in tourism_files:
            file_path = os.path.join(data_dir, file)
            if os.path.exists(file_path):
                df = try_read_csv(file_path)
                if df is not None and not df.empty:
                    key = file.replace('.csv', '').replace('-', '_').lower()
                    datasets['tourism_statistics'][key] = df
        
        # Heritage Data
        heritage_files = {
            'cultural_sites': 'cultural_sites.csv',
            'heritage_cities': 'List_of_Heritage_Cities.csv'
        }
        
        for key, file in heritage_files.items():
            file_path = os.path.join(data_dir, file)
            if os.path.exists(file_path):
                df = try_read_csv(file_path)
                if df is not None and not df.empty:
                    datasets['heritage'][key] = df
        
        # Parliament Data
        parliament_files = [
            'RS_Session_246_AU_2259_1.1.csv',
            'RS_Session_248_AU_1232.csv',
            'RS_Session_255_AU_1292.A_and_B.csv',
            'RS_Session_259_AU_1898_B_and_C.csv',
            'RS_Session_262_AU_497_B.csv',
            'rs_session-238_AU1380_1.1.csv',
            'RS-Session-251-AU308-Annexure-I.csv',
            'RS-Session-251-AU1434-Table1.csv',
            'session_244_AU1787_1.1.csv'
        ]
        
        for file in parliament_files:
            file_path = os.path.join(data_dir, file)
            if os.path.exists(file_path):
                df = try_read_csv(file_path)
                if df is not None and not df.empty:
                    key = file.replace('.csv', '').replace('-', '_').lower()
                    datasets['parliament_data'][key] = df
        
        return datasets
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

def upload_to_snowflake():
    """Upload local data to Snowflake tables"""
    data_dir = "/Users/rdhardubey/india_art_culture/data/raw"
    conn = get_snowflake_connection()
    
    if not conn:
        return
    
    try:
        # Create tables and upload data
        cur = conn.cursor()
        
        # Art Forms
        art_forms_path = os.path.join(data_dir, 'art_forms.csv')
        if os.path.exists(art_forms_path):
            df = pd.read_csv(art_forms_path)
            write_pandas(conn, df, 'ART_FORMS')
        
        # Festivals
        festival_path = os.path.join(data_dir, 'Festival_of_India.json')
        if os.path.exists(festival_path):
            with open(festival_path, 'r') as f:
                festivals = json.load(f)
                df = pd.DataFrame(festivals)
                write_pandas(conn, df, 'FESTIVALS')
        
        # Tourism Statistics
        tourism_files = [
            ('India-Tourism-Statistics-2019-Table-2.1.1.csv', 'TOURISM_STATISTICS_2019_2_1_1'),
            ('India-Tourism-Statistics-2019-Table-2.6.1.csv', 'TOURISM_STATISTICS_2019_2_6_1'),
            ('India-Tourism-Statistics-2021-Table-2.3.3.csv', 'TOURISM_STATISTICS_2021_2_3_3'),
            ('tourism_data.csv', 'TOURISM_DATA'),
            ('Tourism_In_India_Statistics_2018-Table_2.1.1_1.csv', 'TOURISM_STATISTICS_2018_2_1_1')
        ]
        
        for file, table in tourism_files:
            file_path = os.path.join(data_dir, file)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                write_pandas(conn, df, table)
        
        # Heritage Data
        heritage_files = [
            ('cultural_sites.csv', 'CULTURAL_SITES'),
            ('List_of_Heritage_Cities.csv', 'HERITAGE_CITIES')
        ]
        
        for file, table in heritage_files:
            file_path = os.path.join(data_dir, file)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                write_pandas(conn, df, table)
        
        # Parliament Data
        parliament_files = [
            ('RS_Session_246_AU_2259_1.1.csv', 'RS_SESSION_246_AU_2259'),
            ('RS_Session_248_AU_1232.csv', 'RS_SESSION_248_AU_1232'),
            ('RS_Session_255_AU_1292.A_and_B.csv', 'RS_SESSION_255_AU_1292'),
            ('RS_Session_259_AU_1898_B_and_C.csv', 'RS_SESSION_259_AU_1898'),
            ('RS_Session_262_AU_497_B.csv', 'RS_SESSION_262_AU_497'),
            ('rs_session-238_AU1380_1.1.csv', 'RS_SESSION_238_AU1380'),
            ('RS-Session-251-AU308-Annexure-I.csv', 'RS_SESSION_251_AU308'),
            ('RS-Session-251-AU1434-Table1.csv', 'RS_SESSION_251_AU1434'),
            ('session_244_AU1787_1.1.csv', 'SESSION_244_AU1787')
        ]
        
        for file, table in parliament_files:
            file_path = os.path.join(data_dir, file)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                write_pandas(conn, df, table)
        
        st.success("Data successfully uploaded to Snowflake!")
        
    except Exception as e:
        st.error(f"Error uploading data to Snowflake: {str(e)}")
    finally:
        if conn:
            conn.close()

def show_art_and_culture(datasets):
    st.header("Art and Culture")
    
    if 'art_forms' in datasets['art_and_culture']:
        st.subheader("Traditional Art Forms")
        df = datasets['art_and_culture']['art_forms']
        
        # Display summary statistics
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Total Art Forms", len(df))
            if 'category' in df.columns:
                st.metric("Categories", df['category'].nunique())
        
        with col2:
            if 'practitioners' in df.columns:
                total_practitioners = df['practitioners'].sum()
                st.metric("Total Practitioners", f"{total_practitioners:,}")
        
        # Display raw data with column selection
        st.subheader("Raw Data")
        if len(df.columns) > 0:
            columns_to_display = st.multiselect(
                "Select columns to display",
                df.columns.tolist(),
                default=df.columns.tolist()
            )
            st.dataframe(df[columns_to_display])
        
        # Visualizations
        st.subheader("Visualizations")
        
        if 'category' in df.columns:
            # Category distribution
            fig = px.pie(df, names='category', title="Distribution of Art Forms by Category")
            st.plotly_chart(fig)
        
        if 'state' in df.columns:
            # State-wise distribution
            state_counts = df['state'].value_counts().reset_index()
            state_counts.columns = ['state', 'count']
            fig = px.bar(state_counts, x='state', y='count', 
                        title="Art Forms by State",
                        labels={'count': 'Number of Art Forms', 'state': 'State'})
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig)
        
        if 'practitioners' in df.columns and 'state' in df.columns:
            # Practitioners by state
            practitioners_by_state = df.groupby('state')['practitioners'].sum().reset_index()
            fig = px.bar(practitioners_by_state, x='state', y='practitioners',
                        title="Practitioners by State",
                        labels={'practitioners': 'Number of Practitioners', 'state': 'State'})
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig)
    
    # Show festivals data
    if 'festivals' in datasets['art_and_culture']:
        st.subheader("Festivals of India")
        festivals = datasets['art_and_culture']['festivals']
        
        # Display festivals data in a structured way
        if isinstance(festivals, list):
            for festival in festivals:
                with st.expander(festival.get('name', 'Unknown Festival')):
                    st.write(f"**Region:** {festival.get('region', 'Not specified')}")
                    st.write(f"**State:** {festival.get('state', 'Not specified')}")
                    st.write(f"**Description:** {festival.get('description', 'No description available')}")
        elif isinstance(festivals, dict):
            for name, details in festivals.items():
                with st.expander(name):
                    for key, value in details.items():
                        st.write(f"**{key.title()}:** {value}")

def show_tourism_statistics(datasets):
    st.header("Tourism Statistics")
    
    tourism_data = datasets['tourism_statistics']
    
    if not tourism_data:
        st.warning("No tourism data available")
        return
    
    # Create tabs for different years
    years = ["2021", "2019", "2018", "All Years"]
    tabs = st.tabs(years)
    
    for year, tab in zip(years, tabs):
        with tab:
            if year == "All Years":
                st.subheader("Compare Data Across Years")
                # Allow selection of datasets to compare
                selected_datasets = st.multiselect(
                    "Select datasets to compare",
                    list(tourism_data.keys())
                )
                for dataset in selected_datasets:
                    st.write(f"### {dataset}")
                    df = tourism_data[dataset]
                    st.dataframe(df)
                    
                    # Create visualizations based on the data structure
                    if 'state' in df.columns:
                        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                        if len(numeric_cols) > 0:
                            selected_metric = st.selectbox(f"Select metric for {dataset}", numeric_cols)
                            fig = px.bar(df, x='state', y=selected_metric,
                                       title=f"{selected_metric} by State",
                                       labels={'state': 'State'})
                            fig.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig)
            else:
                st.subheader(f"{year} Tourism Data")
                year_data = {k: v for k, v in tourism_data.items() if year in k}
                if year_data:
                    for key, df in year_data.items():
                        st.write(f"### {key}")
                        st.dataframe(df)
                        
                        # Create visualizations based on the data structure
                        if 'state' in df.columns:
                            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                            if len(numeric_cols) > 0:
                                selected_metric = st.selectbox(f"Select metric for {key}", numeric_cols)
                                fig = px.bar(df, x='state', y=selected_metric,
                                           title=f"{selected_metric} by State",
                                           labels={'state': 'State'})
                                fig.update_layout(xaxis_tickangle=-45)
                                st.plotly_chart(fig)
                else:
                    st.info(f"No tourism data available for {year}")

def show_heritage_sites(datasets):
    st.header("Heritage Sites and Cities")
    
    heritage_data = datasets['heritage']
    
    if not heritage_data:
        st.warning("No heritage data available")
        return
    
    # Create tabs for different types of heritage sites
    tabs = st.tabs(["Cultural Sites", "Heritage Cities"])
    
    with tabs[0]:
        if 'cultural_sites' in heritage_data:
            st.subheader("Cultural Sites")
            df = heritage_data['cultural_sites']
            
            # Display summary metrics
            st.metric("Total Cultural Sites", len(df))
            
            # Display the data
            st.dataframe(df)
            
            # Add visualizations if we have relevant columns
            if 'state' in df.columns:
                sites_by_state = df['state'].value_counts().reset_index()
                sites_by_state.columns = ['state', 'count']
                fig = px.bar(sites_by_state, x='state', y='count',
                            title="Cultural Sites by State",
                            labels={'count': 'Number of Sites', 'state': 'State'})
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig)
            
    with tabs[1]:
        if 'heritage_cities' in heritage_data:
            st.subheader("Heritage Cities")
            df = heritage_data['heritage_cities']
            
            # Display summary metrics
            st.metric("Total Heritage Cities", len(df))
            
            # Display the data
            st.dataframe(df)
            
            # Add visualizations if we have relevant columns
            if 'state' in df.columns:
                cities_by_state = df['state'].value_counts().reset_index()
                cities_by_state.columns = ['state', 'count']
                fig = px.bar(cities_by_state, x='state', y='count',
                            title="Heritage Cities by State",
                            labels={'count': 'Number of Cities', 'state': 'State'})
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig)

def show_parliament_insights(datasets):
    st.header("Parliamentary Data Analysis")
    
    parliament_data = datasets['parliament_data']
    
    if not parliament_data:
        st.warning("No parliamentary data available")
        return
    
    # Create tabs for different categories of analysis
    tabs = st.tabs(["Artisan Statistics", "Cultural Funding", "Tourism Impact", "Regional Analysis"])
    
    with tabs[0]:
        st.subheader("Artisan Statistics by State/UT")
        artisan_data = None
        
        # Find the artisan statistics file
        for key, df in parliament_data.items():
            if 'artisans' in df.columns.str.lower().tolist():
                artisan_data = df
                break
        
        if artisan_data is not None:
            # Summary metrics in a nice layout
            col1, col2, col3 = st.columns(3)
            total_artisans = artisan_data.iloc[:, 1].sum()
            avg_artisans = artisan_data.iloc[:, 1].mean()
            max_state = artisan_data.iloc[artisan_data.iloc[:, 1].idxmax(), 0]
            
            with col1:
                st.metric("Total Artisans", f"{total_artisans:,}")
            with col2:
                st.metric("Average per State", f"{int(avg_artisans):,}")
            with col3:
                st.metric("Highest Contributing State", max_state)
            
            # Interactive Choropleth map
            st.subheader("Geographic Distribution of Artisans")
            fig = px.choropleth(
                artisan_data,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations=artisan_data.iloc[:, 0],
                color=artisan_data.iloc[:, 1],
                color_continuous_scale="Viridis",
                hover_data={artisan_data.columns[0]: True, artisan_data.columns[1]: True},
                labels={artisan_data.columns[1]: "Number of Artisans"},
                title="Interactive Map of Artisan Distribution"
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
            
            # Regional Analysis
            st.subheader("Regional Distribution Analysis")
            col1, col2 = st.columns(2)
            
            with col1:
                # Top 5 states
                top_5 = artisan_data.nlargest(5, artisan_data.columns[1])
                fig = px.bar(
                    top_5,
                    x=top_5.iloc[:, 0],
                    y=top_5.iloc[:, 1],
                    title="Top 5 States by Artisan Population",
                    labels={'x': 'State/UT', 'y': 'Number of Artisans'},
                    color=top_5.iloc[:, 1],
                    color_continuous_scale="Viridis"
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig)
            
            with col2:
                # Bottom 5 states
                bottom_5 = artisan_data.nsmallest(5, artisan_data.columns[1])
                fig = px.bar(
                    bottom_5,
                    x=bottom_5.iloc[:, 0],
                    y=bottom_5.iloc[:, 1],
                    title="Bottom 5 States by Artisan Population",
                    labels={'x': 'State/UT', 'y': 'Number of Artisans'},
                    color=bottom_5.iloc[:, 1],
                    color_continuous_scale="Viridis"
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig)
            
            # Distribution Analysis
            st.subheader("Statistical Distribution")
            fig = go.Figure()
            fig.add_trace(go.Box(
                y=artisan_data.iloc[:, 1],
                name="Distribution",
                boxpoints="all",
                jitter=0.3,
                pointpos=-1.8
            ))
            fig.update_layout(
                title="Distribution of Artisans Across States",
                yaxis_title="Number of Artisans",
                showlegend=False
            )
            st.plotly_chart(fig)
    
    with tabs[1]:
        st.subheader("Cultural Funding Analysis")
        funding_data = None
        
        for key, df in parliament_data.items():
            if 'funds' in df.columns.str.lower().tolist():
                funding_data = df
                break
        
        if funding_data is not None:
            # Summary metrics with year-over-year change
            total_allocated = funding_data['Funds Allocated'].sum()
            total_spent = funding_data['Funds Released/Spent'].sum()
            
            # Calculate year-over-year changes
            funding_data['YoY Change'] = funding_data['Funds Allocated'].pct_change() * 100
            latest_yoy = funding_data['YoY Change'].iloc[-1]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Total Funds Allocated",
                    f"₹{total_allocated:,.2f} Cr",
                    f"{latest_yoy:+.1f}% YoY"
                )
            with col2:
                st.metric(
                    "Total Funds Released",
                    f"₹{total_spent:,.2f} Cr"
                )
            with col3:
                utilization = (total_spent/total_allocated) * 100
                st.metric(
                    "Overall Fund Utilization",
                    f"{utilization:.1f}%"
                )
            
            # Interactive Time Series Analysis
            st.subheader("Funding Trends Over Time")
            
            # Allow users to choose metrics
            metrics = st.multiselect(
                "Select metrics to compare",
                ['Funds Allocated', 'Funds Released/Spent', 'Utilization %'],
                default=['Funds Allocated', 'Funds Released/Spent']
            )
            
            funding_data['Utilization %'] = (funding_data['Funds Released/Spent'] / funding_data['Funds Allocated']) * 100
            
            fig = go.Figure()
            
            for metric in metrics:
                if metric in ['Funds Allocated', 'Funds Released/Spent']:
                    fig.add_trace(go.Scatter(
                        x=funding_data['Year'],
                        y=funding_data[metric],
                        name=metric,
                        mode='lines+markers',
                        hovertemplate="%{y:.2f} Cr<extra></extra>"
                    ))
                else:
                    fig.add_trace(go.Scatter(
                        x=funding_data['Year'],
                        y=funding_data[metric],
                        name=metric,
                        mode='lines+markers',
                        yaxis="y2",
                        hovertemplate="%{y:.1f}%<extra></extra>"
                    ))
            
            fig.update_layout(
                title="Interactive Funding Analysis",
                xaxis_title="Year",
                yaxis_title="Amount (in Crores ₹)",
                yaxis2=dict(
                    title="Utilization %",
                    overlaying="y",
                    side="right"
                ),
                hovermode="x unified"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Waterfall chart for year-wise changes
            st.subheader("Year-wise Fund Allocation Changes")
            fig = go.Figure(go.Waterfall(
                name="Fund Allocation",
                orientation="v",
                measure=["relative"] * (len(funding_data) - 1) + ["total"],
                x=funding_data['Year'],
                y=funding_data['Funds Allocated'],
                connector={"line": {"color": "rgb(63, 63, 63)"}},
                decreasing={"marker": {"color": "red"}},
                increasing={"marker": {"color": "green"}},
                totals={"marker": {"color": "blue"}}
            ))
            fig.update_layout(
                title="Fund Allocation Progress",
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tabs[2]:
        st.subheader("Tourism Impact Analysis")
        tourism_data = None
        
        # Find tourism-related data
        for key, df in parliament_data.items():
            if 'tourist' in key.lower() or 'tourism' in key.lower():
                tourism_data = df
                break
        
        if tourism_data is not None:
            # Display tourism trends
            if 'year' in tourism_data.columns.str.lower():
                year_col = tourism_data.columns[tourism_data.columns.str.lower() == 'year'][0]
                numeric_cols = tourism_data.select_dtypes(include=['int64', 'float64']).columns
                
                # Allow metric selection
                selected_metric = st.selectbox(
                    "Select tourism metric to analyze",
                    numeric_cols
                )
                
                # Time series plot
                fig = px.line(
                    tourism_data,
                    x=year_col,
                    y=selected_metric,
                    title=f"Tourism Trend: {selected_metric}",
                    markers=True
                )
                fig.update_traces(line_width=3)
                st.plotly_chart(fig, use_container_width=True)
                
                # Year-over-year growth
                tourism_data['YoY Growth'] = tourism_data[selected_metric].pct_change() * 100
                
                fig = px.bar(
                    tourism_data,
                    x=year_col,
                    y='YoY Growth',
                    title="Year-over-Year Growth Rate",
                    labels={'YoY Growth': 'Growth Rate (%)'}
                )
                fig.update_traces(marker_color=tourism_data['YoY Growth'].apply(
                    lambda x: 'red' if x < 0 else 'green'
                ))
                st.plotly_chart(fig, use_container_width=True)
    
    with tabs[3]:
        st.subheader("Regional Analysis Dashboard")
        
        # Combine data from different sources for regional analysis
        regional_data = {}
        
        for key, df in parliament_data.items():
            if 'state' in df.columns.str.lower():
                state_col = df.columns[df.columns.str.lower() == 'state'][0]
                numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                
                if not numeric_cols.empty:
                    regional_data[key] = {
                        'data': df,
                        'state_col': state_col,
                        'metrics': numeric_cols
                    }
        
        if regional_data:
            # Allow users to select dataset and metric
            selected_dataset = st.selectbox(
                "Select dataset for regional analysis",
                list(regional_data.keys())
            )
            
            selected_metric = st.selectbox(
                "Select metric to analyze",
                regional_data[selected_dataset]['metrics']
            )
            
            df = regional_data[selected_dataset]['data']
            state_col = regional_data[selected_dataset]['state_col']
            
            # Create regional visualization
            fig = px.choropleth(
                df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations=df[state_col],
                color=df[selected_metric],
                color_continuous_scale="Viridis",
                title=f"Regional Distribution: {selected_metric}"
            )
            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig, use_container_width=True)
            
            # Top 5 and Bottom 5 regions
            col1, col2 = st.columns(2)
            
            with col1:
                top_5 = df.nlargest(5, selected_metric)
                fig = px.bar(
                    top_5,
                    x=state_col,
                    y=selected_metric,
                    title=f"Top 5 Regions: {selected_metric}",
                    color=selected_metric,
                    color_continuous_scale="Viridis"
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig)
            
            with col2:
                bottom_5 = df.nsmallest(5, selected_metric)
                fig = px.bar(
                    bottom_5,
                    x=state_col,
                    y=selected_metric,
                    title=f"Bottom 5 Regions: {selected_metric}",
                    color=selected_metric,
                    color_continuous_scale="Viridis"
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig)

def main():
    st.set_page_config(page_title="India's Cultural Heritage & Tourism", layout="wide")
    
    st.title("India's Cultural Heritage & Tourism Dashboard")
    
    # Add a sidebar for navigation
    st.sidebar.title("Navigation")
    
    # Add Snowflake connection status in sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("Snowflake Connection")
    if st.sidebar.button("Check Connection"):
        check_snowflake_config()
    
    # Add data upload option in sidebar
    if st.sidebar.button("Upload Data to Snowflake"):
        upload_to_snowflake()
    
    # Navigation options
    pages = {
        "Art and Culture": show_art_and_culture,
        "Tourism Statistics": show_tourism_statistics,
        "Heritage Sites": show_heritage_sites,
        "Parliamentary Insights": show_parliament_insights
    }
    
    selection = st.sidebar.radio("Go to", list(pages.keys()))
    
    try:
        # Load all datasets from Snowflake
        datasets = load_all_data()
        
        if datasets:
            # Show selected page
            pages[selection](datasets)
            
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.write("Please check your Snowflake connection and ensure all required tables exist.")

if __name__ == "__main__":
    main() 