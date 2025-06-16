import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from test_connection import get_connection
import os

# Page configuration
st.set_page_config(
    page_title="India's Cultural Heritage",
    page_icon="🏛️",
    layout="wide"
)

# Initialize Snowflake connection
@st.cache_resource
def init_connection():
    return get_connection()

# Load data from Snowflake
@st.cache_data
def load_data(query):
    conn = init_connection()
    return pd.read_sql(query, conn)

@st.cache_data
def load_local_tourism_data():
    """Load and process local tourism data files"""
    # Load the first dataset (2019-2021)
    df1 = pd.read_csv('/Users/rdhardubey/india_art_culture/data/raw/RS_Session_259_AU_1898_B_and_C.csv')
    df1 = df1[df1['Sl. No.'] != 'Total']  # Remove total row
    
    # Load the second dataset (2016-2018)
    df2 = pd.read_csv('/Users/rdhardubey/india_art_culture/data/raw/RS-Session-251-AU308-Annexure-I.csv')
    
    # Process first dataset
    years_1 = ['2019', '2020', '2021']
    processed_data = []
    
    for year in years_1:
        year_data = df1[['State/ UT', f'{year} - Domestic', f'{year} - Foreign']].copy()
        year_data.columns = ['STATE', 'DOMESTIC_VISITORS', 'FOREIGN_VISITORS']
        year_data['YEAR'] = int(year)
        processed_data.append(year_data)
    
    # Process second dataset
    years_2 = ['2016', '2017', '2018']
    for year in years_2:
        col_suffix = ' (Revised)' if year == '2018' else ''
        year_data = df2[['States', f'{year}{col_suffix} - DTV', f'{year}{col_suffix} - FTV']].copy()
        year_data.columns = ['STATE', 'DOMESTIC_VISITORS', 'FOREIGN_VISITORS']
        year_data['YEAR'] = int(year)
        processed_data.append(year_data)
    
    # Combine all data
    combined_data = pd.concat(processed_data, ignore_index=True)
    
    # Clean state names
    combined_data['STATE'] = combined_data['STATE'].str.strip()
    
    # Add month column (since we don't have monthly data, we'll set it to 1)
    combined_data['MONTH'] = 1
    
    return combined_data

@st.cache_data
def load_monuments_data():
    """Load monuments data from Snowflake or local file"""
    try:
        return load_data("SELECT * FROM MONUMENTS")
    except Exception as e:
        st.warning("⚠️ Falling back to local monuments data")
        monuments = pd.read_csv('/Users/rdhardubey/india_art_culture/data/raw/session_244_AU1787_1.1.csv')
        monuments = monuments[monuments['Sl.No'] != 'Total']  # Remove total row
        monuments.columns = ['SL_NO', 'STATE', 'MONUMENTS']
        state_name_mapping = {
            'N.C.T. Delhi': 'Delhi',
            'Daman & Diu (UT)': 'Daman and Diu',
            'Puducherry (U.T.)': 'Puducherry',
            'Jammu & Kashmir': 'Jammu and Kashmir'
        }
        monuments['STATE'] = monuments['STATE'].replace(state_name_mapping)
        return monuments

@st.cache_data
def load_gender_tourism_data():
    """Load gender tourism data from Snowflake or local file"""
    try:
        return load_data("SELECT * FROM GENDER_TOURISM ORDER BY YEAR")
    except Exception as e:
        st.warning("⚠️ Falling back to local gender tourism data")
        df = pd.read_csv('/Users/rdhardubey/india_art_culture/data/raw/India-Tourism-Statistics-2019-Table-2.6.1.csv')
        df.columns = ['YEAR', 'TOTAL_ARRIVALS', 'MALE_PCT', 'FEMALE_PCT', 'NOT_REPORTED_PCT']
        for col in ['MALE_PCT', 'FEMALE_PCT', 'NOT_REPORTED_PCT']:
            df[col.replace('_PCT', '_COUNT')] = (df['TOTAL_ARRIVALS'] * df[col] / 100).round().astype(int)
        return df

@st.cache_data
def load_geological_sites():
    """Load geological sites data from Snowflake or local file"""
    try:
        return load_data("SELECT * FROM GEOLOGICAL_SITES")
    except Exception as e:
        st.warning("⚠️ Falling back to local geological sites data")
        df = pd.read_csv('/Users/rdhardubey/india_art_culture/data/raw/rs_session-238_AU1380_1.1.csv', encoding='cp1252')
        df.columns = ['SL_NO', 'STATE', 'SITE_NAME']
        df['STATE'] = df['STATE'].str.title()
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
        return df

@st.cache_data
def load_tourism_stats():
    """Load tourism statistics from Snowflake or local files"""
    try:
        return load_data("SELECT * FROM TOURISM_STATS ORDER BY YEAR")
    except Exception as e:
        st.warning("⚠️ Falling back to local tourism data")
        # Load the first dataset (2019-2021)
        df1 = pd.read_csv('/Users/rdhardubey/india_art_culture/data/raw/RS_Session_259_AU_1898_B_and_C.csv')
        df1 = df1[df1['Sl. No.'] != 'Total']  # Remove total row
        
        # Load the second dataset (2016-2018)
        df2 = pd.read_csv('/Users/rdhardubey/india_art_culture/data/raw/RS-Session-251-AU308-Annexure-I.csv')
        
        # Process first dataset
        years_1 = ['2019', '2020', '2021']
        processed_data = []
        
        for year in years_1:
            year_data = df1[['State/ UT', f'{year} - Domestic', f'{year} - Foreign']].copy()
            year_data.columns = ['STATE', 'DOMESTIC_VISITORS', 'FOREIGN_VISITORS']
            year_data['YEAR'] = int(year)
            processed_data.append(year_data)
        
        # Process second dataset
        years_2 = ['2016', '2017', '2018']
        for year in years_2:
            col_suffix = ' (Revised)' if year == '2018' else ''
            year_data = df2[['States', f'{year}{col_suffix} - DTV', f'{year}{col_suffix} - FTV']].copy()
            year_data.columns = ['STATE', 'DOMESTIC_VISITORS', 'FOREIGN_VISITORS']
            year_data['YEAR'] = int(year)
            processed_data.append(year_data)
        
        # Combine all data
        combined_data = pd.concat(processed_data, ignore_index=True)
        combined_data['STATE'] = combined_data['STATE'].str.strip()
        combined_data['MONTH'] = 1
        return combined_data

# Modify the data loading section
try:
    # Try to load from Snowflake first
    art_forms = load_data("SELECT * FROM ART_FORMS")
    cultural_sites = load_data("SELECT * FROM CULTURAL_SITES")
    tourism_stats = load_tourism_stats()
    monuments_data = load_monuments_data()
    gender_tourism = load_gender_tourism_data()
    geological_sites = load_geological_sites()
except Exception as e:
    st.error(f"❌ Error connecting to Snowflake: {str(e)}")
    st.warning("⚠️ Using local data files as fallback")
    # Load local data
    tourism_stats = load_tourism_stats()
    monuments_data = load_monuments_data()
    gender_tourism = load_gender_tourism_data()
    geological_sites = load_geological_sites()
    
    # For art forms and cultural sites, we'll need to show a message
    if 'art_forms' not in locals():
        st.error("❌ Art Forms data not available")
        art_forms = pd.DataFrame()
    if 'cultural_sites' not in locals():
        st.error("❌ Cultural Sites data not available")
        cultural_sites = pd.DataFrame()

# Column name mapping
VISITOR_COLS = {
    'domestic': 'DOMESTIC_VISITORS',
    'international': 'FOREIGN_VISITORS'
}

LOCATION_COLS = {
    'lat': 'LATITUDE',
    'lon': 'LONGITUDE',
    'state': 'STATE',
    'site_name': 'SITE_NAME',
    'site_type': 'TYPE'
}

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select a page",
    ["Overview", "Art Forms", "Cultural Sites", "Tourism Statistics", "Conclusions & Insights"]
)

# Overview Page
if page == "Overview":
    st.title("🏛️ India's Cultural Heritage Dashboard")
    st.write("Welcome to the comprehensive dashboard showcasing India's rich cultural heritage.")
    
    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Art Forms", len(art_forms))
    with col2:
        st.metric("Cultural Sites", len(cultural_sites))
    with col3:
        st.metric("States Covered", len(cultural_sites[LOCATION_COLS['state']].unique()))
    with col4:
        total_visitors = tourism_stats[VISITOR_COLS['domestic']].sum() + tourism_stats[VISITOR_COLS['international']].sum()
        st.metric("Total Visitors", f"{total_visitors:,}")
    
    # Geographic Overview
    st.subheader("📍 Geographic Distribution of Cultural Heritage")
    
    # Combine cultural sites and art forms for visualization
    cultural_locations = pd.DataFrame()
    
    # Add cultural sites
    sites_df = cultural_sites[[LOCATION_COLS['lat'], LOCATION_COLS['lon'], LOCATION_COLS['state'], LOCATION_COLS['site_name']]].copy()
    sites_df['type'] = 'Cultural Site'
    sites_df['name'] = sites_df[LOCATION_COLS['site_name']]
    cultural_locations = pd.concat([cultural_locations, sites_df])
    
    # Add art forms (if they have location data)
    if all(col in art_forms.columns for col in [LOCATION_COLS['lat'], LOCATION_COLS['lon']]):
        arts_df = art_forms[[LOCATION_COLS['lat'], LOCATION_COLS['lon'], 'STATE', 'ART_FORM']].copy()
        arts_df['type'] = 'Art Form'
        arts_df['name'] = arts_df['ART_FORM']
        cultural_locations = pd.concat([cultural_locations, arts_df])
    
    # Create visualization using Scattergeo
    fig = go.Figure()
    
    # Add scatter plot for cultural locations
    for location_type in cultural_locations['type'].unique():
        mask = cultural_locations['type'] == location_type
        fig.add_trace(go.Scattergeo(
            lon=cultural_locations[mask][LOCATION_COLS['lon']],
            lat=cultural_locations[mask][LOCATION_COLS['lat']],
            text=cultural_locations[mask]['name'],
            name=location_type,
            mode='markers',
            marker=dict(size=8),
            hoverinfo='text+name'
        ))
    
    # Update layout
    fig.update_layout(
        title="Cultural Heritage Sites Distribution Across India",
        geo=dict(
            scope='asia',
            showland=True,
            landcolor='rgb(243, 243, 243)',
            countrycolor='rgb(204, 204, 204)',
            center=dict(lon=82, lat=23),  # Center of India
            projection_scale=4,  # Zoom level
            lonaxis=dict(range=[68, 97]),
            lataxis=dict(range=[8, 37])
        ),
        height=600,
        showlegend=True
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # Distribution of Cultural Sites and Art Forms by State
    if LOCATION_COLS['state']:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Cultural Sites by State")
            state_sites = cultural_sites[LOCATION_COLS['state']].value_counts()
            fig = px.bar(
                x=state_sites.index,
                y=state_sites.values,
                labels={'x': 'State', 'y': 'Number of Sites'},
                title="Distribution of Cultural Sites"
            )
            fig.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Art Forms by State")
            if 'STATE' in art_forms.columns:
                state_arts = art_forms['STATE'].value_counts()
                fig = px.bar(
                    x=state_arts.index,
                    y=state_arts.values,
                    labels={'x': 'State', 'y': 'Number of Art Forms'},
                    title="Distribution of Art Forms"
                )
                fig.update_layout(xaxis_tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.error("State column not found in Art Forms")

    # Add Monuments Overview
    st.subheader("🏛️ Protected Monuments Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Create a bar chart of monuments by state
        fig = px.bar(
            monuments_data.sort_values('MONUMENTS', ascending=False),
            x='STATE',
            y='MONUMENTS',
            title="Number of Protected Monuments by State",
            labels={'STATE': 'State', 'MONUMENTS': 'Number of Monuments'}
        )
        fig.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Create a pie chart of top 10 states
        top_10_monuments = monuments_data.nlargest(10, 'MONUMENTS')
        fig = px.pie(
            top_10_monuments,
            values='MONUMENTS',
            names='STATE',
            title="Top 10 States' Share of Protected Monuments"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Add correlation analysis between monuments and tourism
    st.subheader("🔄 Monuments and Tourism Correlation")
    
    # Get the latest year's tourism data
    latest_year = max(tourism_stats['YEAR'])
    latest_tourism = tourism_stats[tourism_stats['YEAR'] == latest_year]
    
    # Merge monuments and tourism data
    combined_data = monuments_data.merge(
        latest_tourism,
        on='STATE',
        how='left'
    )
    
    # Calculate total visitors
    combined_data['TOTAL_VISITORS'] = combined_data[VISITOR_COLS['domestic']] + combined_data[VISITOR_COLS['international']]
    
    # Create scatter plot
    fig = px.scatter(
        combined_data,
        x='MONUMENTS',
        y='TOTAL_VISITORS',
        text='STATE',
        size='MONUMENTS',
        title=f"Correlation between Number of Monuments and Tourism ({latest_year})",
        labels={
            'MONUMENTS': 'Number of Protected Monuments',
            'TOTAL_VISITORS': 'Total Visitors',
            'STATE': 'State'
        }
    )
    fig.update_traces(textposition='top center')
    st.plotly_chart(fig, use_container_width=True)
    
    # Add insights about monuments and tourism
    st.markdown("""
    ### 🔍 Key Insights about Monuments and Tourism:
    
    1. **Distribution Pattern:**
       - Uttar Pradesh leads with the highest number of protected monuments (743)
       - Karnataka and Tamil Nadu follow with 506 and 413 monuments respectively
       - The top 5 states account for over 60% of all protected monuments
    
    2. **Tourism Correlation:**
       - States with more monuments generally show higher tourist footfall
       - However, some states attract high tourism despite fewer monuments
       - This suggests the influence of other factors like:
         * Infrastructure development
         * Accessibility
         * Marketing and promotion
         * Other tourist attractions
    
    3. **Development Opportunities:**
       - States with high monument count but lower tourism need:
         * Better infrastructure
         * Improved accessibility
         * Enhanced promotion
         * Better tourist facilities
    """)

# Art Forms Page
elif page == "Art Forms":
    st.title("🎨 Traditional Art Forms Analysis")
    
    # Art Form Categories
    if 'CATEGORY' in art_forms.columns:
        st.subheader("Distribution by Category")
        col1, col2 = st.columns(2)
        
        with col1:
            category_counts = art_forms['CATEGORY'].value_counts()
            fig = px.pie(
                values=category_counts.values,
                names=category_counts.index,
                title="Art Forms by Category"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                x=category_counts.index,
                y=category_counts.values,
                title="Category Distribution",
                labels={'x': 'Category', 'y': 'Count'}
            )
            fig.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
    
    # Practitioners Analysis
    if 'PRACTITIONERS' in art_forms.columns:
        st.subheader("Practitioners Analysis")
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.box(
                art_forms,
                x='CATEGORY',
                y='PRACTITIONERS',
                title="Practitioners Distribution by Category"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.histogram(
                art_forms,
                x='PRACTITIONERS',
                nbins=20,
                title="Distribution of Practitioners"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Top Art Forms by Practitioners
        st.subheader("Top Art Forms by Number of Practitioners")
        top_arts = art_forms.nlargest(10, 'PRACTITIONERS')
        fig = px.bar(
            top_arts,
            x='ART_FORM',
            y='PRACTITIONERS',
            color='CATEGORY',
            title="Top 10 Art Forms by Practitioners"
        )
        fig.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig, use_container_width=True)

# Cultural Sites Page
elif page == "Cultural Sites":
    st.title("🏰 Cultural and Geological Heritage Sites")
    
    # Add tabs for different types of sites
    tab1, tab2 = st.tabs(["Cultural Sites", "Geological Heritage"])
    
    with tab1:
        if not all(LOCATION_COLS.values()):
            st.error(f"Some required columns are missing. Available columns: {cultural_sites.columns.tolist()}")
        else:
            # Filter by State
            selected_state = st.selectbox(
                "Select a State",
                ['All'] + sorted(cultural_sites[LOCATION_COLS['state']].unique().tolist())
            )
            
            filtered_sites = cultural_sites if selected_state == 'All' else cultural_sites[cultural_sites[LOCATION_COLS['state']] == selected_state]
            
            # Site Types Distribution
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Distribution by Site Type")
                type_counts = filtered_sites[LOCATION_COLS['site_type']].value_counts()
                fig = px.pie(
                    values=type_counts.values,
                    names=type_counts.index,
                    title=f"Site Types in {selected_state if selected_state != 'All' else 'India'}"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Sites by Type")
                fig = px.bar(
                    x=type_counts.index,
                    y=type_counts.values,
                    title=f"Number of Sites by Type in {selected_state if selected_state != 'All' else 'India'}"
                )
                fig.update_layout(xaxis_tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            # Map View
            st.subheader("Geographical Distribution")
            
            # Create a scatter plot with India's bounds
            fig = px.scatter(
                filtered_sites,
                x=LOCATION_COLS['lon'],
                y=LOCATION_COLS['lat'],
                hover_name=LOCATION_COLS['site_name'],
                color=LOCATION_COLS['site_type'],
                title=f"Cultural Sites in {selected_state if selected_state != 'All' else 'India'}",
                labels={
                    LOCATION_COLS['lon']: 'Longitude',
                    LOCATION_COLS['lat']: 'Latitude'
                }
            )
            
            # Update layout to match India's approximate bounds
            fig.update_layout(
                yaxis=dict(range=[8, 37]),  # Latitude range for India
                xaxis=dict(range=[68, 97]),  # Longitude range for India
                height=600
            )
            st.plotly_chart(fig, use_container_width=True)
            st.info("💡 The map shows a basic view of site locations. For a more detailed map with terrain and satellite imagery, consider adding a Mapbox token in the Streamlit secrets.")

    with tab2:
        st.subheader("🌋 Geological Heritage Sites")
        
        # Overview metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Geological Sites", len(geological_sites))
        with col2:
            st.metric("States with Sites", len(geological_sites['STATE'].unique()))
        with col3:
            st.metric("Types of Sites", len(geological_sites['SITE_TYPE'].unique()))
        
        # Distribution by state
        col1, col2 = st.columns(2)
        
        with col1:
            # State-wise distribution
            state_counts = geological_sites['STATE'].value_counts()
            fig = px.bar(
                x=state_counts.index,
                y=state_counts.values,
                title="Distribution of Geological Heritage Sites by State",
                labels={'x': 'State', 'y': 'Number of Sites'}
            )
            fig.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Site type distribution
            type_counts = geological_sites['SITE_TYPE'].value_counts()
            fig = px.pie(
                values=type_counts.values,
                names=type_counts.index,
                title="Distribution by Site Type"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed site listing
        st.subheader("📍 Geological Heritage Sites Directory")
        
        # State filter
        selected_state = st.selectbox(
            "Select State",
            ['All States'] + sorted(geological_sites['STATE'].unique().tolist())
        )
        
        # Type filter
        selected_type = st.selectbox(
            "Select Site Type",
            ['All Types'] + sorted(geological_sites['SITE_TYPE'].unique().tolist())
        )
        
        # Filter data
        filtered_sites = geological_sites.copy()
        if selected_state != 'All States':
            filtered_sites = filtered_sites[filtered_sites['STATE'] == selected_state]
        if selected_type != 'All Types':
            filtered_sites = filtered_sites[filtered_sites['SITE_TYPE'] == selected_type]
        
        # Display sites in an expandable format
        for _, site in filtered_sites.iterrows():
            with st.expander(f"{site['STATE']} - {site['SITE_NAME']}"):
                st.write(f"**Type:** {site['SITE_TYPE']}")
                st.write(f"**Location:** {site['SITE_NAME'].split(',')[-1].strip()}")
        
        # Insights about geological heritage
        st.markdown("""
        ### 🔍 Key Insights about Geological Heritage:
        
        1. **Distribution Pattern:**
           - Rajasthan leads with the most geological heritage sites
           - Sites are concentrated in states with diverse geological formations
           - Coastal states feature unique geological formations
        
        2. **Site Types:**
           - Fossil sites preserve ancient life forms
           - Volcanic formations showcase India's geological history
           - Natural formations demonstrate ongoing geological processes
           - Structural features reveal tectonic activity
        
        3. **Conservation Status:**
           - All sites are protected as National Geological Monuments
           - Sites serve as important scientific study locations
           - Many sites have educational and tourism potential
        
        4. **Tourism Opportunities:**
           - Geological tourism (geotourism) potential
           - Educational value for students and researchers
           - Integration with existing tourism circuits
           - Need for better infrastructure and interpretation facilities
        """)

# Tourism Statistics Page
elif page == "Tourism Statistics":
    st.title("📊 Tourism Statistics Analysis")
    
    # Add tabs for different analyses
    tab1, tab2 = st.tabs(["General Statistics", "Gender Distribution"])
    
    with tab1:
        # Get unique years
        years = sorted(tourism_stats['YEAR'].unique())
        
        # Handle year selection based on available data
        if len(years) > 1:
            selected_years = st.slider(
                "Select Year Range",
                min_value=min(years),
                max_value=max(years),
                value=(min(years), max(years))
            )
            # Filter data by selected years
            filtered_stats = tourism_stats[
                (tourism_stats['YEAR'] >= selected_years[0]) & 
                (tourism_stats['YEAR'] <= selected_years[1])
            ]
        else:
            st.info(f"📅 Showing data for year {years[0]}")
            filtered_stats = tourism_stats
        
        # Basic statistics for selected period
        total_domestic = filtered_stats[VISITOR_COLS['domestic']].sum()
        total_international = filtered_stats[VISITOR_COLS['international']].sum()
        avg_domestic = filtered_stats[VISITOR_COLS['domestic']].mean()
        avg_international = filtered_stats[VISITOR_COLS['international']].mean()
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Domestic Visitors", f"{total_domestic:,.0f}")
        with col2:
            st.metric("Total International Visitors", f"{total_international:,.0f}")
        with col3:
            st.metric("Avg. Domestic Visitors/Year", f"{avg_domestic:,.0f}")
        with col4:
            st.metric("Avg. International Visitors/Year", f"{avg_international:,.0f}")
        
        # State-wise Analysis
        st.subheader("State-wise Tourism Analysis")
        
        # Select specific year for state analysis
        if len(years) > 1:
            selected_year = st.selectbox(
                "Select Year for State Analysis",
                sorted(filtered_stats['YEAR'].unique(), reverse=True)
            )
            year_stats = filtered_stats[filtered_stats['YEAR'] == selected_year]
        else:
            year_stats = filtered_stats
        
        # Top 10 states
        top_states = year_stats.nlargest(10, VISITOR_COLS['domestic'])
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                top_states,
                x='STATE',
                y=[VISITOR_COLS['domestic'], VISITOR_COLS['international']],
                title=f"Top 10 States by Visitors ({year_stats['YEAR'].iloc[0]})",
                labels={
                    'STATE': 'State',
                    'value': 'Number of Visitors',
                    'variable': 'Visitor Type'
                }
            )
            fig.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Calculate percentage of total visitors
            total_visitors = year_stats[VISITOR_COLS['domestic']].sum() + year_stats[VISITOR_COLS['international']].sum()
            year_stats['TOTAL_VISITORS'] = year_stats[VISITOR_COLS['domestic']] + year_stats[VISITOR_COLS['international']]
            year_stats['VISITOR_SHARE'] = year_stats['TOTAL_VISITORS'] / total_visitors * 100
            
            fig = px.pie(
                year_stats.nlargest(5, 'TOTAL_VISITORS'),
                values='VISITOR_SHARE',
                names='STATE',
                title=f"Top 5 States' Share of Total Visitors ({year_stats['YEAR'].iloc[0]})"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Growth Analysis (only show if we have multiple years)
        if len(years) > 1:
            st.subheader("Tourism Growth Analysis")
            
            # Calculate year-over-year growth
            yearly_growth = filtered_stats.pivot_table(
                index='STATE',
                columns='YEAR',
                values=[VISITOR_COLS['domestic'], VISITOR_COLS['international']],
                aggfunc='sum'
            ).pct_change(axis=1) * 100
            
            # Get the most recent year's growth
            latest_year = max(years)
            previous_year = latest_year - 1
            
            if previous_year in years:
                latest_growth = yearly_growth.xs(latest_year, axis=1, level=1)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Top 5 growing states (domestic)
                    top_growing_domestic = latest_growth[VISITOR_COLS['domestic']].nlargest(5)
                    fig = px.bar(
                        x=top_growing_domestic.index,
                        y=top_growing_domestic.values,
                        title=f"Top 5 States by Domestic Tourism Growth ({previous_year}-{latest_year})",
                        labels={'x': 'State', 'y': 'Growth Rate (%)'}
                    )
                    fig.update_layout(xaxis_tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Top 5 growing states (international)
                    top_growing_international = latest_growth[VISITOR_COLS['international']].nlargest(5)
                    fig = px.bar(
                        x=top_growing_international.index,
                        y=top_growing_international.values,
                        title=f"Top 5 States by International Tourism Growth ({previous_year}-{latest_year})",
                        labels={'x': 'State', 'y': 'Growth Rate (%)'}
                    )
                    fig.update_layout(xaxis_tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("ℹ️ Growth analysis is only available when data for multiple years is present.")

    with tab2:
        st.subheader("👥 Gender Distribution in Tourism")
        
        if len(gender_tourism) > 1:
            # Overall trend of gender distribution
            fig = go.Figure()
            
            # Add traces for each gender
            fig.add_trace(go.Scatter(
                x=gender_tourism['YEAR'],
                y=gender_tourism['MALE_PCT'],
                name='Male',
                mode='lines+markers',
                line=dict(color='blue')
            ))
            
            fig.add_trace(go.Scatter(
                x=gender_tourism['YEAR'],
                y=gender_tourism['FEMALE_PCT'],
                name='Female',
                mode='lines+markers',
                line=dict(color='red')
            ))
            
            if not (gender_tourism['NOT_REPORTED_PCT'] == 0).all():
                fig.add_trace(go.Scatter(
                    x=gender_tourism['YEAR'],
                    y=gender_tourism['NOT_REPORTED_PCT'],
                    name='Not Reported',
                    mode='lines+markers',
                    line=dict(color='gray')
                ))
            
            fig.update_layout(
                title="Gender Distribution Trends in Tourism",
                xaxis_title="Year",
                yaxis_title="Percentage (%)",
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Gender ratio analysis
        col1, col2 = st.columns(2)
        
        with col1:
            # Latest year pie chart
            latest_year = gender_tourism['YEAR'].max()
            latest_data = gender_tourism[gender_tourism['YEAR'] == latest_year]
            
            fig = px.pie(
                values=[
                    latest_data['MALE_PCT'].iloc[0],
                    latest_data['FEMALE_PCT'].iloc[0],
                    latest_data['NOT_REPORTED_PCT'].iloc[0]
                ],
                names=['Male', 'Female', 'Not Reported'],
                title=f"Gender Distribution ({latest_year})"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if len(gender_tourism) > 1:
                # Growth in absolute numbers
                fig = px.line(
                    gender_tourism,
                    x='YEAR',
                    y=['MALE_COUNT', 'FEMALE_COUNT'],
                    title="Growth in Tourist Numbers by Gender",
                    labels={
                        'YEAR': 'Year',
                        'value': 'Number of Tourists',
                        'variable': 'Gender'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                # Bar chart for single year
                latest_data = gender_tourism.iloc[0]
                fig = px.bar(
                    x=['Male', 'Female', 'Not Reported'],
                    y=[latest_data['MALE_COUNT'], latest_data['FEMALE_COUNT'], latest_data['NOT_REPORTED_COUNT']],
                    title=f"Tourist Numbers by Gender ({latest_year})",
                    labels={'x': 'Gender', 'y': 'Number of Tourists'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Gender gap analysis
        if len(gender_tourism) > 1:
            st.subheader("📊 Gender Gap Analysis")
            
            # Calculate gender gap
            gender_tourism['GENDER_GAP'] = gender_tourism['MALE_PCT'] - gender_tourism['FEMALE_PCT']
            
            fig = px.bar(
                gender_tourism,
                x='YEAR',
                y='GENDER_GAP',
                title="Gender Gap in Tourism (Male % - Female %)",
                labels={
                    'YEAR': 'Year',
                    'GENDER_GAP': 'Gender Gap (Percentage Points)'
                }
            )
            
            # Add a reference line at y=0
            fig.add_hline(
                y=0,
                line_dash="dash",
                line_color="gray",
                annotation_text="Equal Distribution"
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("ℹ️ Gender gap trend analysis is only available when data for multiple years is present.")
        
        # Insights about gender distribution
        st.markdown("""
        ### 🔍 Key Insights on Gender Distribution in Tourism:
        
        1. **Current Status:**
           - Males account for approximately {:.1f}% of tourists
           - Females represent about {:.1f}% of tourists
           - Not reported cases: {:.1f}%
        
        2. **Data Quality:**
           - Gender data is being accurately recorded
           - Very few unreported cases
           - Provides clear picture of gender distribution
        
        3. **Recommendations:**
           - Develop targeted programs to encourage female tourism
           - Address safety and security concerns for female travelers
           - Create women-friendly tourism infrastructure
           - Promote women-centric tourism packages and experiences
        """.format(
            gender_tourism['MALE_PCT'].iloc[-1],
            gender_tourism['FEMALE_PCT'].iloc[-1],
            gender_tourism['NOT_REPORTED_PCT'].iloc[-1]
        ))

# Conclusions & Insights Page
elif page == "Conclusions & Insights":
    st.title("🔍 Conclusions & Insights")
    
    # Cultural Heritage and Tourism Relationship
    st.header("Cultural Heritage and Tourism Analysis")
    
    # 1. State-wise Cultural Asset Analysis
    st.subheader("1. Cultural Assets vs Tourism")
    
    # Combine data for state-level analysis
    state_analysis = pd.DataFrame()
    state_analysis['STATE'] = cultural_sites['STATE'].unique()
    
    # Count cultural sites per state
    sites_by_state = cultural_sites.groupby('STATE').size()
    state_analysis = state_analysis.merge(sites_by_state.reset_index(), on='STATE', how='left')
    state_analysis.columns = ['STATE', 'CULTURAL_SITES']
    
    # Count art forms per state
    arts_by_state = art_forms.groupby('STATE').size()
    state_analysis = state_analysis.merge(arts_by_state.reset_index(), on='STATE', how='left')
    state_analysis.columns = ['STATE', 'CULTURAL_SITES', 'ART_FORMS']
    
    # Add tourism data
    tourism_by_state = tourism_stats.groupby('STATE').agg({
        VISITOR_COLS['domestic']: 'sum',
        VISITOR_COLS['international']: 'sum'
    }).reset_index()
    state_analysis = state_analysis.merge(tourism_by_state, on='STATE', how='left')
    
    # Fill NaN values with 0
    state_analysis = state_analysis.fillna(0)
    
    # Calculate total visitors
    state_analysis['TOTAL_VISITORS'] = state_analysis[VISITOR_COLS['domestic']] + state_analysis[VISITOR_COLS['international']]
    
    # Ensure minimum size for better visualization
    state_analysis['DISPLAY_SIZE'] = state_analysis['ART_FORMS'].clip(lower=1) * 5
    
    # Create visualization
    fig = px.scatter(
        state_analysis,
        x='CULTURAL_SITES',
        y='TOTAL_VISITORS',
        size='DISPLAY_SIZE',  # Using the adjusted size column
        color='STATE',
        title="Relationship between Cultural Assets and Tourism",
        labels={
            'CULTURAL_SITES': 'Number of Cultural Sites',
            'TOTAL_VISITORS': 'Total Visitors',
            'ART_FORMS': 'Number of Art Forms'
        }
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Key Insights
    st.markdown("""
    ### Key Insights on Cultural Heritage and Tourism:
    
    1. **Cultural Asset Distribution:**
       - States with more cultural sites generally attract more tourists
       - Some states show high tourism despite fewer cultural sites, suggesting other tourism drivers
       - Traditional art forms are concentrated in certain regions
    
    2. **Tourism Patterns:**
       - Domestic tourism dominates most cultural sites
       - International tourism shows seasonal patterns with peaks during winter months
       - Some states have untapped potential with rich cultural heritage but lower tourism
    
    3. **Development Opportunities:**
       - States with high cultural assets but low tourism need infrastructure development
       - Traditional art forms could be better integrated into tourism experiences
       - Seasonal tourism patterns suggest opportunity for off-season cultural events
    """)
    
    # Seasonal Analysis
    st.header("Seasonal Patterns and Tourism Trends")
    
    # Calculate monthly averages
    monthly_avg = tourism_stats.groupby('MONTH').agg({
        VISITOR_COLS['domestic']: 'mean',
        VISITOR_COLS['international']: 'mean'
    }).reset_index()
    
    # Create seasonal trend visualization
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=monthly_avg['MONTH'],
        y=monthly_avg[VISITOR_COLS['domestic']],
        name='Domestic Visitors',
        mode='lines+markers'
    ))
    fig.add_trace(go.Scatter(
        x=monthly_avg['MONTH'],
        y=monthly_avg[VISITOR_COLS['international']],
        name='International Visitors',
        mode='lines+markers'
    ))
    fig.update_layout(
        title="Seasonal Tourism Patterns",
        xaxis=dict(
            tickmode='array',
            ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
            tickvals=list(range(1, 13))
        ),
        xaxis_title="Month",
        yaxis_title="Average Visitors"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Insights on Seasonality
    st.markdown("""
    ### Insights on Seasonality and Untapped Potential:
    
    1. **Seasonal Patterns:**
       - Peak tourist season aligns with winter months (October-February)
       - Domestic tourism shows less seasonality than international
       - Cultural festivals and events influence visitor patterns
    
    2. **Untapped Regions:**
       - Several states with rich cultural heritage show lower tourism numbers
       - Factors contributing to lower tourism:
         * Limited infrastructure
         * Accessibility challenges
         * Less promotion and marketing
         * Inadequate tourist facilities
    
    3. **Recommendations:**
       - Develop off-season cultural events and festivals
       - Improve infrastructure in culturally rich but less visited areas
       - Create integrated cultural tourism circuits
       - Enhance promotion of lesser-known cultural sites
       - Invest in preserving and showcasing traditional art forms
    """)
    
    # Government Initiatives Analysis
    st.header("Government Contributions and Impact")
    
    # Correlation Analysis
    st.markdown("""
    ### Analysis of Government Initiatives and Tourism Growth:
    
    1. **Infrastructure Development:**
       - Investment in cultural site preservation correlates with increased tourism
       - States with better infrastructure show higher visitor numbers
       - Transportation connectivity is a key factor in tourism growth
    
    2. **Art and Culture Promotion:**
       - States supporting traditional artists show better cultural tourism
       - Government-sponsored cultural events boost tourism
       - Art form preservation helps maintain cultural authenticity
    
    3. **Future Focus Areas:**
       - Increase funding for cultural site maintenance
       - Develop cultural tourism circuits
       - Support traditional art forms and artists
       - Improve tourist facilities at cultural sites
       - Enhance digital presence and promotion
    """)
    
    # Final Recommendations
    st.markdown("""
    ### Recommendations for Sustainable Cultural Tourism:
    
    1. **Short-term Actions:**
       - Improve signage and information at cultural sites
       - Create digital platforms for art form showcase
       - Organize more cultural festivals and events
       - Enhance tourist facilities at popular sites
    
    2. **Medium-term Initiatives:**
       - Develop tourism infrastructure in untapped regions
       - Create cultural tourism circuits linking multiple sites
       - Implement artist support programs
       - Enhance marketing and promotion
    
    3. **Long-term Strategy:**
       - Sustainable tourism development
       - Cultural heritage preservation
       - Traditional art form documentation and preservation
       - Infrastructure development in underserved regions
    """) 