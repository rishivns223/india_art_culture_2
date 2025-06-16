import folium
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def create_map(cultural_sites_df):
    """
    Create an interactive map with cultural sites
    """
    # Create a map centered on India
    m = folium.Map(location=[20.5937, 78.9629], zoom_start=5)
    
    # Add markers for each cultural site
    for idx, row in cultural_sites_df.iterrows():
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=f"<b>{row['site_name']}</b><br>Type: {row['type']}<br>State: {row['state']}",
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(m)
    
    return m

def create_trend_chart(tourism_data, metric='visitors'):
    """
    Create a line chart showing tourism trends
    """
    fig = px.line(
        tourism_data,
        x='month',
        y=metric,
        title=f'Tourism {metric.title()} Trend'
    )
    fig.update_layout(
        xaxis_title="Month",
        yaxis_title=metric.title(),
        showlegend=True
    )
    return fig

def create_regional_distribution(data, value_column, title):
    """
    Create a pie chart showing regional distribution
    """
    regional_dist = data.groupby('region')[value_column].sum().reset_index()
    fig = px.pie(
        regional_dist,
        values=value_column,
        names='region',
        title=title
    )
    return fig

def create_art_forms_sunburst(art_forms_data):
    """
    Create a sunburst chart for art forms categorization
    """
    fig = px.sunburst(
        art_forms_data,
        path=['category', 'art_form'],
        title='Traditional Art Forms Categories'
    )
    return fig

def create_monthly_visitors_heatmap(tourism_data):
    """
    Create a heatmap of monthly visitors
    """
    # Pivot the data for heatmap format
    heatmap_data = tourism_data.pivot_table(
        values='visitors',
        index='region',
        columns=tourism_data['month'].dt.strftime('%B'),
        aggfunc='sum'
    )
    
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=heatmap_data.columns,
        y=heatmap_data.index,
        colorscale='Viridis'
    ))
    
    fig.update_layout(
        title='Monthly Visitors by Region',
        xaxis_title='Month',
        yaxis_title='Region'
    )
    
    return fig 