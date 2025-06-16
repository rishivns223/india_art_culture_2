-- Create database if not exists
CREATE DATABASE IF NOT EXISTS INDIA_CULTURAL_TOURISM;
USE DATABASE INDIA_CULTURAL_TOURISM;

-- Create schema
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- Art Forms table
CREATE OR REPLACE TABLE ART_FORMS (
    art_form_id NUMBER IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    origin_state VARCHAR(50),
    description TEXT,
    historical_period VARCHAR(100),
    current_status VARCHAR(50),
    practitioners_count NUMBER,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Cultural Sites table
CREATE OR REPLACE TABLE CULTURAL_SITES (
    site_id NUMBER IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    district VARCHAR(50),
    type VARCHAR(50),
    unesco_status BOOLEAN DEFAULT FALSE,
    year_established NUMBER,
    visitor_capacity NUMBER,
    description TEXT,
    latitude FLOAT,
    longitude FLOAT,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Tourism Statistics table
CREATE OR REPLACE TABLE TOURISM_STATS (
    stat_id NUMBER IDENTITY(1,1) PRIMARY KEY,
    site_id NUMBER,
    year NUMBER NOT NULL,
    month NUMBER NOT NULL,
    domestic_visitors NUMBER,
    international_visitors NUMBER,
    revenue FLOAT,
    peak_season BOOLEAN,
    FOREIGN KEY (site_id) REFERENCES CULTURAL_SITES(site_id)
);

-- Cultural Events table
CREATE OR REPLACE TABLE CULTURAL_EVENTS (
    event_id NUMBER IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    site_id NUMBER,
    event_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    description TEXT,
    expected_footfall NUMBER,
    historical_significance TEXT,
    FOREIGN KEY (site_id) REFERENCES CULTURAL_SITES(site_id)
);

-- Craft Traditions table
CREATE OR REPLACE TABLE CRAFT_TRADITIONS (
    craft_id NUMBER IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    state VARCHAR(50),
    category VARCHAR(50),
    materials_used TEXT,
    artisan_count NUMBER,
    description TEXT,
    preservation_status VARCHAR(50),
    economic_impact TEXT
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_art_forms_category ON ART_FORMS(category);
CREATE INDEX IF NOT EXISTS idx_cultural_sites_state ON CULTURAL_SITES(state);
CREATE INDEX IF NOT EXISTS idx_tourism_stats_year_month ON TOURISM_STATS(year, month);
CREATE INDEX IF NOT EXISTS idx_cultural_events_dates ON CULTURAL_EVENTS(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_craft_traditions_state ON CRAFT_TRADITIONS(state); 