import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import pytz

# Load environment variables
load_dotenv()

# Database connection parameters
DB_PARAMS = {
    "dbname": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT")
}

def connect_to_db():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

def query_sentiment_data(start_date, end_date):
    """Query sentiment analysis data from the database"""
    conn = connect_to_db()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT * FROM trends.sentiment_analysis
        WHERE created_at BETWEEN %s AND %s
        ORDER BY created_at DESC
        """
        
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def main():
    st.title("Sentiment Analysis Dashboard")
    
    # Set default date range (yesterday to now)
    singapore_tz = pytz.timezone('Asia/Singapore')
    now = datetime.now(singapore_tz)
    yesterday = now - timedelta(days=1)
    
    # Date range selection
    st.sidebar.header("Date Range Selection")
    
    start_date = st.sidebar.date_input(
        "Start Date",
        value=yesterday.date(),
        max_value=now.date()
    )
    
    end_date = st.sidebar.date_input(
        "End Date",
        value=now.date(),
        min_value=start_date,
        max_value=now.date()
    )
    
    # Convert dates to datetime with time components
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())
    
    # Display selected date range
    st.write(f"Selected date range: {start_date} to {end_date}")
    
    # Query button
    if st.button("Run Query"):
        with st.spinner("Querying database..."):
            df = query_sentiment_data(start_datetime, end_datetime)
            
            if not df.empty:
                st.success(f"Query successful! Found {len(df)} records.")
                
                # Display data
                st.subheader("Sentiment Analysis Results")
                st.dataframe(df)
                
                # Export to CSV
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=f"sentiment_analysis_{start_date}_to_{end_date}.csv",
                    mime="text/csv"
                )
            else:
                st.warning("No data found for the selected date range.")

if __name__ == "__main__":
    main() 