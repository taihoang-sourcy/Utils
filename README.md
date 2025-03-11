# Sentiment Analysis Dashboard

A Streamlit application that allows users to query sentiment analysis data from a PostgreSQL database based on a selected date range and export the results as CSV.

## Features

- Date range selection (default: yesterday to today)
- Query data from the `trends.sentiment_analysis` table
- Display results in an interactive table
- Export results as CSV file

## Prerequisites

- Python 3.7+
- PostgreSQL database with the `trends.sentiment_analysis` table
- Environment variables set in a `.env` file

## Installation

1. Clone this repository
2. Install the required packages:

```bash
pip install -r requirements.txt
```

## Running the Application

To run the Streamlit app:

```bash
streamlit run app.py
```

The application will be available at http://localhost:8501 in your web browser.

## Environment Variables

Make sure you have a `.env` file with the following variables:

```
PG_DATABASE=your_database_name
PG_USER=your_database_user
PG_PASSWORD=your_database_password
PG_HOST=your_database_host
PG_PORT=your_database_port
```

## Usage

1. Select a start date and end date from the sidebar
2. Click "Run Query" to fetch data from the database
3. View the results in the table
4. Click "Download as CSV" to export the results 