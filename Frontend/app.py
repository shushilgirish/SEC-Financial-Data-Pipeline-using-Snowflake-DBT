import streamlit as st
import requests
import pandas as pd
import snowflake.connector
from datetime import datetime
import json
import os

# ✅ Correct FastAPI Endpoint
API_URL = "https://sec-data-filling-fastapi-app-974490277552.us-central1.run.app/get_quarter"
config_path = "/app/Airflow/config/sec_config.json"  # ✅ Use direct path

# Check if the file exists before opening
if not os.path.exists(config_path):
    raise FileNotFoundError(f"Config file not found at: {config_path}")

with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# ✅ Snowflake Configuration
SNOWFLAKE_ACCOUNT = config['snowflake_account']
SNOWFLAKE_ROLE = config['snowflake_role']
SNOWFLAKE_WAREHOUSE = config['snowflake_warehouse']
SNOWFLAKE_DATABASE = config['snowflake_db']
SNOWFLAKE_SCHEMA = config['snowflake_schema_raw_data']

def update_config(date_input):
    # Load the existing config
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
    
    # Update the date field
    config['date'] = date_input
    
    # Save the updated config
    with open(config_path, 'w') as config_file:
        json.dump(config, config_file, indent=2)
    
    print(f"Updated config with date: {date_input}")

# ✅ Snowflake Connection Function
def get_snowflake_connection():
    """Establish a secure connection to Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user="SHUSHIL",
            password="Ganesh@1999",
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# ✅ Fetch Table List
def get_table_list():
    """Retrieve all tables in SEC_SCHEMA."""
    conn = get_snowflake_connection()
    if conn:
        try:
            query = f"SHOW TABLES IN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}"
            df = pd.read_sql(query, conn)
            conn.close()
            return df["name"].tolist()
        except Exception as e:
            st.error(f"Error fetching table list: {e}")
            return []
    return []

# ✅ Fetch Table Data
def fetch_table_data(table_name):
    """Retrieve up to 100 rows from a table in SEC_SCHEMA."""
    conn = get_snowflake_connection()
    if conn:
        try:
            query = f'SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}."{table_name}" LIMIT 100'
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# ✅ Validate Date Format
def is_valid_date(date_str):
    """Check if the date is in the correct format (YYYY-MM-DD) and within the valid range."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return 2009 <= dt.year <= 2024  # Ensure the date is in the range 2009-2024
    except ValueError:
        return False

# ✅ Streamlit UI
st.set_page_config(page_title="Quarter Finder & Snowflake Viewer", layout="wide")
st.sidebar.title("📊 Navigation")

# ✅ Sidebar - Select View
view_option = st.sidebar.radio("Choose View:", ["Find Quarter", "View Snowflake Tables"])

# ✅ Quarter Finder Feature
if view_option == "Find Quarter":
    st.title("📆 Quarter Finder API")
    st.markdown("Enter a date (YYYY-MM-DD) to find the corresponding **year and quarter**.")
    
    date_input = st.text_input("Enter Date (YYYY-MM-DD)", "2023-06-15")
    
    if st.button("Get Quarter"):
        if date_input and is_valid_date(date_input):
            # Send POST request to FastAPI backend
            response = requests.post(API_URL, json={"date": date_input.strip()})

            if response.status_code == 200:
                result = response.json()
                st.success(f"🗓 The corresponding year and quarter: **{result['year_quarter']}**")
                update_config(date_input)
                st.info(f"Updated config with date: {date_input}")  # Add logging to verify the update
            else:
                st.error(f"⚠️ API Error: {response.status_code} - {response.text}")
        else:
            st.error("❌ Invalid date! Please enter a valid date between **2009-2024**.")

# ✅ Snowflake Table Viewer Feature
elif view_option == "View Snowflake Tables":
    st.title("📂 Snowflake Table Viewer")

    tables = get_table_list()
   
    if tables:
        selected_table = st.sidebar.selectbox("Select a Table", tables)
       
        if selected_table:
            st.subheader(f"📄 Data from `{selected_table}`")
           
            query = st.text_area("📝 SQL Query", f'SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}."{selected_table}" LIMIT 100')

            if st.button("Run Query"):
                df = fetch_table_data(selected_table)
                if not df.empty:
                    st.dataframe(df)
                else:
                    st.error("⚠️ No data retrieved. Check table permissions or existence.")
    else:
        st.error("⚠️ No tables found. Check your **database connection** or **permissions**.")