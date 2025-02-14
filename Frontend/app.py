import streamlit as st
import requests
import pandas as pd
import snowflake.connector

# FastAPI backend URL
API_URL = "https://sec-data-filling-fastapi-app-974490277552.us-central1.run.app/"

# Snowflake Connection Function
def get_snowflake_connection():
    return snowflake.connector.connect(
        user="SHUSHIL",
        password="Ganesh@1999",
        account="fipbiwy-mj87522",
        warehouse="sec_wh",
        database="sec_db",
        schema="sec_schema"
    )

# Fetch List of Tables
def get_table_list():
    conn = get_snowflake_connection()
    query = "SHOW TABLES"
    df = pd.read_sql(query, conn)
    conn.close()
    return df["name"].tolist()

# Streamlit App UI
st.title("Quarter Finder API & Snowflake Table Viewer")
st.markdown("Enter a date (YYYY-MM-DD) to find the corresponding year and quarter.")

# Date input field
date_input = st.text_input("Enter Date (YYYY-MM-DD)", "2023-06-15")

# Button to get quarter
if st.button("Get Quarter"):
    if date_input:
        response = requests.post(API_URL, json={"date": date_input})
        
        if response.status_code == 200:
            result = response.json()
            st.success(f"The corresponding year and quarter: **{result['year_quarter']}**")
        else:
            st.error("Invalid date format or out of range (2009-2024). Please try again.")

# Snowflake Table Viewer Section
st.sidebar.title("📊 Snowflake Tables")

tables = get_table_list()
selected_table = st.sidebar.selectbox("Select a Table", tables)

st.subheader(f"📄 Data from {selected_table}")

def fetch_table_data(table_name):
    conn = get_snowflake_connection()
    query = f"SELECT * FROM {table_name} LIMIT 100"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

df_filtered = fetch_table_data(selected_table)
st.dataframe(df_filtered)
