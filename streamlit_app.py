import streamlit as st
import os
import requests
import pandas as pd

from data_cleaning_funcs import clean_data, data_quality_summary
from query_Funcs import handle_query


# -----------------------------
# CONFIG
# -----------------------------

API_KEY = st.secrets["MONDAY_API_KEY"]
GROQ_API_KEY = st.secrets["GROQ_API_KEY"]

DEALS_BOARD = 5027136242
WORKORDER_BOARD = 5027136341

MONDAY_URL = "https://api.monday.com/v2"


# -----------------------------
# MONDAY API FETCHING
# -----------------------------

def fetch_board(board_id, trace=None):

    if trace is not None:
        trace.append(f"Calling Monday API for board {board_id}")

    query = f"""
    query {{
      boards(ids: {board_id}) {{
        columns {{
          id
          title
        }}
        items_page {{
          items {{
            id
            name
            column_values {{
              id
              text
            }}
          }}
        }}
      }}
    }}
    """

    headers = {
        "Authorization": API_KEY,
        "Content-Type": "application/json"
    }

    response = requests.post(
        MONDAY_URL,
        json={"query": query},
        headers=headers
    )

    data = response.json()

    board = data["data"]["boards"][0]

    column_map = {
        col["id"]: col["title"]
        for col in board["columns"]
    }

    items = board["items_page"]["items"]

    rows = []

    for item in items:

        row = {
            "item_id": item["id"],
            "deal_name": item["name"]
        }

        for col in item["column_values"]:

            col_id = col["id"]
            col_title = column_map.get(col_id, col_id)

            row[col_title] = col["text"]

        rows.append(row)

    return pd.DataFrame(rows)


def fetch_deals(trace=None):
    return fetch_board(DEALS_BOARD, trace)


def fetch_workorders(trace=None):
    return fetch_board(WORKORDER_BOARD, trace)


# -----------------------------
# DATA PIPELINE
# -----------------------------

def load_live_data(trace):

    trace.append("Fetching live Monday board data")

    deals_raw = fetch_deals(trace)
    workorders_raw = fetch_workorders(trace)

    trace.append("Cleaning datasets")

    deals_clean = clean_data(deals_raw, source="deals")
    workorders_clean = clean_data(workorders_raw, source="workorders")

    trace.append("Running data quality checks")

    dq_deals = data_quality_summary(deals_clean)
    dq_workorders = data_quality_summary(workorders_clean)

    return deals_clean, workorders_clean, dq_deals, dq_workorders

# -----------------------------
# STREAMLIT UI
# -----------------------------

st.set_page_config(
    page_title="Monday BI Agent",
    layout="wide"
)

st.title("📊 Monday.com Business Intelligence Agent")

st.write(
"""
Author: Vamshik M

SR No: 26511

Ask questions about deals, pipeline, revenue, and work orders.

Example queries:
- Show pipeline value by sector
- What is the expected pipeline value?
- Show the top deals
- What is the total executed revenue?
"""
)

# -----------------------------
# QUERY COUNTER
# -----------------------------

if "query_count" not in st.session_state:
    st.session_state.query_count = 0


query = st.text_input("Ask a business question")


if query:

    st.session_state.query_count += 1
    query_number = st.session_state.query_count

    st.markdown(f"### Query #{query_number}")

    trace = []

    with st.spinner("Fetching live data from monday.com..."):

        deals, workorders, dq_deals, dq_workorders = load_live_data(trace)

        result, summary, agent_trace = handle_query(
            query,
            deals,
            workorders
        )

        trace.extend(agent_trace)

    # -----------------------------
    # EXECUTIVE SUMMARY FIRST
    # -----------------------------

    st.subheader("Executive Summary")

    st.success(summary)

    # -----------------------------
    # ANALYTICS RESULT
    # -----------------------------

    st.subheader("Analytics Result")

    if isinstance(result, pd.DataFrame):
        st.dataframe(result, use_container_width=True)
    else:
        st.write(result)

    # -----------------------------
    # OPTIONAL DETAILS
    # -----------------------------

    with st.expander("Data Quality Report"):

        st.write("Deals:", dq_deals)
        st.write("Workorders:", dq_workorders)

    with st.expander("Agent Trace (Debug)"):

        for step in trace:
            st.write("•", step)

    st.markdown(
    """
    ### Source Data
    
    Deals Board  
    https://vamshikmohans-team.monday.com/boards/5027136242
    
    Work Orders Board  
    https://vamshikmohans-team.monday.com/boards/5027136341
    """
    )
