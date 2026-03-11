import os
import requests
import pandas as pd

from data_cleaning_funcs import clean_data, data_quality_summary
from query_Funcs import handle_query


# -----------------------------
# CONFIG
# -----------------------------

API_KEY = "API_KEY_Put_in_streamlit_secrets_VamshikM"
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

    try:

        response = requests.post(
            MONDAY_URL,
            json={"query": query},
            headers=headers
        )

        response.raise_for_status()

        data = response.json()

    except Exception as e:
        raise RuntimeError(f"Monday API error: {e}")

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
# INTERACTIVE AGENT LOOP
# -----------------------------

def run_agent():

    print("\n==============================")
    print(" Monday.com BI Agent")
    print("==============================")

    print("\nType 'exit' to quit")

    while True:

        query = input("\nQuestion: ")

        if query.lower() == "exit":
            print("Exiting agent.")
            break

        trace = []

        try:

            # --------------------------------
            # LIVE DATA FETCH
            # --------------------------------

            deals, workorders, dq_deals, dq_workorders = load_live_data(trace)

            # --------------------------------
            # QUERY HANDLING
            # --------------------------------

            result, summary, agent_trace = handle_query(
                query,
                deals,
                workorders
            )

            trace.extend(agent_trace)

            # --------------------------------
            # OUTPUT
            # --------------------------------

            print("\n=== ANALYTICS RESULT ===")

            if isinstance(result, pd.DataFrame):
                print(result.to_string(index=False))
            else:
                print(result)

            print("\n=== EXECUTIVE SUMMARY ===")
            print(summary)

            print("\n=== DATA QUALITY ===")
            print("Deals:", dq_deals)
            print("Workorders:", dq_workorders)

            print("\n=== AGENT TRACE ===")

            for step in trace:
                print("•", step)

        except Exception as e:

            print("\nAgent encountered an error:")
            print(str(e))


# -----------------------------
# MAIN ENTRY
# -----------------------------

if __name__ == "__main__":

    run_agent()