import requests
import pandas as pd
from data_cleaning_funcs import clean_data, report_missing_values, data_quality_summary
from query_Funcs import handle_query

API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjYzMTU3NDIxNSwiYWFpIjoxMSwidWlkIjoxMDA4NjQ2NjMsImlhZCI6IjIwMjYtMDMtMTFUMDU6NDQ6NDQuMDMyWiIsInBlciI6Im1lOndyaXRlIiwiYWN0aWQiOjM0MTcyMDUyLCJyZ24iOiJhcHNlMiJ9.-kxk3CgnGKYxHtwDugjBwEujPEGTRf874BF03SYNGvg"

DEALS_BOARD = 5027136242
WORKORDER_BOARD = 5027136341

URL = "https://api.monday.com/v2"


def fetch_board(board_id):

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

    response = requests.post(URL, json={"query": query}, headers=headers)

    data = response.json()

    board = data["data"]["boards"][0]

    # Map column ID → real column name
    column_map = {col["id"]: col["title"] for col in board["columns"]}

    items = board["items_page"]["items"]

    rows = []

    for item in items:

        row = {
            "item_id": item["id"],
            "deal_name": item["name"]
        }

        for col in item["column_values"]:
            col_id = col["id"]

            # Convert ID to actual column name
            col_title = column_map.get(col_id, col_id)

            row[col_title] = col["text"]

        rows.append(row)

    return pd.DataFrame(rows)


def fetch_deals():
    return fetch_board(DEALS_BOARD)


def fetch_workorders():
    return fetch_board(WORKORDER_BOARD)


if __name__ == "__main__":

    print("\nFetching Monday boards...")

    deals = fetch_deals()
    workorders = fetch_workorders()

    print("Cleaning datasets...")

    deals_clean = clean_data(deals, source="deals")
    workorders_clean = clean_data(workorders, source="workorders")

    print("\nData Quality Report:")
    print(data_quality_summary(deals_clean))
    print(data_quality_summary(workorders_clean))

    # -----------------------------
    # Simulated Agent Query
    # -----------------------------

    query = input("\nAsk a business question: ")

    result, trace = handle_query(query, deals_clean, workorders_clean)

    print("\n=== RESULT ===")
    print(result)

    print("\n=== AGENT TRACE ===")

    for step in trace:
        print("•", step)