import pandas as pd

# -----------------------------
# Analytics Functions
# -----------------------------

def pipeline_by_sector(deals, trace):

    trace.append("Running pipeline_by_sector analytics")

    result = (
        deals
        .groupby("sector_service")["masked_deal_value"]
        .sum()
        .sort_values(ascending=False)
    )

    return result


def expected_pipeline(deals, trace):

    trace.append("Calculating expected pipeline value")

    total = deals["expected_pipeline_value"].sum()

    return total


def top_deals(deals, trace, n=5):

    trace.append("Finding top deals by deal value")

    result = (
        deals
        .sort_values("masked_deal_value", ascending=False)
        [["deal_name", "client_code", "masked_deal_value"]]
        .head(n)
    )

    return result


def executed_revenue(workorders, trace):

    trace.append("Calculating executed revenue from work orders")

    if "amount_in_rupees_excl_of_gst_masked" not in workorders.columns:
        trace.append("Revenue column not found in workorders dataset")
        return 0

    revenue = workorders[
        "amount_in_rupees_excl_of_gst_masked"
    ].sum()

    return revenue


def sector_performance(deals, trace):

    trace.append("Computing sector performance metrics")

    result = (
        deals
        .groupby("sector_service")
        .agg(
            deals_count=("deal_name", "count"),
            pipeline_value=("masked_deal_value", "sum"),
            expected_value=("expected_pipeline_value", "sum")
        )
        .sort_values("pipeline_value", ascending=False)
    )

    return result


# -----------------------------
# Query Understanding Layer
# -----------------------------

def handle_query(query, deals, workorders):

    trace = []

    trace.append("Received query: " + query)

    q = query.lower()

    if "pipeline" in q and "sector" in q:

        trace.append("Intent detected: pipeline_by_sector")

        result = pipeline_by_sector(deals, trace)

        return result, trace


    if "expected pipeline" in q:

        trace.append("Intent detected: expected_pipeline")

        result = expected_pipeline(deals, trace)

        return result, trace


    if "top deals" in q:

        trace.append("Intent detected: top_deals")

        result = top_deals(deals, trace)

        return result, trace


    if "revenue" in q:

        trace.append("Intent detected: executed_revenue")

        result = executed_revenue(workorders, trace)

        return result, trace


    if "sector performance" in q:

        trace.append("Intent detected: sector_performance")

        result = sector_performance(deals, trace)

        return result, trace


    trace.append("No matching intent detected")

    return "Sorry, I could not understand the query.", trace