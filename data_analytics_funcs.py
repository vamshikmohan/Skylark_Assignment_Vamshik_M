def pipeline_by_sector(deals):

    result = (
        deals
        .groupby("sector_service")["masked_deal_value"]
        .sum()
        .sort_values(ascending=False)
    )

    return result

def expected_pipeline(deals):

    total = deals["expected_pipeline_value"].sum()

    return total

def expected_pipeline(deals):

    total = deals["expected_pipeline_value"].sum()

    return total

def top_deals(deals, n=5):

    return (
        deals
        .sort_values("masked_deal_value", ascending=False)
        [["deal_name", "client_code", "masked_deal_value"]]
        .head(n)
    )

def executed_revenue(workorders):

    return workorders[
        "amount_in_rupees_excl_of_gst_masked"
    ].sum()

def sector_performance(deals):

    return (
        deals
        .groupby("sector_service")
        .agg(
            deals_count=("deal_name", "count"),
            pipeline_value=("masked_deal_value", "sum"),
            expected_value=("expected_pipeline_value", "sum")
        )
        .sort_values("pipeline_value", ascending=False)
    )