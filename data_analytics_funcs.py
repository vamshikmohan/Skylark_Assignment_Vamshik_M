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

# analytics.py

def total_pipeline_value(deals):
    return deals["masked_deal_value"].sum()


def expected_pipeline_value(deals):
    return deals["expected_pipeline_value"].sum()


def pipeline_by_sector(deals):
    return deals.groupby("sector_service")["masked_deal_value"].sum()


def pipeline_by_client(deals):
    return deals.groupby("client_code")["masked_deal_value"].sum()


def top_deals(deals, n=5):
    return deals.sort_values(
        "masked_deal_value",
        ascending=False
    ).head(n)


def deals_by_status(deals):
    return deals.groupby("deal_status").size()


def deals_by_owner(deals):
    return deals.groupby("owner_code").size()


def revenue_executed(workorders):
    return workorders[
        "amount_in_rupees_excl_of_gst_masked"
    ].sum()


def revenue_by_sector(workorders):
    return workorders.groupby("sector")[
        "amount_in_rupees_excl_of_gst_masked"
    ].sum()


def workorders_status(workorders):
    return workorders.groupby("execution_status").size()


def billing_status_summary(workorders):
    return workorders.groupby("billing_status").size()


def outstanding_revenue(workorders):
    return (
        workorders["amount_in_rupees_excl_of_gst_masked"].sum()
        - workorders["collected_amount_in_rupees_incl_of_gst_masked"].sum()
    )