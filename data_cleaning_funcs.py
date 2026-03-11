import pandas as pd


# -----------------------------
# Utility Functions
# -----------------------------

def normalize_columns(df):

    df = df.copy()

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("(", "")
        .str.replace(")", "")
    )

    return df


def replace_empty_strings(df):

    return df.replace("", None)


def normalize_text_columns(df, columns):

    for col in columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .fillna("unknown")
                .astype(str)
                .str.strip()
                .str.lower()
            )

    return df


def convert_numeric_columns(df, columns):

    for col in columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "")
            )

            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def convert_date_columns(df, columns):

    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def add_probability_column(df):

    probability_map = {
        "high": 0.8,
        "medium": 0.5,
        "low": 0.2
    }

    if "closure_probability" in df.columns:

        df["closure_probability_numeric"] = (
            df["closure_probability"]
            .str.lower()
            .map(probability_map)
            .fillna(0.3)
        )

    return df


def create_expected_pipeline(df):

    if "masked_deal_value" in df.columns and "closure_probability_numeric" in df.columns:

        df["expected_pipeline_value"] = (
            df["masked_deal_value"] * df["closure_probability_numeric"]
        )

    return df


def create_unique_deal_key(df):

    if "deal_name" in df.columns and "client_code" in df.columns:

        df["deal_key"] = df["deal_name"] + "_" + df["client_code"]

    return df


def report_missing_values(df):

    missing = df.isna().sum()

    return missing[missing > 0]


# -----------------------------
# MASTER CLEANING FUNCTION
# -----------------------------

def clean_data(df, source="generic"):

    df = df.copy()

    # Normalize column names
    df = normalize_columns(df)

    # Replace blank strings
    df = replace_empty_strings(df)

    # Source specific logic
    if source == "deals":

        df = normalize_text_columns(df, [
            "sector_service",
            "deal_status",
            "closure_probability"
        ])

        df = convert_numeric_columns(df, [
            "masked_deal_value"
        ])

        df = convert_date_columns(df, [
            "created_date",
            "tentative_close_date",
            "close_date_a"
        ])

        df = add_probability_column(df)

        df = create_expected_pipeline(df)

        df = create_unique_deal_key(df)


    elif source == "workorders":

        df = normalize_text_columns(df, [
            "sector",
            "execution_status",
            "billing_status"
        ])

        df = convert_numeric_columns(df, [
            "amount_in_rupees_excl_of_gst_masked",
            "billed_value_in_rupees_excl_of_gst_masked",
            "collected_amount_in_rupees_incl_of_gst_masked"
        ])

        df = convert_date_columns(df, [
            "probable_start_date",
            "probable_end_date",
            "collection_date"
        ])

    return df

def data_quality_summary(df):

    missing = df.isna().sum()
    issues = missing[missing > 0]

    if len(issues) == 0:
        return "No major data quality issues."

    return f"Data quality issues detected: {issues.to_dict()}"