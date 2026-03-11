from groq import Groq
import pandas as pd
import json
import re
import streamlit as st

GROQ_API_KEY = st.secrets["GROQ_API_KEY"]

client = Groq(api_key=GROQ_API_KEY)
# -----------------------------
# SYSTEM PROMPT
# -----------------------------

SYSTEM_PROMPT = """You are a business intelligence routing agent.

Your task is to select the correct analytics function for answering a founder’s business question.

The company tracks data in two monday.com boards:

--------------------------------
BOARD 1 — Deals (Sales Pipeline)
--------------------------------

Each row represents a sales opportunity.

Important fields include:

deal_name — name of the deal
client_code — client identifier
sector_service — sector of the deal (energy, infrastructure, etc.)
masked_deal_value — total potential deal value
closure_probability — probability of closing
expected_pipeline_value — expected revenue (deal_value × probability)
deal_status — stage of the deal
created_date — when the deal was created
tentative_close_date — expected closing date


--------------------------------
BOARD 2 — Work Orders (Execution / Billing)
--------------------------------

Each row represents a project that has been executed or is currently being delivered.

Important fields include:

deal_name — name of the related deal
sector — sector of the project
execution_status — execution progress
billing_status — billing progress
amount_in_rupees_excl_of_gst_masked — project value
billed_value_in_rupees_excl_of_gst_masked — amount billed so far
collected_amount_in_rupees_incl_of_gst_masked — amount collected
probable_start_date — expected project start
probable_end_date — expected completion


--------------------------------
Available Analytics Functions
--------------------------------

pipeline_by_sector  
→ total deal pipeline value grouped by sector

expected_pipeline  
→ sum of expected pipeline values across deals

top_deals  
→ highest value deals in the pipeline

executed_revenue  
→ total executed project value from work orders

sector_performance  
→ summary of deals by sector including pipeline and expected value

custom_filter_query  
→ keyword search across deals and workorders when a specific project, client, or phrase is mentioned


--------------------------------
Routing Rules
--------------------------------

If the user asks about:

pipeline by sector → pipeline_by_sector

expected revenue / expected pipeline → expected_pipeline

top deals → top_deals

billing, billed value, revenue, invoicing → executed_revenue

sector performance → sector_performance

a specific project, deal, client name, or phrase (example: "proof of concept") → custom_filter_query

If unsure → custom_filter_query


--------------------------------
Output Format (STRICT JSON)
--------------------------------

Return ONLY JSON.

{
 "function": "function_name",
 "parameters": {
   "keyword": "optional keyword if relevant"
 }
}
"""

# -----------------------------
# Utility Functions
# -----------------------------


def safe_json_parse(text):
    """
    Extract JSON safely even if the model returns extra text
    """
    try:
        return json.loads(text)

    except Exception:

        match = re.search(r"\{.*\}", text, re.DOTALL)

        if match:
            return json.loads(match.group())

        raise ValueError("No valid JSON returned from model")


def is_business_query(query):

    business_terms = [
        "deal",
        "pipeline",
        "sector",
        "revenue",
        "billing",
        "client",
        "project",
        "value",
        "workorder"
    ]

    q = query.lower()

    return any(term in q for term in business_terms)


def extract_keyword(query):

    stopwords = {
        "what","is","the","a","an","of",
        "show","give","summary","value",
        "bill","billing","info","get","me"
    }

    words = query.lower().split()

    keywords = [w for w in words if w not in stopwords]

    if not keywords:
        return None

    return " ".join(keywords)

# -----------------------------
# Analytics Functions
# -----------------------------


def pipeline_by_sector(deals, trace):

    trace.append("Running pipeline_by_sector analytics")

    return (
        deals
        .groupby("sector_service")["masked_deal_value"]
        .sum()
        .sort_values(ascending=False)
    )


def expected_pipeline(deals, trace):

    trace.append("Calculating expected pipeline")

    return deals["expected_pipeline_value"].sum()


def top_deals(deals, trace, n=5):

    trace.append("Finding top deals")

    return (
        deals
        .sort_values("masked_deal_value", ascending=False)
        [["deal_name", "client_code", "masked_deal_value"]]
        .head(n)
    )


def executed_revenue(workorders, trace):

    trace.append("Calculating executed revenue")

    if "amount_in_rupees_excl_of_gst_masked" not in workorders.columns:
        return 0

    return workorders["amount_in_rupees_excl_of_gst_masked"].sum()


def sector_performance(deals, trace):

    trace.append("Computing sector performance")

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


def custom_filter_query(deals, workorders, trace, keyword=None):

    trace.append(f"Running keyword search: {keyword}")

    if keyword is None:
        return "No keyword detected."

    keyword = keyword.lower()

    deals_match = deals[
        deals.astype(str).apply(
            lambda col: col.str.lower().str.contains(keyword)
        ).any(axis=1)
    ]

    work_match = workorders[
        workorders.astype(str).apply(
            lambda col: col.str.lower().str.contains(keyword)
        ).any(axis=1)
    ]

    return {
        "matching_deals": deals_match.head(10),
        "matching_workorders": work_match.head(10)
    }

# -----------------------------
# FUNCTION REGISTRY
# -----------------------------

FUNCTION_REGISTRY = {
    "pipeline_by_sector": pipeline_by_sector,
    "expected_pipeline": expected_pipeline,
    "top_deals": top_deals,
    "sector_performance": sector_performance,
    "custom_filter_query": custom_filter_query
}

WORKORDER_FUNCTIONS = {
    "executed_revenue": executed_revenue
}


# -----------------------------
# LLM INFERENCE
# -----------------------------


def infer_function(query):

    completion = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": query}
        ]
    )

    response = completion.choices[0].message.content

    return safe_json_parse(response)


# -----------------------------
# LLM SUMMARY
# -----------------------------


def summarize_result(query, result):

    if isinstance(result, pd.DataFrame):
        result_str = result.to_string()

    else:
        result_str = str(result)

    prompt = f"""
User question:
{query}

Analytics result:
{result_str}

Explain this result in a short executive summary.
"""

    completion = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        temperature=0.3,
        messages=[{"role": "user", "content": prompt}]
    )

    return completion.choices[0].message.content


# -----------------------------
# MAIN QUERY HANDLER
# -----------------------------


def handle_query(query, deals, workorders):

    trace = []

    trace.append("Received user query")

    trace.append("Attempting to route query to analytics function")

    trace.append("Sending query to Groq for routing")

    inference = infer_function(query)

    func_name = inference.get("function")

    keyword = inference.get("parameters", {}).get("keyword")

    trace.append(f"Groq selected function: {func_name}")

    allowed = set(FUNCTION_REGISTRY.keys()) | set(WORKORDER_FUNCTIONS.keys())

    if func_name not in allowed:

        trace.append("Invalid function returned by LLM → fallback to custom search")

        func_name = "custom_filter_query"

    if func_name == "custom_filter_query":

        if keyword is None:
            keyword = extract_keyword(query)

        result = custom_filter_query(deals, workorders, trace, keyword)

    elif func_name in WORKORDER_FUNCTIONS:

        func = WORKORDER_FUNCTIONS[func_name]

        result = func(workorders, trace)

    else:

        func = FUNCTION_REGISTRY[func_name]

        result = func(deals, trace)

    trace.append("Executed analytics function")

    summary = summarize_result(query, result)

    trace.append("Generated executive summary")

    return result, summary, trace
