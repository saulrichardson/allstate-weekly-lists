"""
Normalize the Renewal Audit report into canonical task records.
"""

def normalize(df):
    """
    Normalize the Renewal sheet into canonical columns, renaming key fields
    (policy_number, premium_amount, company_code, item_count, renewal_effective_date)
    and preserving all other columns to retain full source data.
    """
    cols = {
        "Insured First Name": "first_name",
        "Insured Last Name": "last_name",
        "Street Address": "street_address",
        "City": "city",
        "State": "state",
        "Zip Code": "zip_code",
        "Insured Email": "insured_email",
        "Insured Phone": "insured_phone",
        "Agent#": "agent_number",
        "Policy Number": "policy_number",
        "Original Year": "original_year",
        "Product Code": "product_code",
        "Product Name": "product_name",
        "Amount Due($)": "amount_due",
        "Renewal Issue Date": "renewal_issue_date",
        "Renewal Status": "renewal_status",
        "Renewal Effective Date": "renewal_effective_date",
        "Anniversary Effective Date": "anniversary_effective_date",
        "Status": "status",
        "Premium New($)": "premium_new",
        "Premium Old($)": "premium_old",
        "Premium Change($)": "premium_change_dollars",
        "Premium Change(%)": "premium_change_percent",
        "Easy Pay": "easy_pay",
        "Option Package": "option_package",
        "Cede Code": "cede_code",
        "Account Type": "account_type",
        "Company Code": "company_code",
        "Multi-line Indicator": "multi_line_indicator",
        "Item Count": "item_count",
        "Years Prior Insurance": "years_prior_insurance",
    }
    missing = set(cols) - set(df.columns)
    if missing:
        raise KeyError(f"Renewal normalizer missing columns: {missing}")
    df = df.rename(columns=cols)
    # Derive canonical columns expected by downstream logic
    if "renewal_effective_date" in df.columns:
        import pandas as pd
        df["event_date"] = pd.to_datetime(df["renewal_effective_date"], errors="coerce").dt.date
    if "premium_new" in df.columns:
        import pandas as pd
        df["amount"] = pd.to_numeric(df["premium_new"], errors="coerce")

    return df