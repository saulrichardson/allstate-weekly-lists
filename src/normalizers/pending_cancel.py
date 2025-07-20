"""
Normalize the Pending Cancel Audit report into canonical task records.
"""

def normalize(df):
    """
    Normalize the Pending Cancel sheet into canonical columns, renaming key fields
    (policy_number, premium_amount, company_code, item_count, pending_cancel_date)
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
        "Renewal Effective Date": "renewal_effective_date",
        # Original names kept; we'll create canonical columns after rename
        "Pending Cancel Date": "pending_cancel_date",
        "Premium New($)": "premium_new",
        # Remaining source columns
        "Status": "status",
        "No. of Items": "item_count",
        # Keep old premium columns for reference
        "Premium Old($)": "premium_old",
        "Premium Old($)": "premium_old",
        "Account Type": "account_type",
        "Company Code": "company_code",
    }
    missing = set(cols) - set(df.columns)
    if missing:
        raise KeyError(f"Pending-cancel normalizer missing columns: {missing}")
    df = df.rename(columns=cols)

    # Derive canonical columns expected by downstream logic
    if "pending_cancel_date" in df.columns:
        import pandas as pd
        df["event_date"] = pd.to_datetime(df["pending_cancel_date"], errors="coerce").dt.date
    if "premium_new" in df.columns:
        import pandas as pd
        df["amount"] = pd.to_numeric(df["premium_new"], errors="coerce")

    return df