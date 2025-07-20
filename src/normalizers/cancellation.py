"""
Normalize the Cancellation Audit report into canonical task records.
"""

def normalize(df):
    """
    Normalize the Cancellation sheet into canonical columns, renaming key fields
    (policy_number, premium_amount, company_code, item_count, cancel_date)
    and preserving all other columns to retain full source data.
    """
    cols = {
        "Last Contact date": "last_contact_date",
        "Number Of Times Contacted": "number_of_times_contacted",
        "Customer Consent": "customer_consent",
        "Click Here To Get Customer Consent": "customer_consent_click",
        "Insured First Name": "first_name",
        "Insured Last Name": "last_name",
        "Street Address": "street_address",
        "City": "city",
        "State": "state",
        "Zip Code": "zip_code",
        "Insured Email": "insured_email",
        "Insured Phone": "insured_phone",
        "Insured Preferred  Phone": "insured_preferred_phone",
        "Agent#": "agent_number",
        "Policy Number": "policy_number",
        "Original Year": "original_year",
        "Product Code": "product_code",
        "Product Name": "product_name",
        "Amount Due($)": "amount_due",
        "Cancel Date": "cancel_date",
        "Status": "status",
        "Premium New($)": "premium_new",
        "Premium Old($)": "premium_old",
        "No. of Items": "item_count",
        "Account Type": "account_type",
        "Company Code": "company_code",
    }
    missing = set(cols) - set(df.columns)
    if missing:
        raise KeyError(f"Cancellation normalizer missing columns: {missing}")
    df = df.rename(columns=cols)
    # Derive canonical columns expected by downstream logic
    if "cancel_date" in df.columns:
        import pandas as pd
        df["event_date"] = pd.to_datetime(df["cancel_date"], errors="coerce").dt.date
    if "premium_new" in df.columns:
        import pandas as pd
        df["amount"] = pd.to_numeric(df["premium_new"], errors="coerce")

    return df