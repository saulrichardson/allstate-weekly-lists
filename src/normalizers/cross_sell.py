"""
Normalize the Cross Sell Audit report into canonical task records.

This audit does not include premium columns; we derive a canonical
`event_date` from the "Renewal Effective Date" and set `amount` to 0 so
the assignment engine can sort consistently.
"""

from __future__ import annotations

import pandas as pd
import numpy as np
import glob
import os


def get_customer_premium(policy_number: str, data_path: str = "data") -> float:
    """Look up customer's existing premium from renewal data."""
    try:
        # Find renewal audit files
        renewal_files = glob.glob(os.path.join(data_path, "*Renewal*.xlsx"))
        for file in renewal_files:
            df = pd.read_excel(file, header=4)
            if "Policy Number" in df.columns and "Premium New($)" in df.columns:
                match = df[df["Policy Number"] == policy_number]
                if not match.empty:
                    premium = match["Premium New($)"].iloc[0]
                    if pd.notna(premium):
                        return float(premium)
    except:
        pass
    return 0.0


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the Cross Sell sheet into canonical columns while preserving all
    other original columns.

    Expected source columns (header row at index 4 in the Excel sheet):
      - Insured First Name, Insured Last Name, Street Address, City, State,
        Zip Code, Insured Email, Insured Phone
      - Agent#, Policy Number, Original Year
      - Renewal Effective Date
      - Product Code, Product Name
      - Associated ... (various columns kept for context)
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
        "Renewal Effective Date": "renewal_effective_date",
        "Product Code": "product_code",
        "Product Name": "product_name",
        # Associated policy details (normalize to snake_case)
        "Associated Product Code": "associated_product_code",
        "Associated Product Name": "associated_product_name",
        "Associated Policy Number": "associated_policy_number",
        "Associated Original Year": "associated_original_year",
        "Associated Effective Date": "associated_effective_date",
        "Associated Agent#": "associated_agent_number",
        "Associated Insured Name": "associated_insured_name",
        "Associated Insured Street Address": "associated_insured_street_address",
        "Associated Insured City": "associated_insured_city",
        "Associated Insured State": "associated_insured_state",
        "Associated Insured Zip Code": "associated_insured_zip_code",
    }

    # Only rename those present to be resilient to slight layout changes
    present = {k: v for k, v in cols.items() if k in df.columns}
    df = df.rename(columns=present)

    # Canonical fields for downstream logic
    if "renewal_effective_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["renewal_effective_date"], errors="coerce").dt.date

    # Use the customer's existing premium from their current policy for prioritization
    # This ensures high-value customers get assigned to the best closers
    if "policy_number" in df.columns:
        df["amount"] = df["policy_number"].apply(get_customer_premium)
    else:
        df["amount"] = 0

    return df
