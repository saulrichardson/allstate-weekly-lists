"""Cancellation-specific row cleaner."""

from .generic import clean as generic_clean


def clean(row):
    row = generic_clean(row)
    # Rename verbose consent column to a shorter alias if present
    if "customer_consent_click" in row:
        row["consent_link"] = row.pop("customer_consent_click")
    return row
