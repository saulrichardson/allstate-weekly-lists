"""
Build predicate functions from configuration.
"""

import operator

OPS = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
    "in": lambda series, vals: series.isin(vals),
    "between": lambda val, rng: val >= rng["low"] and val <= rng["high"],
}


def build_predicate(pred_cfg):
    """
    Return a function f(row: pandas.Series) -> bool based on pred_cfg.
    Supports:
      - falsy cfg: always True
      - single dict: one condition (field, op, value)
      - list of dicts: all conditions must be True (logical AND)
    """
    if not pred_cfg:
        return lambda row: True

    if isinstance(pred_cfg, list):
        preds = [build_predicate(cfg) for cfg in pred_cfg]
        return lambda row: all(p(row) for p in preds)

    field = pred_cfg.get("field")
    op_name = pred_cfg.get("op")
    if op_name == "between":
        low, high = pred_cfg.get("low"), pred_cfg.get("high")
        return lambda row: row.get(field) is not None and OPS[op_name](row.get(field), {"low": low, "high": high})
    return lambda row: row.get(field) is not None and OPS[op_name](row.get(field), pred_cfg.get("value"))