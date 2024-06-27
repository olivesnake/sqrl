"""
Oliver 2024
"""

from typing import List, Dict, Any, Tuple

def AND(*params) -> str:
    clause = " AND ".join(params)
    return clause


def OR(*params) -> str:
    clause = " OR ".join(params)
    return clause


def BETWEEN(a, b, c) -> str:
    clause = f"{a} BETWEEN {b} AND {c}"
    return clause

def process_dict(d: Dict[Any, Any] | None) -> Tuple[List[Any], List[Any]]:
    """
    dictionary helper that splits the key and values into lists
    :param d: a dictionary
    :return: tuple of key list and values list
    """
    if not d:
        return [], []
    keys = list(d.keys())
    vals = list(d.values())
    return keys, vals