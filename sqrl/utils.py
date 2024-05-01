def AND(*params):
    clause = " AND ".join(params)
    return clause


def OR(*params):
    clause = " OR ".join(params)
    return clause


def BETWEEN(a, b, c):
    clause = f"{a} BETWEEN {b} AND {c}"
    return clause
