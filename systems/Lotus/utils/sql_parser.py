import re

def parse_sql(sql: str, extractions: list) -> tuple[list[int], str, list[int]]:
    """
    Analizza una query SQL per estrarre le colonne SELECT e la clausola WHERE.
    Ritorna: (select_indices, where_clause, attr_indices)
    """
    # Parsing SELECT
    select_match = re.search(r'SELECT (.+?)\s+FROM', sql, re.I)
    select_col = select_match.group(1).strip() if select_match else ""
    select_cols = [c.strip().lower() for c in select_col.split(',')]
    select_indices = [extractions.index(col) for col in select_cols if col in extractions]

    # Parsing WHERE
    where_match = re.search(r'WHERE (.*);', sql, re.I)
    where = where_match.group(1).strip() if where_match else ""

    attr_indices = []
    if where:
        attr_names = re.findall(r'([A-Za-z_][A-Za-z0-9_]*)\s*(?:==|=|!=|<>|>=|<=|>|<)', where)
        attr_indices = [extractions.index(attr.lower()) for attr in attr_names if attr.lower() in extractions]

    return select_indices, where, attr_indices