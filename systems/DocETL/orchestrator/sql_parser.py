from sqlglot import parse_one, exp


def parse_sql(sql: str) -> dict:
    tree = parse_one(sql)

    select_fields = []
    aggregations = []
    from_tables = []
    joins = []
    where_conditions = []
    group_by = []
    order_by = []
    limit = None

    # SELECT
    for sel in tree.expressions:
        if isinstance(sel, exp.Alias):
            expr = sel.this
            alias = sel.alias
        else:
            expr = sel
            alias = None

        if isinstance(expr, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
            aggregations.append({
                "func": expr.key.upper(),
                "field": expr.this.sql() if expr.this else "*",
                "alias": alias or expr.sql()
            })
        else:
            select_fields.append(alias or expr.sql())

    # FROM
    from_expr = tree.args.get("from")
    if from_expr:
        for table in from_expr.find_all(exp.Table):
            from_tables.append(table.name)

    # JOIN
    for join in tree.find_all(exp.Join):
        right_table = join.this.name if isinstance(join.this, exp.Table) else join.this.sql()
        on_expr = join.args.get("on")

        left_key = None
        right_key = None

        if isinstance(on_expr, exp.EQ):
            left = on_expr.left.sql()
            right = on_expr.right.sql()

            if "." in left and "." in right:
                left_key = left.split(".")[-1]
                right_key = right.split(".")[-1]

        joins.append({
            "right_table": right_table,
            "left_key": left_key,
            "right_key": right_key,
            "on_sql": on_expr.sql() if on_expr else None
        })

    # WHERE
    where_expr = tree.args.get("where")
    if where_expr:
        flattened = flatten_and_conditions(where_expr.this)
        for cond in flattened:
            parsed = parse_condition(cond)
            if parsed:
                where_conditions.append(parsed)

    # GROUP BY
    group = tree.args.get("group")
    if group:
        for g in group.expressions:
            group_by.append(g.sql())

    # ORDER BY
    order = tree.args.get("order")
    if order:
        for o in order.expressions:
            desc = bool(o.args.get("desc"))
            order_by.append({
                "field": o.this.sql(),
                "desc": desc
            })

    # LIMIT
    lim = tree.args.get("limit")
    if lim and lim.expression:
        try:
            limit = int(lim.expression.this)
        except Exception:
            limit = None

    # Ricostruzione lista tabelle completa
    if joins:
        joined_tables = [j["right_table"] for j in joins]
        from_tables = from_tables + joined_tables

    return {
        "select": select_fields,
        "from": from_tables,
        "where": where_conditions,
        "joins": joins,
        "aggregations": aggregations,
        "group_by": group_by,
        "order_by": order_by,
        "limit": limit,
        "sql": sql
    }


def flatten_and_conditions(expr):
    if isinstance(expr, exp.And):
        return flatten_and_conditions(expr.left) + flatten_and_conditions(expr.right)
    return [expr]


def parse_condition(cond):
    op_map = {
        exp.EQ: "=",
        exp.GT: ">",
        exp.GTE: ">=",
        exp.LT: "<",
        exp.LTE: "<=",
        exp.NEQ: "!=",
    }

    for exp_type, op in op_map.items():
        if isinstance(cond, exp_type):
            left = cond.left.sql()
            right = cond.right.sql()

            field = left.split(".")[-1]
            value = try_parse_literal(right)

            table = left.split(".")[0] if "." in left else None

            return {
                "table": table,
                "field": field,
                "op": op,
                "value": value
            }

    # LIKE / ILIKE
    if isinstance(cond, (exp.Like, exp.ILike)):
        left = cond.this.sql()
        right = cond.expression.sql()
        field = left.split(".")[-1]
        table = left.split(".")[0] if "." in left else None
        return {
            "table": table,
            "field": field,
            "op": "LIKE",
            "value": try_parse_literal(right),
        }

    # BETWEEN
    if isinstance(cond, exp.Between):
        left = cond.this.sql()
        field = left.split(".")[-1]
        table = left.split(".")[0] if "." in left else None
        low = try_parse_literal(cond.args.get("low").sql()) if cond.args.get("low") else None
        high = try_parse_literal(cond.args.get("high").sql()) if cond.args.get("high") else None
        return {
            "table": table,
            "field": field,
            "op": "BETWEEN",
            "value": [low, high],
        }

    # IN (...)
    if isinstance(cond, exp.In):
        left = cond.this.sql()
        field = left.split(".")[-1]
        table = left.split(".")[0] if "." in left else None
        values = []
        for v in cond.expressions:
            values.append(try_parse_literal(v.sql()))
        return {
            "table": table,
            "field": field,
            "op": "IN",
            "value": values,
        }

    # Fallback for complex predicates (e.g. OR / nested expressions):
    # keep raw SQL and expose referenced columns so planner can still
    # extract needed fields for downstream SQL post-processing.
    fields = []
    tables = []
    for col in cond.find_all(exp.Column):
        if col.name and col.name not in fields:
            fields.append(col.name)
        if col.table and col.table not in tables:
            tables.append(col.table)

    return {
        "raw": cond.sql(),
        "fields": fields,
        "tables": tables,
    }


def try_parse_literal(value: str):
    v = value.strip().strip("'").strip('"')
    try:
        if "." in v:
            return float(v)
        return int(v)
    except Exception:
        return v
