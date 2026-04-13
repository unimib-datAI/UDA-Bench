def build_plan(config: dict, parsed_query: dict, query_meta: dict) -> dict:
    tables_config = config["tables"]
    all_tables = list(tables_config.keys())

    extract_fields = {t: [] for t in all_tables}

    # SELECT
    for field in parsed_query["select"]:
        add_field_to_best_table(field, extract_fields, tables_config)

    # AGG
    for agg in parsed_query["aggregations"]:
        field = agg["field"]
        if field != "*":
            add_field_to_best_table(field, extract_fields, tables_config)

    # WHERE
    for cond in parsed_query["where"]:
        field = cond.get("field")
        table = cond.get("table")
        if not field:
            # Complex predicate fallback (e.g. OR expression):
            # include all referenced fields so SQL post-processing can run.
            for raw_field in cond.get("fields", []):
                add_field_to_best_table(raw_field, extract_fields, tables_config)
            continue
        if table and table in extract_fields:
            if field not in extract_fields[table]:
                extract_fields[table].append(field)
        else:
            add_field_to_best_table(field, extract_fields, tables_config)

    # JOIN KEYS
    base_table = parsed_query["from"][0] if parsed_query["from"] else None
    current_left_table = base_table

    join_steps = []
    for join in parsed_query["joins"]:
        right_table = join["right_table"]

        if current_left_table is None:
            continue

        left_key = join["left_key"]
        right_key = join["right_key"]

        if current_left_table in extract_fields and left_key and left_key not in extract_fields[current_left_table]:
            extract_fields[current_left_table].append(left_key)

        if right_table in extract_fields and right_key and right_key not in extract_fields[right_table]:
            extract_fields[right_table].append(right_key)

        join_steps.append({
            "left_table": current_left_table,
            "right_table": right_table,
            "left_key": left_key,
            "right_key": right_key,
        })

        current_left_table = right_table

    return {
        "query_id": query_meta["id"],
        "category": query_meta["category"],
        "sql": query_meta["sql"],
        "tables": parsed_query["from"],
        "extract_fields": {k: v for k, v in extract_fields.items() if v},
        "filters": parsed_query["where"],
        "joins": join_steps,
        "aggregations": parsed_query["aggregations"],
        "group_by": parsed_query["group_by"],
        "order_by": parsed_query["order_by"],
        "limit": parsed_query["limit"],
        "output_fields": parsed_query["select"],
    }


def add_field_to_best_table(field: str, extract_fields: dict, tables_config: dict):
    if not field:
        return
    clean = field.split(".")[-1]
    for table_name, table_cfg in tables_config.items():
        if clean in table_cfg["fields"]:
            if clean not in extract_fields[table_name]:
                extract_fields[table_name].append(clean)
            return
