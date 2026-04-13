import os
import yaml


def build_yaml(
    config: dict,
    plan: dict,
    yaml_output_dir: str,
    json_output_dir: str,
    strict_mode: bool = False,
) -> tuple[str, str]:
    os.makedirs(yaml_output_dir, exist_ok=True)
    os.makedirs(json_output_dir, exist_ok=True)

    datasets = {}
    operations = []
    resolved_docs = config["resolved_documents"]
    table_fields = config["tables"]

    # DATASETS
    for table_name, doc_glob in resolved_docs.items():
        if table_name in plan["extract_fields"]:
            datasets[table_name] = {
                "type": "file",
                "path": doc_glob
            }

    map_names = {}
    steps = []

    # MAP OPS
    for table_name, fields in plan["extract_fields"].items():
        op_name = f"extract_{table_name}"

        schema = {}
        field_lines = []

        for field in fields:
            dtype = table_fields[table_name]["fields"].get(field, "string")
            schema[field] = map_dtype(dtype)
            field_lines.append(f"- {field}")

        prompt = build_extract_prompt(field_lines, schema, strict_mode=strict_mode)

        operations.append({
            "name": op_name,
            "type": "map",
            "prompt": prompt,
            "output": {
                "schema": schema
            },
            "drop_keys": ["text", "content"]
        })

        map_names[table_name] = op_name
        steps.append({
            "name": f"step_extract_{table_name}",
            "input": table_name,
            "operations": [op_name],
        })

    # JOIN CHAIN
    if not map_names:
        raise ValueError(f"Nessun campo da estrarre per query {plan['query_id']}")

    step_for_table = {
        table_name: f"step_extract_{table_name}"
        for table_name in map_names
    }

    if plan["tables"] and plan["tables"][0] in step_for_table:
        current_input = step_for_table[plan["tables"][0]]
        current_alias = plan["tables"][0]
    else:
        first_table = list(step_for_table.keys())[0]
        current_input = step_for_table[first_table]
        current_alias = first_table

    for idx, join in enumerate(plan["joins"], start=1):
        right_table = join["right_table"]
        join_name = f"join_{idx}_{current_alias}_{right_table}"
        join_step_name = f"step_join_{idx}_{current_alias}_{right_table}"
        if right_table not in step_for_table:
            raise ValueError(
                f"Join table non disponibile nel piano: {right_table}"
            )
        right_input = step_for_table[right_table]

        operations.append({
            "name": join_name,
            "type": "equijoin",
            "comparison_prompt": (
                "Determine whether these records should be joined. "
                "Return true only if they represent the same entity/row for the join key.\n\n"
                "Left record: {{ left }}\n"
                "Right record: {{ right }}"
            ),
            "blocking_conditions": [
                f"left.get('{join['left_key']}') == right.get('{join['right_key']}')"
            ],
        })

        steps.append({
            "name": join_step_name,
            "operations": [
                {
                    join_name: {
                        "left": current_input,
                        "right": right_input,
                    }
                }
            ],
        })

        current_input = join_step_name
        current_alias = right_table

    # FILTERS
    # By default we avoid pre-filtering inside DocETL because brittle comparisons
    # on partially extracted fields may drop true positives. Filtering is applied
    # deterministically in SQL post-processing over the extracted table.
    apply_pre_filter = os.environ.get("DOCETL_APPLY_PRE_FILTER", "0") == "1"
    if apply_pre_filter:
        for idx, cond in enumerate(plan["filters"], start=1):
            if "field" not in cond or "op" not in cond:
                continue

            filter_name = f"filter_{idx}"
            filter_step_name = f"step_filter_{idx}"

            operations.append({
                "name": filter_name,
                "type": "code_filter",
                "code": build_code_filter(cond),
            })

            steps.append({
                "name": filter_step_name,
                "input": current_input,
                "operations": [filter_name],
            })

            current_input = filter_step_name

    # Force the pipeline to end on the intended branch.
    finalize_op = "finalize_output"
    operations.append({
        "name": finalize_op,
        "type": "map",
        "drop_keys": ["__docetl_noop__"]
    })
    steps.append({
        "name": "step_finalize_output",
        "input": current_input,
        "operations": [finalize_op],
    })

    yaml_path = os.path.join(yaml_output_dir, f"{plan['query_id']}.yaml")
    json_path = os.path.join(json_output_dir, f"{plan['query_id']}.json")

    pipeline = {
        "default_model": os.environ.get("DOCETL_DEFAULT_MODEL", "gemini/gemini-2.5-flash"),
        "datasets": datasets,
        "operations": operations,
        "pipeline": {
            "steps": steps,
            "output": {
                "type": "file",
                "path": json_path,
            }
        }
    }

    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(pipeline, f, sort_keys=False, allow_unicode=True)

    return yaml_path, json_path


def map_dtype(dtype: str) -> str:
    if dtype in ("int", "float", "number"):
        return "number"
    return "string"


def build_extract_prompt(field_lines: list[str], schema: dict, strict_mode: bool = False) -> str:
    base_rules = [
        "Extract the requested fields from the document.",
        "Return only one valid JSON object (no markdown, no commentary).",
        "Use exactly the listed keys.",
        "If a value is not present, use null.",
    ]
    if strict_mode:
        base_rules.extend(
            [
                "For number fields, return raw numbers only (no currency symbol, no commas, no unit text).",
                "For string fields, return concise plain text without extra explanation.",
                "Do not invent values.",
            ]
        )

    typed_lines = []
    for f in field_lines:
        field = f[2:].strip() if f.startswith("- ") else f.strip()
        field_type = schema.get(field, "string")
        if field_type == "number":
            typed_lines.append(f"- {field}: number")
        else:
            typed_lines.append(f"- {field}: string")

    return (
        "\n".join(base_rules)
        + "\n\nFields:\n"
        + "\n".join(typed_lines)
        + "\n\nDocument:\n{{ input.text }}"
    )


def build_code_filter(cond: dict) -> str:
    field = cond["field"]
    op = cond["op"]
    value = repr(cond["value"])

    if op == "=":
        expr = f"left == {value}"
    elif op == "!=":
        expr = f"left != {value}"
    elif op == ">":
        expr = f"left > {value}"
    elif op == ">=":
        expr = f"left >= {value}"
    elif op == "<":
        expr = f"left < {value}"
    elif op == "<=":
        expr = f"left <= {value}"
    elif op == "IN":
        expr = f"left in {value}"
    elif op == "LIKE":
        return (
            "def transform(doc):\n"
            f"    left = doc.get({field!r})\n"
            "    if left is None:\n"
            "        return False\n"
            f"    pattern = str({value})\n"
            "    token = pattern.replace('%', '').strip()\n"
            "    return token.lower() in str(left).lower()\n"
        )
    elif op == "BETWEEN":
        return (
            "def transform(doc):\n"
            f"    left = doc.get({field!r})\n"
            "    try:\n"
            f"        low, high = {value}\n"
            "        left = float(left)\n"
            "        return left >= float(low) and left <= float(high)\n"
            "    except Exception:\n"
            "        return False\n"
        )
    else:
        return (
            "def transform(doc):\n"
            "    return True\n"
        )

    return (
        "def transform(doc):\n"
        f"    left = doc.get({field!r})\n"
        "    try:\n"
        f"        return bool({expr})\n"
        "    except Exception:\n"
        "        return False\n"
    )
