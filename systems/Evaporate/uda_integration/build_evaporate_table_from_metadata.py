import argparse
import csv
import json
import re
from pathlib import Path
from collections import defaultdict


def load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected dict JSON in {path}")
    return data


def normalize_doc_id(doc_path: str) -> str:
    normalized = doc_path.replace("\\", "/")
    return normalized.split("/")[-1]


def _clean_text(value: str) -> str:
    s = str(value) if value is not None else ""
    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = s.strip().strip("\"' ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_empty_like(s: str) -> bool:
    return s.lower() in {"", "none", "null", "nan", "n/a", "not available", "not specified"}


def _extract_numbers(s: str) -> list[float]:
    s2 = s.replace(",", "")
    s2 = re.sub(r"(?i)(usd|eur|gbp|aud|cad|jpy)", " ", s2)
    tokens = re.findall(r"-?\d+(?:\.\d+)?", s2)
    nums = []
    for t in tokens:
        try:
            nums.append(float(t))
        except Exception:
            continue
    return nums


def _format_number(x: float) -> str:
    if abs(x - int(x)) < 1e-9:
        return str(int(x))
    return f"{x:.6f}".rstrip("0").rstrip(".")


def _extract_names(s: str, limit: int = 12) -> list[str]:
    # Capitalized human-name-like spans (2-4 tokens)
    pattern = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+){1,3})\b")
    bad = {
        "Board Of Directors",
        "Annual Report",
        "Table Of Contents",
        "Corporate Governance",
        "Financial Statements",
        "Report On",
        "Statement Of",
    }
    out: list[str] = []
    seen = set()
    for m in pattern.finditer(s):
        name = m.group(1).strip()
        if len(name) < 5:
            continue
        if name in bad:
            continue
        if name.lower().startswith("for the "):
            continue
        if name not in seen:
            seen.add(name)
            out.append(name)
        if len(out) >= limit:
            break
    return out


def _normalize_by_attribute(attribute: str, raw_value: str) -> str:
    attr = attribute.lower()
    s = _clean_text(raw_value)
    if _is_empty_like(s):
        return ""

    # Remove common LLM boilerplate/noise.
    low = s.lower()
    noise_markers = [
        "provided text",
        "sample text",
        "does not contain",
        "cannot extract",
        "table of contents",
    ]
    if any(m in low for m in noise_markers):
        return ""

    # Categorical booleans.
    if attr == "major_equity_changes":
        yes_markers = ["yes", "acquisition", "merger", "rights issue", "buyback", "capital raising", "restructuring"]
        no_markers = ["no", "none", "no major changes", "no significant changes"]
        if any(m in low for m in yes_markers):
            return "Yes"
        if any(m in low for m in no_markers):
            return "No"
        return ""

    # Categorical mapping: remuneration policy.
    if attr == "remuneration_policy":
        has_fixed = any(k in low for k in ["fixed salary", "base salary", "fixed remuneration"])
        has_var = any(k in low for k in ["bonus", "incentive", "performance-based", "variable remuneration"])
        has_eq = any(k in low for k in ["equity", "stock option", "share-based", "restricted stock"])
        if has_fixed and (has_var or has_eq):
            return "Mixed"
        if has_fixed and not (has_var or has_eq):
            return "Fixed"
        if has_var and not has_eq:
            return "Performance-based"
        if has_eq and not has_var:
            return "Equity"
        return "Not disclosed"

    # Categorical list mapping: risks.
    if attr == "business_risks":
        mapping = {
            "Market Risk": ["market risk", "price risk", "interest rate risk", "currency risk", "foreign exchange risk"],
            "Credit Risk": ["credit risk", "counterparty risk", "default risk"],
            "Operational Risk": ["operational risk", "process risk", "cyber", "system failure"],
            "Legal/Compliance Risk": ["legal risk", "compliance risk", "regulatory risk", "litigation"],
            "Environmental Risk": ["environmental risk", "climate risk", "esg risk"],
            "Strategic Risk": ["strategic risk", "competition", "business model", "macroeconomic"],
        }
        out = [label for label, kws in mapping.items() if any(k in low for k in kws)]
        return "||".join(out)

    # Categorical list mapping: major events.
    if attr == "major_events":
        mapping = {
            "Major Contract": ["major contract", "contract award", "large contract"],
            "Leadership Change": ["ceo", "cfo", "chairman", "appointed", "resigned", "board change"],
            "Restructuring": ["restructuring", "reorganization", "spin-off", "cost reduction"],
            "M&A": ["acquisition", "merger", "takeover", "disposed", "sale of business"],
            "Litigation": ["litigation", "lawsuit", "legal proceeding"],
            "Other": ["other", "material event", "significant event"],
        }
        out = [label for label, kws in mapping.items() if any(k in low for k in kws)]
        return "||".join(out)

    # Sector-like mapping: principal activities.
    if attr == "principal_activities":
        mapping = {
            "Finance": ["bank", "insurance", "financial", "asset management"],
            "Technology": ["software", "technology", "semiconductor", "digital", "platform"],
            "Manufacturing": ["manufacturing", "industrial", "production", "factory"],
            "Healthcare": ["pharma", "healthcare", "biotech", "medical"],
            "Energy": ["energy", "oil", "gas", "electricity", "power"],
            "Retail": ["retail", "consumer", "stores", "e-commerce"],
            "Real Estate": ["real estate", "property", "reit"],
            "Mining": ["mining", "metals", "resources"],
            "Utilities": ["utility", "water", "gas distribution"],
            "Transportation": ["transport", "logistics", "shipping", "airline"],
        }
        out = [label for label, kws in mapping.items() if any(k in low for k in kws)]
        return "||".join(out)

    # Name-like list fields.
    if attr in {"board_members", "executive_profiles"}:
        names = _extract_names(s)
        return "||".join(names)

    # Auditor normalization.
    if attr == "auditor":
        firm_map = [
            ("Ernst & Young LLP", ["ernst", "ey"]),
            ("KPMG LLP", ["kpmg"]),
            ("Deloitte LLP", ["deloitte"]),
            ("PricewaterhouseCoopers LLP", ["pricewaterhouse", "pwc"]),
            ("Grant Thornton Audit Pty Ltd", ["grant thornton"]),
            ("BDO", ["bdo"]),
        ]
        for canonical, keys in firm_map:
            if any(k in low for k in keys):
                return canonical
        return ""

    # Exchange code.
    if attr == "exchange_code":
        for code in re.findall(r"\b[A-Z]{2,5}\b", s):
            if code not in {"FORM", "REPORT", "TABLE", "BOARD", "ITEM", "NOTE", "CEO", "CFO"}:
                return code
        return ""

    # Address-like field.
    if attr == "registered_office":
        m = re.search(r"\b\d{1,5}\s+[A-Za-z][A-Za-z .'-]+,\s*[A-Za-z][A-Za-z .'-]+", s)
        if m:
            return _clean_text(m.group(0))
        return ""

    # Numeric-like fields.
    numeric_markers = [
        "revenue", "profit", "cost", "asset", "debt", "cash",
        "earnings_per_share", "dividend_per_share", "stake", "segments_num",
    ]
    if any(m in attr for m in numeric_markers):
        nums = _extract_numbers(s)
        if not nums:
            return ""
        if "segments_num" in attr:
            ints = [int(round(n)) for n in nums if 0 <= n <= 30]
            return str(ints[0]) if ints else ""
        if "stake" in attr:
            candidates = [n for n in nums if 0 <= n <= 100]
            if candidates:
                return _format_number(candidates[0])
            return ""
        if "per_share" in attr:
            candidates = [n for n in nums if -1000 <= n <= 1000]
            if candidates:
                return _format_number(candidates[0])
            return ""
        # Totals/income/cost style: choose max magnitude.
        best = max(nums, key=lambda x: abs(x))
        return _format_number(best)

    # Company name fallback (concise title-like span).
    if attr == "company_name":
        m = re.search(r"\b([A-Z][A-Za-z0-9&.,' -]{2,80}\b(?:Ltd|Limited|Inc\.?|LLC|PLC|Corp\.?|Corporation|Group|Holdings))\b", s)
        if m:
            return _clean_text(m.group(1))
        return _clean_text(s[:90])

    # Default: keep concise value only.
    if len(s) > 140:
        s = s[:140].rsplit(" ", 1)[0].strip()
    return s


def extract_attribute_name(file_path: Path) -> str:
    name = file_path.name
    match = re.search(r"_([^_]+(?:_[^_]+)*)_file2metadata\.json$", name)
    if not match:
        raise ValueError(f"Cannot extract attribute name from filename: {name}")

    suffix = match.group(1)
    parts = suffix.split("_")

    config_like = {"fs100", "ts1", "ts5", "k1", "cs2000", "rt0", "b0", "c0", "ub1", "m1"}
    attr_start = 0
    for i, part in enumerate(parts):
        if part in config_like or re.fullmatch(r"[a-z]+\d+", part):
            attr_start = i + 1

    attribute = "_".join(parts[attr_start:]) if attr_start < len(parts) else suffix
    return attribute


def find_metadata_files(input_dir: Path, run_prefix: str | None) -> list[Path]:
    files = sorted(input_dir.glob("*_file2metadata.json"))
    if run_prefix:
        files = [f for f in files if f.name.startswith(run_prefix)]
    if not files:
        raise FileNotFoundError(
            f"No *_file2metadata.json files found in {input_dir} "
            f"with run_prefix={run_prefix!r}"
        )
    return files


def build_table(input_dir: Path, run_prefix: str | None):
    metadata_files = find_metadata_files(input_dir, run_prefix)

    table = defaultdict(dict)
    attributes = []

    print("\n=== Processing metadata files ===")
    for file_path in metadata_files:
        attribute = extract_attribute_name(file_path)
        data = load_json(file_path)

        attributes.append(attribute)

        for doc_path, value in data.items():
            doc_id = normalize_doc_id(doc_path)
            if value is None:
                value = ""
            table[doc_id][attribute] = _normalize_by_attribute(attribute, str(value))

        print(f"- {file_path.name}")
        print(f"  -> attribute: {attribute}")
        print(f"  -> documents: {len(data)}")

    return sorted(set(attributes)), table


def write_csv(output_path: Path, attributes: list[str], table: dict[str, dict[str, str]]):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    columns = ["doc_id"] + attributes

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)

        for doc_id in sorted(table.keys()):
            row = [doc_id] + [table[doc_id].get(attr, "") for attr in attributes]
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(
        description="Build document-level CSV table from Evaporate *_file2metadata.json outputs."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing *_file2metadata.json files"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output CSV"
    )
    parser.add_argument(
        "--run-prefix",
        required=False,
        default=None,
        help="Optional run prefix, e.g. dlfinance_d04032026_fs100_ts5_k1_cs2000_rt0_b0_c0_ub1_m1"
    )

    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_path = Path(args.output)
    run_prefix = args.run_prefix

    attributes, table = build_table(input_dir, run_prefix)
    write_csv(output_path, attributes, table)

    print("\n=== Final table completed ===")
    print(f"Attributes consolidated: {len(attributes)}")
    print(f"Documents in final table: {len(table)}")
    print(f"Output CSV: {output_path}")


if __name__ == "__main__":
    main()
