import re
import argparse
import random
import os
import json
from bs4 import BeautifulSoup
from collections import Counter, defaultdict

# NEW ADDITION: Comprehensive synonym dictionary for finance dataset attributes
# This was not present in the old version
ATTRIBUTE_SYNONYMS = {
   # Finance dataset attributes
    "company_name": [
        "company", "corporation", "limited", "ltd", "pty", "inc", "incorporated", "technologies",
        "group", "holdings", "enterprise", "business", "firm", "entity", "organization",
        r"\b([A-Z][a-z]+ Ltd)\b", r"\b([A-Z][a-z]+ Limited)\b", r"\b([A-Z][a-z]+ Pty Ltd)\b",
        r"\b([A-Z][a-z]+ Corporation)\b", r"\b([A-Z][a-z]+ Technologies)\b", r"\b([A-Z][a-z]+ Group)\b"
    ],
    "registered_office": [
        "registered office", "head office", "principal office", "main office", "corporate office",
        "address", "location", "headquarters", "domicile", "registered address",
        r"\b(\d+ [A-Z][a-z]+ Street)\b", r"\b([A-Z][a-z]+, [A-Z][A-Z] \d+)\b",
        r"\b(Suite \d+)\b", r"\b(Level \d+)\b", r"\b(PO Box \d+)\b"
    ],
    "exchange_code": [
        "exchange", "stock exchange", "listed", "asx", "nasdaq", "nyse", "lse", "trading",
        "ticker", "symbol", "code", "market", "securities exchange",
        r"\b(ASX)\b", r"\b(NYSE)\b", r"\b(NASDAQ)\b", r"\b(LSE)\b", r"\b([A-Z]{3,4})\b"
    ],
    "principal_activities": [
        "principal activities", "main activities", "business activities", "operations",
        "industry", "sector", "business", "activities", "services", "products",
        "mining", "finance", "healthcare", "manufacturing", "technology", "retail",
        "energy", "utilities", "real estate", "transportation", "agriculture",
        "telecommunications", "media", "other"
    ],
    "board_members": [
        "board", "directors", "board of directors", "board members", "director",
        "chairman", "chairperson", "non-executive", "executive", "independent",
        r"\b(Mr\. [A-Z][a-z]+ [A-Z][a-z]+)\b", r"\b(Ms\. [A-Z][a-z]+ [A-Z][a-z]+)\b",
        r"\b(Chairman)\b", r"\b(Director)\b", r"\b(Board Member)\b"
    ],
    "executive_profiles": [
        "executive", "management", "executive team", "senior management", "ceo", "cfo",
        "coo", "managing director", "chief", "officer", "manager", "executive management",
        r"\b(CEO)\b", r"\b(CFO)\b", r"\b(COO)\b", r"\b(Chief Executive Officer)\b",
        r"\b(Managing Director)\b", r"\b(Executive)\b"
    ],
    "revenue": [
        "revenue", "income", "sales", "turnover", "gross income", "operating revenue",
        "total revenue", "net sales", "receipts", "earnings",
        r"\b(\$[\d,]+)\b", r"\b(\d+,\d+)\b", r"\b(million)\b", r"\b(thousand)\b"
    ],
    "net_profit_or_loss": [
        "net profit", "net loss", "profit", "loss", "net income", "earnings",
        "profit after tax", "net result", "bottom line", "net earnings",
        r"\b(\$[\d,]+)\b", r"\b(\(\$[\d,]+\))\b", r"\b(loss)\b", r"\b(profit)\b"
    ],
    "total_debt": [
        "total debt", "debt", "liabilities", "borrowings", "loans", "total liabilities",
        "financial debt", "interest bearing debt", "bank loans", "credit facilities",
        r"\b(\$[\d,]+)\b", r"\b(debt)\b", r"\b(loan)\b", r"\b(borrowing)\b"
    ],
    "total_assets": [
        "total assets", "assets", "total resources", "balance sheet total",
        "current assets", "non-current assets", "fixed assets", "investments",
        r"\b(\$[\d,]+)\b", r"\b(assets)\b", r"\b(property)\b", r"\b(equipment)\b"
    ],
    "cash_reserves": [
        "cash", "cash equivalents", "cash reserves", "liquid assets", "bank balances",
        "short-term deposits", "money market", "treasury", "cash on hand",
        r"\b(\$[\d,]+)\b", r"\b(cash)\b", r"\b(deposits)\b", r"\b(liquid)\b"
    ],
    "net_assets": [
        "net assets", "equity", "shareholders equity", "net worth", "total equity",
        "stockholders equity", "owners equity", "capital", "retained earnings",
        r"\b(\$[\d,]+)\b", r"\b(equity)\b", r"\b(net worth)\b", r"\b(capital)\b"
    ],
    "earnings_per_share": [
        "earnings per share", "eps", "basic eps", "diluted eps", "per share earnings",
        "share earnings", "earnings ratio", "profit per share",
        r"\b(\$?[\d.]+)\b", r"\b(cents)\b", r"\b(per share)\b", r"\b(EPS)\b"
    ],
    "dividend_per_share": [
        "dividend", "dividend per share", "dividends paid", "dividend declared",
        "dividend payment", "interim dividend", "final dividend", "special dividend",
        r"\b(\$?[\d.]+)\b", r"\b(cents)\b", r"\b(dividend)\b", r"\b(per share)\b"
    ],
    "largest_shareholder": [
        "largest shareholder", "major shareholder", "principal shareholder", "controlling shareholder",
        "substantial shareholder", "significant shareholder", "top shareholder", "main investor",
        r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", r"\b([A-Z][a-z]+ Group)\b", r"\b([A-Z][a-z]+ Holdings)\b"
    ],
    "the_highest_ownership_stake": [
        "ownership", "shareholding", "stake", "percentage", "percent", "holding",
        "interest", "share", "equity interest", "voting rights",
        r"\b(\d+\.?\d*%)\b", r"\b(\d+\.?\d* percent)\b", r"\b(\d+\.?\d* per cent)\b"
    ],
    "major_equity_changes": [
        "equity changes", "capital changes", "share issues", "share buyback",
        "rights issue", "capital raising", "merger", "acquisition", "restructuring",
        "yes", "no", "major changes", "significant changes"
    ],
    "major_events": [
        "major events", "significant events", "corporate events", "material events",
        "m&a", "merger", "acquisition", "litigation", "lawsuit", "major contract",
        "leadership change", "restructuring", "delisting", "other", "material matters"
    ],
    "bussiness_sales": [
        "business sales", "segment revenue", "division sales", "operating segments",
        "business lines", "revenue breakdown", "sales by segment", "operating revenue",
        r"\b(\$[\d,]+)\b", r"\b(segment)\b", r"\b(division)\b", r"\b(business unit)\b"
    ],
    "bussiness_profit": [
        "business profit", "segment profit", "division profit", "operating profit",
        "segment earnings", "profit by segment", "divisional profit", "business unit profit",
        r"\b(\$[\d,]+)\b", r"\b(segment)\b", r"\b(profit)\b", r"\b(earnings)\b"
    ],
    "bussiness_cost": [
        "business cost", "segment cost", "operating costs", "cost of sales",
        "divisional costs", "business expenses", "segment expenses", "operating expenses",
        r"\b(\$[\d,]+)\b", r"\b(cost)\b", r"\b(expense)\b", r"\b(expenditure)\b"
    ],
    "business_segments_num": [
        "business segments", "operating segments", "reportable segments", "divisions",
        "business units", "segment reporting", "number of segments",
        r"\b(\d+)\b", r"\b(segment)\b", r"\b(division)\b", r"\b(business unit)\b"
    ],
    "business_risks": [
        "business risks", "risk factors", "principal risks", "key risks", "material risks",
        "market risk", "credit risk", "operational risk", "legal risk", "compliance risk",
        "environmental risk", "strategic risk", "other", "risk management"
    ],
    "remuneration_policy": [
        "remuneration", "compensation", "executive pay", "director fees", "salary",
        "fixed", "performance-based", "stock option", "equity", "mixed", "incentive",
        "bonus", "not disclosed", "remuneration policy", "pay structure"
    ],
    "auditor": [
        "auditor", "audit firm", "external auditor", "independent auditor", "accountant",
        "audit", "pwc", "deloitte", "kpmg", "ey", "ernst & young", "grant thornton",
        r"\b([A-Z][a-z]+ [A-Z][a-z]+ Audit)\b", r"\b([A-Z][a-z]+ Thornton)\b"
    ]
}


def set_profiler_args(profiler_args):

    parser = argparse.ArgumentParser(
        "LLM profiler.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--chunk_size",
        type=int,
        default=5000
    )

    parser.add_argument(
        "--train_size",
        type=int,
        default=15,
    )

    parser.add_argument(
        "--eval_size",
        type=int,
        default=15,
    )

    parser.add_argument(
        "--max_chunks_per_file",
        type=int,
        default=-1,
    )

    parser.add_argument(
        "--num_top_k_scripts",
        type=int,
        default=1,
        help="of all the scripts we generate for the metadata fields, number to retain after scoring their qualities",
    )

    parser.add_argument(
        "--extraction_fraction_thresh",
        type=int,
        default=0.9,
        help="for abstensions approach",
    )

    parser.add_argument(
        "--remove_tables",
        type=bool,
        default=False,
        help="Remove tables from the html files?",
    )

    parser.add_argument(
        "--body_only",
        type=bool,
        default=False,
        help="Only use HTML body",
    )

    parser.add_argument(
        "--max_metadata_fields",
        type=int,
        default=15,
    )

    parser.add_argument(
        "--use_dynamic_backoff",
        type=bool,
        default=True,
        help="Whether to do the function generation workflow or directly extract from chunks",
    )

    parser.add_argument(
        "--use_qa_model",
        type=bool,
        default=False,
        help="Whether to apply the span-extractor QA model.",
    )

    parser.add_argument(
        "--overwrite_cache",
        type=int,
        default=0,
        help="overwrite the manifest cache"
    )

    parser.add_argument(
        "--MODELS",
        type=list,
        help="models to use in the pipeline"
    )

    parser.add_argument(
        "--KEYS",
        type=list,
        help="keys for openai models"
    )

    parser.add_argument(
        "--GOLDKEY",
        type=str,
        help="models to use in the pipeline"
    )

    parser.add_argument(
        "--MODEL2URL",
        type=dict,
        default={"llama-3-1-70B": "https://aihubmix.com/v1"},
        help="models to use in the pipeline"
    )

    parser.add_argument(
        "--swde_plus",
        type=bool,
        default=False,
        help="Whether to use the extended SWDE dataset to measure OpenIE performance",
    )

    parser.add_argument(
        "--schema_id_sizes",
        type=int,
        default=0,
        help="Number of documents to use for schema identification stage, if it differs from extraction",
    )

    parser.add_argument(
        "--slice_results",
        type=bool,
        default=False,
        help="Whether to measure the results by attribute-slice",
    )

    parser.add_argument(
        "--fn_generation_prompt_num",
        type=int,
        default=-1,
        help="For ablations on function generation with diversity, control which prompt we use. Default is all.",
    )

    parser.add_argument(
        "--upper_bound_fns",
        type=bool,
        default=False,
        help="For ablations that select functions using ground truth instead of the FM.",
    )

    parser.add_argument(
        "--combiner_mode",
        type=str,
        default='mv',
        help="For ablations that select functions using ground truth instead of the FM.",
    )

    parser.add_argument(
        "--use_alg_filtering",
        type=str,
        default=True,
        help="Whether to filter functions based on quality.",
    )

    parser.add_argument(
        "--use_abstension",
        type=str,
        default=True,
        help="Whether to use the abstensions approach.",
    )

    args = parser.parse_args(args=[])
    for arg, val in profiler_args.items():
        setattr(args, arg, val)
    return args


#################### GET SOME SAMPLE FILES TO SEED THE METADATA SEARCH #########################

def sample_scripts(files, train_size=5):
    import os
    import re

    def extract_number(filepath):
        name = os.path.basename(filepath)
        match = re.search(r'(\d+)\.txt$', name)
        return int(match.group(1)) if match else float('inf')

    sorted_files = sorted(files, key=extract_number)
    sample_files = sorted_files[:train_size]
    print(f"Selected training files: {[os.path.basename(f) for f in sample_files]}")
    return sample_files


#################### BOILERPLATE CHUNKING CODE, CRITICAL FOR LONG SEUQENCES ####################
def chunk_file(
    parser, file, chunk_size=5000, mode="train", remove_tables=False, body_only=False
):
    content = get_file_contents(file)
    if "html" in parser:
        content, chunks = get_html_parse(
            content,
            chunk_size=chunk_size,
            mode=mode,
            remove_tables=remove_tables,
            body_only=body_only
        )
    else:
        content, chunks = get_txt_parse(content, chunk_size=chunk_size, mode=mode)
    return content, chunks


# HTML --> CHUNKS
def clean_html(content):
    for tag in ['script', 'style', 'svg']:
        content = content.split("\n")
        clean_content = []
        in_script = 0
        for c in content:
            if c.strip().strip("\t").startswith(f"<{tag}"):
                in_script = 1
            endstr = "</" + tag
            if endstr in c or "/>" in c:
                in_script = 0
            if not in_script:
                clean_content.append(c)
        content = "\n".join(clean_content)
    return content


def get_flattened_items(content, chunk_size=500):
    flattened_divs = str(content).split("\n")
    flattened_divs = [ch for ch in flattened_divs if ch.strip() and ch.strip("\n").strip()]

    clean_flattened_divs = []
    for div in flattened_divs:
        if len(str(div)) > chunk_size:
            sub_divs = div.split("><")
            if len(sub_divs) == 1:
                clean_flattened_divs.append(div)
            else:
                clean_flattened_divs.append(sub_divs[0] + ">")
                for sd in sub_divs[1:-1]:
                    clean_flattened_divs.append("<" + sd + ">")
                clean_flattened_divs.append("<" + sub_divs[-1])
        else:
            clean_flattened_divs.append(div)
    return clean_flattened_divs


def get_html_parse(content, chunk_size=5000, mode="train", remove_tables=False, body_only=False):
    clean_flattened_divs = []

    if remove_tables:
        soup = BeautifulSoup(content)
        tables = soup.find_all("table")
        for table in tables:
            if "infobox" not in str(table):
                content = str(soup)
                content = content.replace(str(table), "")
                soup = BeautifulSoup(content)

    if body_only:
        soup = BeautifulSoup(content)
        body = soup.find("body")
        content = str(body) if body is not None else content
        flattened_divs = get_flattened_items(content, chunk_size=chunk_size)

        for i, div in enumerate(flattened_divs):
            new_div = re.sub(r'style="[^"]*"', '', str(div))
            new_div = re.sub(r'<style>.*?</style>', '', str(new_div))
            new_div = re.sub(r'<style.*?/style>', '', str(new_div))
            new_div = re.sub(r'<meta.*?/>', '', str(new_div))
            new_div = "\n".join([l for l in new_div.split("\n") if l.strip() and l.strip("\n").strip()])
            if new_div:
                clean_flattened_divs.append(new_div)

    else:
        content = clean_html(content)
        flattened_divs = get_flattened_items(content, chunk_size=chunk_size)
        for i, div in enumerate(flattened_divs):
            new_div = re.sub(r'style="[^"]*"', '', str(div))
            new_div = re.sub(r'<style>.*?</style>', '', str(new_div))
            new_div = re.sub(r'<style.*?/style>', '', str(new_div))
            new_div = re.sub(r'<meta.*?/>', '', str(new_div))
            new_div = "\n".join([l for l in new_div.split("\n") if l.strip() and l.strip("\n").strip()])
            if new_div:
                clean_flattened_divs.append(new_div)

    if mode == "eval":
        return content, []

    grouped_divs = []
    current_div = []
    current_length = 0
    max_length = chunk_size
    use_raw_text = False
    join_str = " " if use_raw_text else "\n"

    for div in clean_flattened_divs:
        str_div = str(div)
        len_div = len(str_div)
        if (current_length + len_div > max_length):
            grouped_divs.append(join_str.join(current_div))
            current_div = []
            current_length = 0
        elif not current_div and (current_length + len_div > max_length):
            grouped_divs.append(str_div)
            continue
        current_div.append(str_div)
        current_length += len_div

    if current_div:
        grouped_divs.append(join_str.join(current_div))

    return content, grouped_divs


# GENERIC TXT --> CHUNKS
def get_txt_parse(content, chunk_size=5000, mode="train"):
    if mode == "train":
        chunks = content.split("\n")
        clean_chunks = []
        for chunk in chunks:
            if len(chunk) > chunk_size:
                sub_chunks = chunk.split(". ")
                clean_chunks.extend(sub_chunks)
            else:
                clean_chunks.append(chunk)

        chunks = clean_chunks.copy()
        clean_chunks = []
        for chunk in chunks:
            if len(chunk) > chunk_size:
                sub_chunks = chunk.split(", ")
                clean_chunks.extend(sub_chunks)
            else:
                clean_chunks.append(chunk)

        final_chunks = []
        cur_chunk = []
        cur_chunk_size = 0
        for chunk in clean_chunks:
            if cur_chunk_size + len(chunk) > chunk_size:
                final_chunks.append("\n".join(cur_chunk))
                cur_chunk = []
                cur_chunk_size = 0
            cur_chunk.append(chunk)
            cur_chunk_size += len(chunk)
        if cur_chunk:
            final_chunks.append("\n".join(cur_chunk))
    else:
        final_chunks = []
    return content, final_chunks


def get_file_contents(file):
    text = ''
    if file.endswith(".swp"):
        return text
    try:
        with open(file) as f:
            text = f.read()
    except:
        with open(file, "rb") as f:
            text = f.read().decode("utf-8", "ignore")
    return text


def clean_metadata(field):
    return field.replace("\t", " ").replace("\n", " ").strip().lower()


def match_with_synonyms(attribute, chunk):
    synonyms = ATTRIBUTE_SYNONYMS.get(attribute.lower(), [attribute])
    for syn in synonyms:
        if syn.startswith(r"\b"):
            if re.search(syn, chunk):
                return True
        else:
            if syn.lower() in chunk.lower():
                return True
    return False


def filter_file2chunks(file2chunks, sample_files, attribute):
    def get_attribute_parts(attribute):
        for char in ["/", "-", "(", ")", "[", "]", "{", "}", ":"]:
            attribute = attribute.replace(char, " ")
        attribute_parts = attribute.lower().split()
        return attribute_parts

    attribute_chunks = defaultdict(list)
    starting_num_chunks = 0
    ending_num_chunks = 0
    ending_in_sample_chunks = 0
    starting_in_sample_chunks = 0
    for file, chunks in file2chunks.items():
        starting_num_chunks += len(chunks)
        if file in sample_files:
            starting_in_sample_chunks += len(chunks)
        cleaned_chunks = []

        for chunk in chunks:
            if match_with_synonyms(attribute, chunk):
                cleaned_chunks.append(chunk)

        if len(cleaned_chunks) == 0:
            for chunk in chunks:
                if attribute.lower().replace(" ", "") in chunk.lower().replace(" ", ""):
                    cleaned_chunks.append(chunk)

        if len(cleaned_chunks) == 0:
            chunk2num_word_match = Counter()
            for chunk_num, chunk in enumerate(chunks):
                attribute_parts = get_attribute_parts(attribute.lower())
                for wd in attribute_parts:
                    if wd.lower() in chunk.lower():
                        chunk2num_word_match[chunk_num] += 1

            sorted_chunks = sorted(chunk2num_word_match.items(), key=lambda x: x[1], reverse=True)
            if len(sorted_chunks) > 0:
                cleaned_chunks.append(chunks[sorted_chunks[0][0]])
            if len(sorted_chunks) > 1:
                cleaned_chunks.append(chunks[sorted_chunks[1][0]])

        ending_num_chunks += len(cleaned_chunks)
        num_chunks = len(cleaned_chunks)
        num_chunks = min(num_chunks, 2)

        cleaned_chunks = cleaned_chunks[:num_chunks]
        attribute_chunks[file] = cleaned_chunks
        if file in sample_files:
            ending_in_sample_chunks += len(attribute_chunks[file])

    file2chunks = attribute_chunks
    if ending_num_chunks == 0 or ending_in_sample_chunks == 0:
        print(f"Removing because no chunks for attribute {attribute} in any file")
        return None

    print(f"For attribute {attribute}\n-- Starting with {starting_num_chunks} chunks\n-- Ending with {ending_num_chunks} chunks")
    print(f"-- {starting_in_sample_chunks} starting chunks in sample files\n-- {ending_in_sample_chunks} chunks in sample files")

    return file2chunks


def clean_function_predictions(extraction, attribute=None):
    def _flatten_to_strings(value):
        if value is None:
            return []
        if isinstance(value, list):
            out = []
            for item in value:
                out.extend(_flatten_to_strings(item))
            return out
        s = str(value).strip()
        return [s] if s else []

    def _dedup_keep_order(items):
        seen = set()
        out = []
        for item in items:
            key = item.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(item.strip())
        return out

    def _is_numeric_attribute(attr_name):
        if not attr_name:
            return False
        a = attr_name.lower()
        numeric_markers = [
            'revenue',
            'profit',
            'cost',
            'asset',
            'debt',
            'cash',
            'earnings',
            'dividend',
            'stake',
            'segments_num',
            'count_',
            'sum_',
            'avg_',
            'min_',
            'max_',
        ]
        return any(m in a for m in numeric_markers)

    def _normalize_number(text):
        cleaned = str(text).strip()
        cleaned = cleaned.replace(',', '')
        cleaned = cleaned.replace('$', '')
        cleaned = cleaned.replace('€', '')
        cleaned = cleaned.replace('£', '')
        cleaned = cleaned.replace('¥', '')
        cleaned = cleaned.replace('%', '')
        match = re.search(r'-?\d+(?:\.\d+)?', cleaned)
        if not match:
            return ''
        try:
            number = float(match.group(0))
            if abs(number - int(number)) < 1e-9:
                return str(int(number))
            return str(number)
        except Exception:
            return ''

    def _normalize_percent(text):
        s = str(text).strip()
        m = re.search(r'(\d+(?:\.\d+)?)\s*(?:%|percent|per cent)\b', s, flags=re.IGNORECASE)
        if not m:
            return ''
        try:
            v = float(m.group(1))
            if abs(v - int(v)) < 1e-9:
                return f'{int(v)}%'
            return f'{v}%'
        except Exception:
            return ''

    def _normalize_bool(text):
        t = str(text).lower().strip()
        if t in {'yes', 'y', 'true', '1', 'present', 'significant', 'major'}:
            return 'Yes'
        if t in {'no', 'n', 'false', '0', 'none', 'absent'}:
            return 'No'
        return ''

    def _is_junk_phrase(s):
        low = str(s).lower().strip()
        if not low:
            return True
        junk = [
            'provided text',
            'sample text',
            'does not contain',
            'cannot extract',
            'trick question',
            'typically include',
            'function to extract',
            'table of contents',
            'in this context',
            'based on the provided text',
        ]
        if any(x in low for x in junk):
            return True
        if low in {'none', 'null', 'n/a', 'na', 'not available', 'not specified'}:
            return True
        return False

    def _clean_surface(s):
        s = str(s).replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
        s = re.sub(r'\s+', ' ', s).strip()
        s = re.sub(r'^[#>\-\*\d\.\)\(\s]+', '', s).strip()
        s = s.strip("`'\"|;, ")
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    def _score_candidate(c, attr):
        low = c.lower()
        score = 0.0
        if _is_junk_phrase(c):
            score -= 3.0
        if len(c) > 120:
            score -= 1.0
        if low.startswith(('item ', 'section ', 'table ', 'annual report', 'financial review')):
            score -= 1.0
        if 'exact name of registrant' in low:
            score -= 1.5
        if attr == 'company_name' and re.search(r'\b(inc|corp|corporation|ltd|limited|plc|group|holdings|sa|nv)\b', low):
            score += 2.0
        if attr == 'exchange_code' and re.fullmatch(r'[A-Z]{2,8}(?::[A-Z0-9\.-]{1,8})?', c):
            score += 2.0
        if attr == 'auditor' and any(x in low for x in ['pwc', 'pricewaterhouse', 'deloitte', 'kpmg', 'ernst', ' ey ']):
            score += 2.0
        if _is_numeric_attribute(attr) and re.search(r'-?\d', c):
            score += 1.5
        if attr == 'the_highest_ownership_stake' and ('%' in c or 'percent' in low or 'per cent' in low):
            score += 2.0
        return score

    def _normalize_by_attribute(raw, attr):
        s = _clean_surface(raw)
        if not s or _is_junk_phrase(s):
            return ''
        low = s.lower()

        if attr == 'major_equity_changes':
            if any(k in low for k in ['rights issue', 'buyback', 'share issue', 'merger', 'acquisition', 'restructure', 'capital raising']):
                return 'Yes'
            return _normalize_bool(s)

        if attr == 'the_highest_ownership_stake':
            p = _normalize_percent(s)
            if p:
                return p
            n = _normalize_number(s)
            if n:
                try:
                    v = float(n)
                    if 0 <= v <= 100:
                        return f'{int(v)}%' if abs(v - int(v)) < 1e-9 else f'{v}%'
                except Exception:
                    pass
            return ''

        if _is_numeric_attribute(attr):
            return _normalize_number(s)

        if attr == 'exchange_code':
            m = re.search(r'\b(ASX|NYSE|NASDAQ|LSE|TSX|HKEX|SSE|SZSE|BSE|NSE)\b(?::\s*([A-Z0-9\.-]{1,8}))?', s, flags=re.IGNORECASE)
            if m:
                exch = m.group(1).upper()
                tick = m.group(2).upper() if m.group(2) else ''
                return f'{exch}:{tick}' if tick else exch
            m2 = re.search(r'\b([A-Z]{2,8}:[A-Z0-9\.-]{1,8})\b', s)
            return m2.group(1).upper() if m2 else ''

        if attr == 'auditor':
            if 'pricewaterhouse' in low or 'pwc' in low:
                return 'PwC'
            if 'deloitte' in low:
                return 'Deloitte'
            if 'kpmg' in low:
                return 'KPMG'
            if 'ernst' in low or re.search(r'\bey\b', low):
                return 'EY'
            return ''

        if attr == 'company_name':
            if 'exact name of registrant' in low:
                return ''
            m = re.search(r'\b([A-Z][A-Za-z0-9&\.\-]*(?:\s+[A-Z][A-Za-z0-9&\.\-]*){0,6}\s+(?:Inc\.?|Corporation|Corp\.?|Ltd\.?|Limited|PLC|plc|Group|Holdings|NV|SA))\b', s)
            if m:
                return _clean_surface(m.group(1))
            m2 = re.search(r'\b([A-Z][A-Za-z0-9&\.\-]*(?:\s+[A-Z][A-Za-z0-9&\.\-]*){0,5})\b', s)
            if m2:
                candidate = _clean_surface(m2.group(1))
                if 1 <= len(candidate.split()) <= 8:
                    return candidate
            return ''

        if attr in {'board_members', 'executive_profiles'}:
            tmp = re.sub(r'\b(and|&)\b', ',', s, flags=re.IGNORECASE)
            parts = [p.strip() for p in re.split(r'[|,;/]', tmp) if p.strip()]
            names = []
            for p in parts:
                p = _clean_surface(p)
                if _is_junk_phrase(p) or len(p.split()) > 8:
                    continue
                if re.search(r'\b(chief|officer|director|ceo|cfo|coo|chair)\b', p, flags=re.IGNORECASE) or re.search(r"^[A-Z][A-Za-z\.\-']+(?:\s+[A-Z][A-Za-z\.\-']+){1,4}$", p):
                    names.append(p)
            names = _dedup_keep_order(names)
            return ', '.join(names[:10]) if names else ''

        if attr in {'principal_activities', 'business_risks', 'major_events', 'remuneration_policy'}:
            categories = {
                'principal_activities': ['mining', 'finance', 'healthcare', 'manufacturing', 'technology', 'retail', 'energy', 'utilities', 'real estate', 'transportation', 'agriculture', 'telecommunications', 'media', 'other'],
                'business_risks': ['market risk', 'credit risk', 'operational risk', 'legal risk', 'compliance risk', 'environmental risk', 'strategic risk', 'other'],
                'major_events': ['m&a', 'merger', 'acquisition', 'litigation', 'lawsuit', 'major contract', 'leadership change', 'restructuring', 'delisting', 'other'],
                'remuneration_policy': ['fixed', 'performance-based', 'stock option', 'equity', 'mixed', 'incentive', 'bonus', 'not disclosed'],
            }
            for cat in categories.get(attr, []):
                if cat in low:
                    return cat
            return _clean_surface(s) if len(s.split()) <= 8 else ''

        return _clean_surface(s)

    if extraction is None:
        return ''

    parts = _flatten_to_strings(extraction)
    if not parts:
        return ''

    attr = (attribute or '').strip().lower()
    normalized_parts = []
    for p in parts:
        chunks = re.split(r'(?:\n|[|]|;;|\t)', str(p))
        for ch in chunks:
            s = _clean_surface(ch)
            if not s:
                continue
            if attr and s.lower().startswith(attr):
                s = s[len(attr):].strip(' :,-')
            s = _normalize_by_attribute(s, attr) if attr else _clean_surface(s)
            if s and not _is_junk_phrase(s):
                normalized_parts.append(s)

    normalized_parts = _dedup_keep_order(normalized_parts)
    if not normalized_parts:
        return ''

    if attr in {'company_name', 'registered_office', 'exchange_code', 'auditor', 'the_highest_ownership_stake'} or _is_numeric_attribute(attr):
        return max(normalized_parts, key=lambda c: _score_candidate(c, attr))

    if attr in {'board_members', 'executive_profiles'}:
        expanded = []
        for c in normalized_parts:
            expanded.extend([p.strip() for p in re.split(r'[|,;/]', c) if p.strip()])
        expanded = _dedup_keep_order(expanded)
        return ', '.join(expanded[:10]) if expanded else ''

    return ', '.join(normalized_parts)

def check_vs_train_extractions(train_extractions, final_extractions, gold_key, attribute=None):
    clean_final_extractions = {}

    gold_values = train_extractions[gold_key]
    modes = []
    start_toks = []
    end_toks = []
    for file, gold in gold_values.items():
        if type(gold) == dict:
            gold = gold[attribute]
        if type(gold) == list:
            if gold and type(gold[0]) == list:
                gold = [g[0] for g in gold]
                gold = ", ".join(gold)
            else:
                gold = ", ".join(gold)
        gold = gold.lower()
        pred = final_extractions[file].lower()
        if not pred or not gold:
            continue
        if ("<" in pred and "<" not in gold) or (">" in pred and ">" not in gold):
            check_pred = BeautifulSoup(pred).text
            if check_pred in gold or gold in check_pred:
                modes.append("soup")
        elif gold in pred and len(pred) > len(gold):
            modes.append("longer")
            idx = pred.index(gold)
            if idx > 0:
                start_toks.append(pred[:idx-1])
            end_idx = idx + len(gold)
            if end_idx < len(pred):
                end_toks.append(pred[end_idx:])

    def long_substr(data):
        substr = ''
        if len(data) > 1 and len(data[0]) > 0:
            for i in range(len(data[0])):
                for j in range(len(data[0]) - i + 1):
                    if j > len(substr) and is_substr(data[0][i:i + j], data):
                        substr = data[0][i:i + j]
        return substr

    def is_substr(find, data):
        if len(data) < 1 and len(find) < 1:
            return False
        for i in range(len(data)):
            if find not in data[i]:
                return False
        return True

    longest_end_tok = long_substr(end_toks)
    longest_start_tok = long_substr(start_toks)
    if len(set(modes)) == 1:
        num_golds = len(gold_values)
        for file, extraction in final_extractions.items():
            if "longer" in modes:
                if len(end_toks) == num_golds and longest_end_tok in extraction and extraction.count(longest_end_tok) == 1:
                    idx = extraction.index(longest_end_tok)
                    extraction = extraction[:idx]
                if len(start_toks) == num_golds and longest_start_tok in extraction and extraction.count(longest_start_tok) == 1:
                    idx = extraction.index(longest_start_tok)
                    extraction = extraction[idx:]
            elif "soup" in modes:
                extraction = BeautifulSoup(extraction).text
            clean_final_extractions[file] = extraction
    else:
        clean_final_extractions = final_extractions
    return clean_final_extractions

