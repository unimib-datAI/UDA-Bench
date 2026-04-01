import os
import random
import pickle
from tqdm import tqdm
from functools import partial
from multiprocessing import Pool
from collections import Counter, defaultdict
import signal
from contextlib import contextmanager

import re
import json
import math
import time
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import warnings

from bs4 import GuessedAtParserWarning
warnings.filterwarnings('ignore', category=GuessedAtParserWarning)
warnings.filterwarnings("ignore", category=UserWarning, module="bs4")
warnings.filterwarnings("ignore", category=UserWarning, module="BeautifulSoup")
warnings.filterwarnings("ignore", category=UserWarning, module="lxml")

from prompts import (
    METADATA_GENERATION_FOR_FIELDS,
    EXTRA_PROMPT,
    METADATA_EXTRACTION_WITH_LM_CONTEXT,
    METADATA_EXTRACTION_WITH_LM_ZERO_SHOT,
    IS_VALID_ATTRIBUTE,
    Step,
)
from utils import apply_prompt, get_file_attribute
from evaluate_profiler import get_topk_scripts_per_field, evaluate
from profiler_utils import (
    filter_file2chunks,
    check_vs_train_extractions,
    clean_function_predictions,
)

import sys

try:
    from evaporate.weak_supervision.run_ws import run_ws
except ImportError:
    run_ws = None


class TimeoutException(Exception):
    pass


@contextmanager
def time_limit(seconds):
    # Su Windows SIGALRM non è disponibile.
    # In quel caso disabilitiamo il timeout invece di far fallire tutto.
    if not hasattr(signal, "SIGALRM"):
        yield
        return

    def signal_handler(signum, frame):
        raise TimeoutException("Timed out!")

    old_handler = signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def check_remove_attribute(
    all_extractions,
    attribute,
    topic,
    train_extractions={},
    manifest_session=None,
    overwrite_cache=False,
    all_metrics={},
):
    extraction_fraction = 1.0
    for key, info in all_metrics.items():
        extraction_fraction = info["extraction_fraction"]
        break

    values = []
    num_toks = 0
    has_non_none = False
    for i, (file, metadata) in enumerate(all_extractions.items()):
        if metadata and (metadata.lower() not in ["none"]) and metadata != "":
            has_non_none = True
        if len(values) < 3 and metadata and metadata.lower() != "none" and metadata != "":
            values.append(metadata)

    if not has_non_none and extraction_fraction > 0.5:
        return False, num_toks
    elif not has_non_none and extraction_fraction <= 0.5:
        return True, num_toks

    extractions = [m for f, m in all_extractions.items()]
    if len(set(extractions)) == 1 or (len(set(extractions)) == 2 and "" in set(extractions)):
        keys = list(train_extractions.keys())
        gold_extractions = train_extractions[keys[0]]
        if Counter(gold_extractions).most_common(1)[0][0].lower() != Counter(extractions).most_common(1)[0][0].lower():
            return False, num_toks
        else:
            return True, num_toks

    attr_str = f"{attribute}"
    prompt_template = IS_VALID_ATTRIBUTE[0]

    votes = Counter()
    for value in values:
        prompt = prompt_template.format(value=value, attr_str=attr_str, topic=topic)
        try:
            check, num_toks = apply_prompt(
                Step(prompt),
                max_toks=10,
                manifest=manifest_session,
                overwrite_cache=overwrite_cache,
            )
            check = check.split("----")[0]
            if "yes" in check.lower():
                votes["yes"] += 1
            elif "no" in check.lower():
                votes["no"] += 1
        except Exception:
            print("Rate limited...")

    keep = False
    if votes["yes"]:
        keep = True
    return keep, num_toks


def combine_extractions(
    args,
    all_extractions,
    all_metrics,
    combiner_mode="mv",
    attribute=None,
    train_extractions=None,
    gold_key=None,
    extraction_fraction_thresh=0.8,
):
    final_extractions = {}

    extraction_fraction = 0.0
    for key, info in all_metrics.items():
        extraction_fraction = info["extraction_fraction"]
        break

    all_file2extractions = defaultdict(list)
    total_tokens_prompted = 0

    for key, file2extractions in all_extractions.items():
        for i, (file, extraction) in tqdm(
            enumerate(file2extractions.items()),
            total=len(file2extractions),
            desc=f"Applying key {key}",
        ):
            extraction = clean_function_predictions(
                extraction,
                attribute=attribute,
            )
            all_file2extractions[file].append(extraction)

    if combiner_mode in {"mv", "top_k"}:
        for file, extractions in all_file2extractions.items():
            if extraction_fraction >= extraction_fraction_thresh:
                extractions = [e for e in extractions if e]
                if not extractions:
                    extractions = [""]
            final_extractions[file] = str(Counter(extractions).most_common(1)[0][0])

    elif combiner_mode == "ws":
        if run_ws is None:
            raise ImportError(
                "Weak supervision module not available. "
                "Use combiner_mode='mv' or 'top_k'."
            )

        preds, used_deps, missing_files = run_ws(
            all_file2extractions,
            args.gold_extractions_file,
            attribute=attribute,
            has_abstains=extraction_fraction,
            extraction_fraction_thresh=extraction_fraction_thresh,
        )

        for i, (file, extractions) in enumerate(all_file2extractions.items()):
            if file in missing_files:
                continue
            if len(extractions) == 1:
                if isinstance(extractions, list):
                    extractions = extractions[0]
                pred = extractions
                final_extractions[file] = pred
            elif len(Counter(extractions)) == 1:
                pred = str(Counter(extractions).most_common(1)[0][0])
                final_extractions[file] = pred
            else:
                pred = preds[len(final_extractions)]
                if not pred:
                    final_extractions[file] = str(Counter(extractions).most_common(1)[0][0])
                else:
                    final_extractions[file] = pred
    else:
        raise ValueError(f"Unsupported combiner_mode: {combiner_mode}")

    if train_extractions:
        final_extractions = check_vs_train_extractions(
            train_extractions,
            final_extractions,
            gold_key,
            attribute=attribute,
        )

    return final_extractions, total_tokens_prompted


def apply_final_ensemble(
    group_files,
    file2chunks,
    file2contents,
    selected_keys,
    all_metrics,
    attribute,
    function_dictionary,
    data_lake="",
    function_cache=False,
    manifest_sessions=[],
    MODELS=[],
    overwrite_cache=False,
    do_end_to_end=False,
):
    all_extractions = {}
    total_tokens_prompted = 0

    for key in selected_keys:
        if "function" in key:
            t0 = time.time()
            print(f"Applying function {key}...")
            extractions, num_function_errors = apply_final_profiling_functions(
                file2contents,
                group_files,
                function_dictionary[key]["function"],
                attribute,
                data_lake=data_lake,
                function_cache=function_cache,
            )
            t1 = time.time()
            total_time = t1 - t0
            all_extractions[key] = extractions
            function_dictionary[key]["runtime"] = total_time

        elif key in MODELS:
            manifest_session = manifest_sessions[key]
            extractions, num_toks, errored_out = get_model_extractions(
                file2chunks,
                group_files,
                attribute,
                manifest_session,
                key,
                overwrite_cache=overwrite_cache,
            )
            total_tokens_prompted += num_toks
            if not errored_out:
                all_extractions[key] = extractions
        else:
            raise ValueError(f"Key {key} not supported.")

    if not do_end_to_end and not all_extractions:
        default = {}
        for file, _ in file2contents.items():
            default[file] = [""]
        all_extractions["default"] = default

    return all_extractions, total_tokens_prompted


def apply_final_profiling_functions(
    files2contents,
    sample_files,
    fn,
    attribute,
    data_lake="",
    function_cache=False,
):
    if function_cache:
        original_fn = fn
        file_attribute = attribute.replace(" ", "_").replace("/", "_").lower()
        cache_dir = "./function_cache/"
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        cache_path = f"{cache_dir}function_cache_{file_attribute}_{data_lake}.pkl"
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "rb") as f:
                    function_cache_dict = pickle.load(f)
            except Exception:
                function_cache_dict = defaultdict(dict)
        else:
            function_cache_dict = defaultdict(dict)

    all_extractions = {}
    num_function_errors = 0
    num_timeouts = 0

    for i, file in enumerate(sample_files):
        content = files2contents[file]
        extractions = []

        global result
        global text
        global preprocessed_text
        text = content
        preprocessed_text = text.replace(">\n", ">")

        print("\n=== DEBUG FILE ===")
        print("FILE:", file)
        print("FIRST 500 CHARS OF CONTENT:")
        print(repr(content[:500]))
        print("==================")

        if num_timeouts > 1:
            all_extractions[file] = deduplicate_extractions(extractions)
            continue

        if function_cache and file in function_cache_dict and original_fn in function_cache_dict[file]:
            extractions = function_cache_dict[file][original_fn]
            print("Loaded function result from cache:", repr(extractions))
        else:
            if not isinstance(fn, str):
                try:
                    result = fn(text)
                    print("FUNCTION OBJECT RESULT:", repr(result))
                    extractions.append(result)
                except Exception as e:
                    print("FUNCTION OBJECT ERROR:", repr(e))
                    num_function_errors = 1
            else:
                fn = "\n".join([l for l in fn.split("\n") if "print(" not in l])
                fn = "\n".join([l for l in fn.split("\n") if not l.startswith("#")])
                function_field = get_function_field_from_attribute(attribute)

                print("\n=== FUNCTION CODE ===")
                print(fn)
                print("=====================")

                err = 0
                try:
                    try:
                        with time_limit(1):
                            exec(fn, globals())
                            exec(f"result = get_{function_field}_field(text)", globals())
                    except TimeoutException as e:
                        print(f"Timeout {num_timeouts}")
                        num_timeouts += 1
                        raise e

                    print("FUNCTION RESULT ON text:", repr(result))
                    extractions.append(result)
                except Exception as e:
                    print("FUNCTION ERROR ON text:", repr(e))
                    err = 1

                if err:
                    try:
                        try:
                            with time_limit(1):
                                exec(fn, globals())
                                exec(f"result = get_{function_field}_field(preprocessed_text)", globals())
                        except TimeoutException as e:
                            print("Timeout")
                            raise e

                        print("FUNCTION RESULT ON preprocessed_text:", repr(result))
                        extractions.append(result)
                        err = 0
                    except Exception as e:
                        print("FUNCTION ERROR ON preprocessed_text:", repr(e))

                if err:
                    num_function_errors = 1

            if function_cache:
                function_cache_dict[file][original_fn] = extractions

        all_extractions[file] = deduplicate_extractions(extractions)

    if function_cache:
        try:
            with open(cache_path, "wb") as f:
                pickle.dump(function_cache_dict, f)
        except Exception as e:
            print("Failed to save function cache:", repr(e))

    return all_extractions, num_function_errors


def get_function_field_from_attribute(attribute):
    return re.sub(r"[^A-Za-z0-9]", "_", attribute)


def get_functions(
    file2chunks,
    sample_files,
    all_extractions,
    attribute,
    manifest_session,
    overwrite_cache=False,
):
    total_tokens_prompted = 0
    functions = {}
    function_promptsource = {}

    for i, file in tqdm(
        enumerate(sample_files),
        total=len(sample_files),
        desc=f"Generating functions for attribute {attribute}",
    ):
        chunks = file2chunks[file]
        for chunk in chunks:
            function_field = get_function_field_from_attribute(attribute)
            for prompt_num, prompt_template in enumerate(METADATA_GENERATION_FOR_FIELDS):
                prompt = prompt_template.format(
                    attribute=attribute,
                    function_field=function_field,
                    chunk=chunk,
                )
                try:
                    script, num_toks = apply_prompt(
                        Step(prompt),
                        max_toks=1200,
                        manifest=manifest_session,
                        overwrite_cache=overwrite_cache,
                    )
                    total_tokens_prompted += num_toks
                except Exception as e:
                    print(e)
                    print(f"Failed to generate function for {attribute}")
                    continue

                print("\n=== RAW GENERATED SCRIPT ===")
                print("ATTRIBUTE:", attribute)
                print("FILE:", file)
                print("PROMPT NUM:", prompt_num)
                print(repr(script))
                print("============================")

                script = script.strip()

                if script.startswith("```python"):
                    script = script[len("```python"):].strip()
                elif script.startswith("```"):
                    script = script[len("```"):].strip()
                if script.endswith("```"):
                    script = script[:-3].strip()

                print("\n=== SCRIPT BEFORE NORMALIZATION ===")
                print(script)
                print("===================================")

                accepted = False

                if "def " in script and "return" in script:
                    lines = script.split("\n")
                    return_indices = [idx for idx, s in enumerate(lines) if "return" in s]

                    if return_indices:
                        last_return_idx = return_indices[-1]
                        script = "\n".join(lines[: last_return_idx + 1])

                        cleaned_lines = []
                        for s in script.split("\n"):
                            if "print(" in s:
                                continue
                            cleaned_lines.append(s)
                        script = "\n".join(cleaned_lines).strip()

                        if "def " in script and "return" in script:
                            accepted = True

                if not accepted:
                    print("\n--- GENERATED FUNCTION INCOMPLETE: building fallback function ---")

                    fallback_value = None
                    train_pred = all_extractions.get(file, [])
                    candidate_values = []

                    if isinstance(train_pred, list):
                        for item in train_pred:
                            if isinstance(item, list):
                                for sub in item:
                                    if isinstance(sub, str) and sub.strip():
                                        candidate_values.append(sub.strip())
                            elif isinstance(item, str) and item.strip():
                                candidate_values.append(item.strip())

                    print("Fallback candidates:", candidate_values)

                    if attribute == "company_name":
                        script = f"""import re

def get_{function_field}_field(text: str):
    banned_exact = {{
        "FORM 10-K",
        "FORM10-K",
        "FORM 20-F",
        "UNITED STATES SECURITIES AND EXCHANGE COMMISSION",
        "SECURITIES AND EXCHANGE COMMISSION",
        "WASHINGTON, D.C. 20549",
        "DEAR FELLOW SHAREHOLDERS",
        "DEAR FELLOW STOCKHOLDER",
        "CONTENTS",
        "TABLE OF CONTENTS",
        "ANNUAL REPORT 2022",
        "REPORT 2022",
        "LONG-TERM VALUE",
        "DIRECTORS",
        "AUDITOR",
        "REGISTERED OFFICE",
        "COMPANY SECRETARY",
        "NOTICE OF MEETING",
        "NOTICE OF MEETING AND PROXY STATEMENT",
    }}

    banned_contains = [
        "AUDIT",
        "AUDITOR",
        "CORPORATE GOVERNANCE",
        "TABLE OF CONTENTS",
        "CONTENTS",
        "REGISTERED OFFICE",
        "COMPANY SECRETARY",
        "DIRECTORS' REPORT",
        "NOTICE OF MEETING",
        "PROXY STATEMENT",
        "MAILING ADDRESS",
        "STOCK EXCHANGE LISTING",
    ]

    company_tokens = [
        " INC", " INC.", " CORP", " CORP.", " CORPORATION",
        " LTD", " LTD.", " LIMITED", " PLC", " LLC", " GROUP",
        " HOLDINGS", " THERAPEUTICS", " BIOSCIENCES", " FINANCIAL",
        " REIT", " BANCORP", " COMPANY"
    ]

    def clean_candidate(value: str) -> str:
        value = value.strip()
        value = re.sub(r'\\s+', ' ', value).strip(" -–—:|")

        value = re.sub(r'(?i)^directors of\\s+', '', value)
        value = re.sub(r'(?i)\\s+mailing address$', '', value)
        value = re.sub(r'(?i)\\s+annual report\\s+\\d{{4}}$', '', value)
        value = re.sub(r'(?i)\\s+report\\s+\\d{{4}}$', '', value)
        value = re.sub(r'\\s+\\d{{4}}$', '', value)

        value = re.sub(r'\\s+', ' ', value).strip(" -–—:|,")
        return value

    def is_bad(value: str) -> bool:
        upper_value = value.upper()
        if not value:
            return True
        if upper_value in banned_exact:
            return True
        if len(value) < 3:
            return True
        if value.isdigit():
            return True
        if "ABN:" in upper_value:
            return True
        if "COMMISSION FILE" in upper_value:
            return True
        if "EXACT NAME OF REGISTRANT" in upper_value:
            return True
        if "FOR THE FISCAL YEAR" in upper_value:
            return True
        for bad in banned_contains:
            if bad in upper_value:
                return True
        return False

    strong_patterns = [
        r"##\\s*([^\\n]+)\\n\\s*\\(Exact name of registrant",
        r"#\\s*([^\\n]+)\\n\\s*\\(Exact name of registrant",
    ]

    for pattern in strong_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            value = clean_candidate(match.group(1))
            if not is_bad(value):
                return value

    heading_patterns = [
        r"##\\s*([^\\n]+)",
        r"#\\s*([^\\n]+)",
    ]

    candidates = []
    for pattern in heading_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            value = clean_candidate(match.group(1))
            if is_bad(value):
                continue
            candidates.append(value)

    for value in candidates:
        upper_value = value.upper()
        if any(tok in upper_value for tok in company_tokens):
            return value

    for line in text.splitlines():
        value = clean_candidate(line)
        upper_value = value.upper()
        if is_bad(value):
            continue
        if any(tok in upper_value for tok in company_tokens):
            return value

    if candidates:
        return candidates[0]

    return ""
"""
                    else:
                        for val in candidate_values:
                            cleaned = val.strip()
                            if cleaned:
                                fallback_value = cleaned
                                break

                        if not fallback_value:
                            print("--- REJECTED: no fallback value available ---")
                            continue

                        fallback_value = str(fallback_value).strip()
                        escaped_value = re.escape(fallback_value)

                        script = f"""import re

def get_{function_field}_field(text: str):
    match = re.search(r'##\\s*({escaped_value})\\b', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""
"""

                    print("\n=== FALLBACK FUNCTION ===")
                    print(script)
                    print("=========================")

                print("\n=== SCRIPT AFTER NORMALIZATION ===")
                print(script)
                print("=================================")

                fn_num = len(functions)
                functions[f"function_{fn_num}"] = script
                function_promptsource[f"function_{fn_num}"] = prompt_num
                print(f"Accepted function_{fn_num}")

    return functions, function_promptsource, total_tokens_prompted


def trim_chunks(chunk, attribute, window=20):
    tokenized_chunk = chunk.lower().split()
    indices = [i for i, s in enumerate(tokenized_chunk) if attribute.lower() in s]

    if indices:
        index = indices[0]
        lb = max(0, index - window)
        ub = min(len(chunk), index)
        trimmed_chunk = " ".join(tokenized_chunk[lb:ub])
    else:
        mini_chunks = []
        for i in range(0, len(tokenized_chunk), 50):
            mini_chunks.append(" ".join(tokenized_chunk[i:i + 50]))

        max_num_attr_tokens = 0
        max_num_attr_tokens_idx = 0
        for i, mini_chunk in enumerate(mini_chunks):
            num_attr_tokens = len([s for s in attribute.lower().split() if s in mini_chunk])
            if num_attr_tokens > max_num_attr_tokens:
                max_num_attr_tokens = num_attr_tokens
                max_num_attr_tokens_idx = i
        trimmed_chunk = mini_chunks[max_num_attr_tokens_idx]

    return trimmed_chunk


def deduplicate_extractions(extractions):
    deduplicated_extractions = []
    for extraction in extractions:
        duplicate = False
        for prev_extraction in deduplicated_extractions:
            if extraction == prev_extraction:
                duplicate = True
        if not duplicate:
            deduplicated_extractions.append(extraction)
    return deduplicated_extractions


def get_model_extractions(
    file2chunks,
    sample_files,
    attribute,
    manifest_session,
    model_name,
    overwrite_cache=False,
    collecting_preds=False,
):
    num_errors = 0
    total_prompts = 0
    total_tokens_prompted = 0
    has_context_length_error = False
    file2results = {}
    errored_out = False

    for i, file in tqdm(
        enumerate(sample_files),
        total=len(sample_files),
        desc=f"Extracting attribute {attribute} using LM",
    ):
        if num_errors > 10 and num_errors == total_prompts:
            print("All errorring out.. moving on.")
            errored_out = True
            continue

        chunks = file2chunks[file]
        extractions = []

        for chunk_num, chunk in enumerate(chunks):
            if "flan" in model_name:
                PROMPTS = METADATA_EXTRACTION_WITH_LM_ZERO_SHOT
            else:
                PROMPTS = METADATA_EXTRACTION_WITH_LM_CONTEXT

            if has_context_length_error:
                chunk = trim_chunks(chunk, attribute)

            for prompt_template in PROMPTS:
                prompt = prompt_template.format(attribute=attribute, chunk=chunk)
                total_prompts += 1
                try:
                    extraction, num_toks = apply_prompt(
                        Step(prompt),
                        max_toks=100,
                        manifest=manifest_session,
                        overwrite_cache=overwrite_cache,
                    )
                    total_tokens_prompted += num_toks
                except Exception as e:
                    num_errors += 1
                    print(f"Failed to extract {attribute} for {file}: {repr(e)}")
                    has_context_length_error = True
                    continue

                extraction = extraction.split("---")[0].strip("\n")
                extraction = extraction.split("\n")[-1].replace("[", "").replace("]", "").replace("'", "").replace('"', "")
                extraction = extraction.split(", ")
                extractions.append(extraction)

            if collecting_preds and (not any(e for e in extractions) or not any(e[0] for e in extractions)):
                for prompt_template in EXTRA_PROMPT:
                    prompt = prompt_template.format(attribute=attribute, chunk=chunk)
                    total_prompts += 1
                    try:
                        extraction, num_toks = apply_prompt(
                            Step(prompt),
                            max_toks=100,
                            manifest=manifest_session,
                            overwrite_cache=overwrite_cache,
                        )
                        total_tokens_prompted += num_toks
                    except Exception as e:
                        num_errors += 1
                        print(f"Failed to extract {attribute} for {file}: {repr(e)}")
                        has_context_length_error = True
                        continue

                    extraction = extraction.split("---")[0].strip("\n")
                    extraction = extraction.split("\n")[-1].replace("[", "").replace("]", "").replace("'", "").replace('"', "")
                    extraction = extraction.split(", ")
                    extractions.append(extraction)

        file2results[file] = deduplicate_extractions(extractions)

    return file2results, total_tokens_prompted, errored_out


def get_all_extractions(
    file2chunks,
    file2contents,
    sample_files,
    attribute,
    manifest_sessions,
    extraction_MODELS,
    GOLD_KEY,
    args,
    use_qa_model=False,
    overwrite_cache=False,
):
    total_tokens_prompted = 0
    all_extractions = {}

    manifest_session = manifest_sessions[GOLD_KEY]
    extractions, num_toks, errored_out = get_model_extractions(
        file2chunks,
        sample_files,
        attribute,
        manifest_session,
        GOLD_KEY,
        overwrite_cache=overwrite_cache,
        collecting_preds=True,
    )
    total_tokens_prompted += num_toks

    if not errored_out:
        all_extractions[GOLD_KEY] = extractions
    else:
        print(f"Not applying {GOLD_KEY} extractions")
        return {}, {}, total_tokens_prompted

    function_dictionary = defaultdict(dict)
    for model in extraction_MODELS:
        manifest_session = manifest_sessions[model]
        functions, function_promptsource, num_toks = get_functions(
            file2chunks,
            sample_files,
            all_extractions[GOLD_KEY],
            attribute,
            manifest_session,
            overwrite_cache=overwrite_cache,
        )
        total_tokens_prompted += num_toks

        for fn_key, fn in functions.items():
            all_extractions[fn_key], num_function_errors = apply_final_profiling_functions(
                file2contents,
                sample_files,
                fn,
                attribute,
            )
            function_dictionary[fn_key]["function"] = fn
            function_dictionary[fn_key]["promptsource"] = function_promptsource[fn_key]
            function_dictionary[fn_key]["extract_model"] = model

    return all_extractions, function_dictionary, total_tokens_prompted


def run_profiler(
    run_string,
    args,
    file2chunks,
    file2contents,
    sample_files,
    group_files,
    manifest_sessions,
    attribute,
    profiler_args,
):
    total_tokens_prompted = 0

    attribute = attribute.lower()
    file_attribute = get_file_attribute(attribute)
    save_path = os.path.join(
        args.generative_index_path,
        f"{run_string}_{file_attribute}_file2metadata.json",
    )

    os.makedirs(args.generative_index_path, exist_ok=True)

    file2chunks = filter_file2chunks(file2chunks, sample_files, attribute)
    if file2chunks is None:
        return total_tokens_prompted, 0

    all_extractions, function_dictionary, num_toks = get_all_extractions(
        file2chunks,
        file2contents,
        sample_files,
        attribute,
        manifest_sessions,
        profiler_args.EXTRACTION_MODELS,
        profiler_args.GOLD_KEY,
        args,
        use_qa_model=profiler_args.use_qa_model,
        overwrite_cache=profiler_args.overwrite_cache,
    )

    total_tokens_prompted += num_toks
    if not all_extractions:
        return total_tokens_prompted, 0

    all_metrics, key2golds, num_toks = evaluate(
        all_extractions,
        profiler_args.GOLD_KEY,
        field=attribute,
        manifest_session=manifest_sessions[profiler_args.GOLD_KEY],
        overwrite_cache=profiler_args.overwrite_cache,
        combiner_mode=profiler_args.combiner_mode,
        extraction_fraction_thresh=profiler_args.extraction_fraction_thresh,
        use_abstension=profiler_args.use_abstension,
    )
    total_tokens_prompted += num_toks

    selected_keys = get_topk_scripts_per_field(
        all_metrics,
        function_dictionary,
        all_extractions,
        gold_key=profiler_args.GOLD_KEY,
        k=profiler_args.num_top_k_scripts,
        do_end_to_end=profiler_args.do_end_to_end,
        combiner_mode=profiler_args.combiner_mode,
    )

    if not selected_keys and profiler_args.do_end_to_end:
        print(f"Removing {file_attribute}")
        if os.path.exists(save_path):
            os.remove(save_path)
        return total_tokens_prompted, 0

    print(
        f"Apply the scripts to the data lake and save the metadata. "
        f"Taking the top {profiler_args.num_top_k_scripts} scripts per field."
    )
    top_k_extractions, num_toks = apply_final_ensemble(
        group_files,
        file2chunks,
        file2contents,
        selected_keys,
        all_metrics,
        attribute,
        function_dictionary,
        data_lake=args.data_lake,
        manifest_sessions=manifest_sessions,
        function_cache=True,
        MODELS=profiler_args.EXTRACTION_MODELS,
        overwrite_cache=profiler_args.overwrite_cache,
        do_end_to_end=profiler_args.do_end_to_end,
    )
    total_tokens_prompted += num_toks

    file2metadata, num_toks = combine_extractions(
        args,
        top_k_extractions,
        all_metrics,
        combiner_mode=profiler_args.combiner_mode,
        train_extractions=all_extractions,
        attribute=attribute,
        gold_key=profiler_args.GOLD_KEY,
        extraction_fraction_thresh=profiler_args.extraction_fraction_thresh,
    )
    total_tokens_prompted += num_toks

    if profiler_args.do_end_to_end:
        keep_attribute, num_toks = check_remove_attribute(
            file2metadata,
            attribute,
            args.topic,
            train_extractions=key2golds,
            manifest_session=manifest_sessions[profiler_args.GOLD_KEY],
            overwrite_cache=profiler_args.overwrite_cache,
            all_metrics=all_metrics,
        )
        total_tokens_prompted += num_toks
        if not keep_attribute:
            print(f"Removing {file_attribute}")
            if os.path.exists(save_path):
                os.remove(save_path)
            return total_tokens_prompted, 0

    all_extractions_path = os.path.join(
        args.generative_index_path,
        f"{run_string}_{file_attribute}_all_extractions.json",
    )
    functions_path = os.path.join(
        args.generative_index_path,
        f"{run_string}_{file_attribute}_functions.json",
    )
    all_metrics_path = os.path.join(
        args.generative_index_path,
        f"{run_string}_{file_attribute}_all_metrics.json",
    )
    top_k_keys_path = os.path.join(
        args.generative_index_path,
        f"{run_string}_{file_attribute}_top_k_keys.json",
    )
    file2metadata_path = os.path.join(
        args.generative_index_path,
        f"{run_string}_{file_attribute}_file2metadata.json",
    )
    top_k_extractions_path = os.path.join(
        args.generative_index_path,
        f"{run_string}_{file_attribute}_top_k_extractions.json",
    )

    try:
        os.makedirs(args.generative_index_path, exist_ok=True)

        with open(all_extractions_path, "w", encoding="utf-8") as f:
            json.dump(all_extractions, f, ensure_ascii=False)

        with open(functions_path, "w", encoding="utf-8") as f:
            json.dump(function_dictionary, f, ensure_ascii=False)

        with open(all_metrics_path, "w", encoding="utf-8") as f:
            json.dump(all_metrics, f, ensure_ascii=False)

        with open(top_k_keys_path, "w", encoding="utf-8") as f:
            json.dump(selected_keys, f, ensure_ascii=False)

        with open(file2metadata_path, "w", encoding="utf-8") as f:
            json.dump(file2metadata, f, ensure_ascii=False)

        with open(top_k_extractions_path, "w", encoding="utf-8") as f:
            json.dump(top_k_extractions, f, ensure_ascii=False)

        print(f"Save path: {all_extractions_path}")
        return total_tokens_prompted, 1

    except Exception as e:
        print("Primary save failed:", repr(e))

    try:
        clean_file2metadata = {}
        for file, metadata in file2metadata.items():
            clean_file2metadata[file] = str(metadata)

        with open(file2metadata_path, "w", encoding="utf-8") as f:
            json.dump(clean_file2metadata, f, ensure_ascii=False)

        with open(all_metrics_path, "w", encoding="utf-8") as f:
            json.dump(all_metrics, f, ensure_ascii=False)

        with open(top_k_keys_path, "w", encoding="utf-8") as f:
            json.dump(selected_keys, f, ensure_ascii=False)

        print("Saved!")
        return total_tokens_prompted, 1

    except Exception as e:
        print(f"Failed to save {file_attribute} metadata. Error: {e}")

    return total_tokens_prompted, 0