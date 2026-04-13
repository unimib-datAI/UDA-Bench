import os
import time
import random
import json
import datetime
from tqdm import tqdm
import pickle
import argparse
from collections import defaultdict, Counter
import pandas as pd

from utils import get_structure, get_manifest_sessions, get_file_attribute
from profiler_utils import chunk_file, sample_scripts
from schema_identification import identify_schema
from profiler import run_profiler
from evaluate_synthetic import main as evaluate_synthetic_main
from llm_metrics import get_global_tracker, reset_global_tracker
from configs import get_experiment_args


random.seed(0)


def get_data_lake_info(args, data_lake):
    extractions_file = None

    DATA_DIR = args.data_dir
    file_groups = os.listdir(args.data_dir)
    if not DATA_DIR.endswith("/"):
        DATA_DIR += "/"
    file_groups = [
        f"{DATA_DIR}{file_group}"
        for file_group in file_groups
        if not file_group.startswith(".")
    ]
    full_file_groups = file_groups.copy()
    extractions_file = args.gold_extractions_file
    parser = "txt"

    return file_groups, extractions_file, parser, full_file_groups


def chunk_files(file_group, parser, chunk_size, remove_tables, max_chunks_per_file, body_only):
    file2chunks = {}
    file2contents = {}
    for file in tqdm(file_group, total=len(file_group), desc="Chunking files"):
        content, chunks = chunk_file(
            parser,
            file,
            chunk_size=chunk_size,
            mode="train",
            remove_tables=remove_tables,
            body_only=body_only,
        )
        if max_chunks_per_file > 0:
            chunks = chunks[:max_chunks_per_file]
        file2chunks[file] = chunks
        file2contents[file] = content
    return file2chunks, file2contents


def prepare_data(profiler_args, file_group, data_args, parser="html"):
    data_lake = profiler_args.data_lake
    if profiler_args.body_only:
        body_only = profiler_args.body_only
        suffix = f"_bodyOnly{body_only}"
    else:
        suffix = ""

    manifest_sessions = get_manifest_sessions(
        profiler_args.MODELS,
        MODEL2URL=profiler_args.MODEL2URL,
        KEYS=profiler_args.KEYS,
    )

    os.makedirs(data_args.cache_dir, exist_ok=True)

    chunks_cache = (
        f"{data_args.cache_dir}/{data_lake}_size{len(file_group)}"
        f"_chunkSize{profiler_args.chunk_size}_{suffix}_file2chunks.pkl"
    )
    contents_cache = (
        f"{data_args.cache_dir}/{data_lake}_size{len(file_group)}"
        f"_chunkSize{profiler_args.chunk_size}_{suffix}_file2contents.pkl"
    )

    if os.path.exists(chunks_cache) and os.path.exists(contents_cache):
        with open(chunks_cache, "rb") as f:
            file2chunks = pickle.load(f)
        with open(contents_cache, "rb") as f:
            file2contents = pickle.load(f)
    else:
        file2chunks, file2contents = chunk_files(
            file_group,
            parser,
            profiler_args.chunk_size,
            profiler_args.remove_tables,
            profiler_args.max_chunks_per_file,
            profiler_args.body_only,
        )

        with open(chunks_cache, "wb") as f:
            pickle.dump(file2chunks, f)
        with open(contents_cache, "wb") as f:
            pickle.dump(file2contents, f)

    return file2chunks, file2contents, manifest_sessions


def get_run_string(
    data_lake,
    today,
    file_groups,
    profiler_args,
    do_end_to_end,
    train_size,
    dynamicbackoff,
    models,
):
    body = int(bool(profiler_args.body_only))
    remove_tables = int(bool(profiler_args.remove_tables))
    cascading = int(bool(do_end_to_end))
    backoff = int(bool(dynamicbackoff))

    model_ct = len(models)
    if getattr(profiler_args, "use_qa_model", False):
        model_ct += 1

    run_string = (
        f"dl{data_lake}"
        f"_d{today}"
        f"_fs{len(file_groups)}"
        f"_ts{train_size}"
        f"_k{profiler_args.num_top_k_scripts}"
        f"_cs{profiler_args.chunk_size}"
        f"_rt{remove_tables}"
        f"_b{body}"
        f"_c{cascading}"
        f"_ub{backoff}"
        f"_m{model_ct}"
    )
    return run_string


def get_gold_metadata(args):
    return [
        "company_name",
        "registered_office",
        "exchange_code",
        "principal_activities",
        "board_members",
        "executive_profiles",
        "revenue",
        "net_profit_or_loss",
        "total_Debt",
        "total_assets",
        "cash_reserves",
        "net_assets",
        "earnings_per_share",
        "dividend_per_share",
        "largest_shareholder",
        "the_highest_ownership_stake",
        "major_equity_changes",
        "major_events",
        "bussiness_sales",
        "bussiness_profit",
        "bussiness_cost",
        "business_segments_num",
        "business_risks",
        "environmental_compliance",
        "remuneration_policy",
        "auditor",
    ]


def determine_attributes_to_remove(attributes, args, run_string, num_attr_to_cascade):
    attributes_reordered = {}
    attributes_to_remove = []
    attributes_to_metrics = {}
    attribute_to_first_extractions = {}
    mappings_names = {}

    for num, attribute in enumerate(attributes):
        attribute = attribute.lower()
        file_attribute = get_file_attribute(attribute)

        metrics_path = f"{args.generative_index_path}/{run_string}_{file_attribute}_all_metrics.json"
        metadata_path = f"{args.generative_index_path}/{run_string}_{file_attribute}_file2metadata.json"
        topk_path = f"{args.generative_index_path}/{run_string}_{file_attribute}_top_k_keys.json"

        if not os.path.exists(metrics_path) or not os.path.exists(metadata_path):
            continue

        if num >= num_attr_to_cascade:
            if os.path.exists(metrics_path):
                os.remove(metrics_path)
            if os.path.exists(metadata_path):
                os.remove(metadata_path)
            continue

        with open(metrics_path) as f:
            metrics = json.load(f)
        with open(topk_path) as f:
            selected_keys = json.load(f)
        with open(metadata_path) as f:
            file2metadata = json.load(f)

        attributes_reordered[attribute] = metrics[selected_keys[0]]

        if selected_keys and metrics:
            for a, m in attributes_to_metrics.items():
                if attribute.lower() in a.lower() or a.lower() in attribute.lower():
                    if m == metrics[selected_keys[0]]["average_f1"]:
                        attributes_to_remove.append(attribute)
                        mappings_names[a] = attribute
                        mappings_names[attribute] = a
                        break

        first_extractions = [m for i, (f, m) in enumerate(file2metadata.items()) if i < 5]
        if any(f != "" for f in first_extractions):
            first_extractions = " ".join(first_extractions)
            for a, m in attribute_to_first_extractions.items():
                if m == first_extractions:
                    attributes_to_remove.append(attribute)
                    mappings_names[a] = attribute
                    mappings_names[attribute] = a
                    break

        if attribute in attributes_to_remove:
            continue

        if selected_keys:
            attributes_to_metrics[attribute] = metrics[selected_keys[0]]["average_f1"]
        attribute_to_first_extractions[attribute] = first_extractions

    return attributes_to_remove, mappings_names, attributes


def measure_openie_results(
    attributes,
    args,
    profiler_args,
    run_string,
    gold_attributes,
    attributes_to_remove,
    file_groups,
    mappings_names,
):
    file2extractions = defaultdict(dict)
    unique_attributes = set()
    num_extractions2results = {}
    data_lake = profiler_args.data_lake

    for attr_num, attribute in enumerate(attributes):
        attribute = attribute.lower()
        file_attribute = get_file_attribute(attribute)
        metadata_path = f"{args.generative_index_path}/{run_string}_{file_attribute}_file2metadata.json"

        if os.path.exists(metadata_path):
            if attribute in attributes_to_remove:
                print(f"Removing: {attribute}")
                os.remove(metadata_path)
                continue

            with open(metadata_path) as f:
                file2metadata = json.load(f)
                for file, extraction in file2metadata.items():
                    file2extractions[file][attribute] = extraction

        unique_attributes.add(attribute)

        if file2extractions:
            num_extractions = len(unique_attributes)

        nums = [1, 2, 3, 4, len(attributes) - 1, len(gold_attributes)]
        if (
            file2extractions
            and ((num_extractions) % 5 == 0 or num_extractions in nums)
        ) or attr_num == len(attributes) - 1:
            if num_extractions in num_extractions2results:
                continue

            with open(f"{args.generative_index_path}/{run_string}_file2extractions.json", "w") as f:
                json.dump(file2extractions, f)

            results = evaluate_synthetic_main(
                run_string,
                args,
                profiler_args,
                data_lake,
                sample_files=file_groups,
                stage="openie",
                mappings_names=mappings_names,
            )
            num_extractions2results[num_extractions] = results

    rows = []
    for file, attrs in file2extractions.items():
        row = {"file": file}
        row.update(attrs)
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv("extracted_attributes.csv", index=False)
    print("属性-值表格已保存为 extracted_attributes.csv")
    return num_extractions2results


def prerun_profiler(profiler_args):
    file2chunks, file2contents, manifest_sessions = prepare_data(
        profiler_args, profiler_args.full_file_groups, profiler_args, profiler_args.parser
    )
    manifest_sessions = {
        k: v for k, v in manifest_sessions.items() if k in profiler_args.MODELS
    }

    gold_attributes = get_gold_metadata(profiler_args)

    try:
        with open(profiler_args.gold_extractions_file) as f:
            gold_extractions_tmp = json.load(f)
    except Exception:
        with open(profiler_args.gold_extractions_file, "rb") as f:
            gold_extractions_tmp = pickle.load(f)

    gold_extractions = {}
    for file, extractions in gold_extractions_tmp.items():
        gold_extractions[os.path.join(profiler_args.data_dir, file.split("/")[-1])] = extractions

    manifest_sessions[profiler_args.GOLD_KEY] = {}
    manifest_sessions[profiler_args.GOLD_KEY]["__name"] = "gold_extraction_file"

    for attribute in gold_attributes:
        manifest_sessions[profiler_args.GOLD_KEY][attribute] = {}
        for file in profiler_args.file_groups:
            manifest_sessions[profiler_args.GOLD_KEY][attribute][file] = gold_extractions[file][attribute]

    sample_files = sample_scripts(
        profiler_args.file_groups,
        train_size=profiler_args.train_size,
    )

    data_dict = {
        "file2chunks": file2chunks,
        "file2contents": file2contents,
        "manifest_sessions": manifest_sessions,
        "gold_attributes": gold_attributes,
        "sample_files": sample_files,
        "gold_extractions": gold_extractions,
    }
    return data_dict


def identify_attributes(profiler_args, data_dict, evaluation=False):
    print("开始模式识别阶段，重置LLM统计...")
    reset_global_tracker()

    file2chunks = data_dict["file2chunks"]
    file2contents = data_dict["file2contents"]
    manifest_sessions = data_dict["manifest_sessions"]

    sample_files = sample_scripts(
        profiler_args.file_groups,
        train_size=profiler_args.train_size,
    )

    t0 = time.time()
    num_toks = identify_schema(
        profiler_args.run_string,
        profiler_args,
        file2chunks,
        file2contents,
        sample_files,
        manifest_sessions,
        profiler_args.data_lake,
        profiler_args,
    )
    t1 = time.time()

    print(f"\n=== 模式识别阶段LLM调用统计 ===")
    tracker = get_global_tracker()
    summary = tracker.get_summary()
    print(f"调用次数: {summary['total_calls']}")
    print(f"总Token: {summary['total_tokens']['total']:,}")
    print(f"总成本: ${summary['total_cost']['total']:.4f}")
    print(f"总延迟: {summary['average_latency']:.3f}秒")

    with open(f"{profiler_args.generative_index_path}/{profiler_args.run_string}_identified_schema.json") as f:
        most_common_fields = json.load(f)
    with open(f"{profiler_args.generative_index_path}/{profiler_args.run_string}_order_of_addition.json") as f:
        order_of_addition = json.load(f)
        order = {item: (len(order_of_addition) - i) for i, item in enumerate(order_of_addition)}

    ctr = Counter(most_common_fields)
    pred_metadata = sorted(
        ctr.most_common(profiler_args.num_attr_to_cascade),
        key=lambda x: (x[1], order[x[0]]),
        reverse=True,
    )
    attributes = [item[0].lower() for item in pred_metadata]

    if evaluation:
        evaluation_result = evaluate_synthetic_main(
            profiler_args.run_string,
            profiler_args,
            profiler_args,
            profiler_args.data_lake,
            stage="schema_id",
        )
    else:
        evaluation_result = None

    return attributes, t1 - t0, num_toks, evaluation_result


def get_attribute_function(profiler_args, data_dict, attribute):
    print(f"\n开始提取属性 '{attribute}'，记录LLM调用统计...")
    attr_start_time = time.time()

    tracker = get_global_tracker()
    start_calls = tracker.total_calls
    start_summary = tracker.get_summary()

    sample_files = sample_scripts(
        profiler_args.file_groups,
        train_size=profiler_args.train_size,
    )

    t0 = time.time()
    num_toks, success = run_profiler(
        profiler_args.run_string,
        profiler_args,
        data_dict["file2chunks"],
        data_dict["file2contents"],
        sample_files,
        profiler_args.full_file_groups,
        data_dict["manifest_sessions"],
        attribute,
        profiler_args,
    )
    t1 = time.time()

    end_summary = tracker.get_summary()
    attr_calls = end_summary["total_calls"] - start_calls
    attr_tokens = end_summary["total_tokens"]["total"] - start_summary["total_tokens"]["total"]
    attr_cost = end_summary["total_cost"]["total"] - start_summary["total_cost"]["total"]
    attr_total_time = time.time() - attr_start_time

    print(f"属性 '{attribute}' 提取完成:")
    print(f"  LLM调用次数: {attr_calls}")
    print(f"  使用Token: {attr_tokens:,}")
    print(f"  花费成本: ${attr_cost:.4f}")
    print(f"  总耗时: {attr_total_time:.2f}秒")

    try:
        file_attribute = get_file_attribute(attribute)
        with open(f"{profiler_args.generative_index_path}/{profiler_args.run_string}_{file_attribute}_functions.json") as f:
            function_dictionary = json.load(f)
        with open(f"{profiler_args.generative_index_path}/{profiler_args.run_string}_{file_attribute}_top_k_keys.json") as f:
            selected_keys = json.load(f)
    except Exception:
        selected_keys = None
        function_dictionary = None

    return function_dictionary, selected_keys, t1 - t0, num_toks


def run_experiment(profiler_args):
    reset_global_tracker()
    print("=== 开始LLM调用统计跟踪 ===")

    do_end_to_end = profiler_args.do_end_to_end
    num_attr_to_cascade = profiler_args.num_attr_to_cascade
    train_size = profiler_args.train_size
    data_lake = profiler_args.data_lake

    print("Data lake")
    today = datetime.datetime.today().strftime("%m%d%Y")

    _, _, _, _, args = get_structure(data_lake, profiler_args)
    print("args.generative_index_path =", repr(args.generative_index_path))
    print("args.cache_dir =", repr(args.cache_dir))

    os.makedirs(args.generative_index_path, exist_ok=True)
    os.makedirs(args.cache_dir, exist_ok=True)
    os.makedirs(os.path.join(profiler_args.base_data_dir, "results_dumps"), exist_ok=True)

    file_groups, extractions_file, parser, full_file_groups = get_data_lake_info(args, data_lake)
    file2chunks, file2contents, manifest_sessions = prepare_data(
        profiler_args, full_file_groups, args, parser
    )

    manifest_sessions = {
        k: v for k, v in manifest_sessions.items() if k in profiler_args.MODELS
    }

    gold_attributes = get_gold_metadata(args)

    results_by_train_size = defaultdict(dict)
    total_time_dict = defaultdict(dict)
    total_tokens_prompted = 0

    print(f"\n\nData-lake: {data_lake}, Train size: {train_size}")
    setattr(profiler_args, "train_size", train_size)

    run_string = get_run_string(
        data_lake,
        today,
        full_file_groups,
        profiler_args,
        do_end_to_end,
        train_size,
        profiler_args.use_dynamic_backoff,
        profiler_args.EXTRACTION_MODELS,
    )
    print("run_string =", repr(run_string))

    sample_files = sample_scripts(
        file_groups,
        train_size=profiler_args.train_size,
    )

    if do_end_to_end:
        t0 = time.time()
        num_toks = identify_schema(
            run_string,
            args,
            file2chunks,
            file2contents,
            sample_files,
            manifest_sessions,
            data_lake,
            profiler_args,
        )
        t1 = time.time()
        total_time = t1 - t0
        total_tokens_prompted += num_toks
        total_time_dict["schemaId"][f"totalTime_trainSize{train_size}"] = int(total_time)

        results = evaluate_synthetic_main(
            run_string,
            args,
            profiler_args,
            data_lake,
            stage="schema_id",
        )
        results_by_train_size[train_size]["schema_id"] = results

    if do_end_to_end:
        with open(f"{args.generative_index_path}/{run_string}_identified_schema.json") as f:
            most_common_fields = json.load(f)
        with open(f"{args.generative_index_path}/{run_string}_order_of_addition.json") as f:
            order_of_addition = json.load(f)
            order = {item: (len(order_of_addition) - i) for i, item in enumerate(order_of_addition)}

        ctr = Counter(most_common_fields)
        pred_metadata = sorted(
            ctr.most_common(num_attr_to_cascade),
            key=lambda x: (x[1], order[x[0]]),
            reverse=True,
        )
        attributes = [item[0].lower() for item in pred_metadata]
    else:
        attributes = gold_attributes

    num_collected = 0
    for i, attribute in enumerate(attributes):
        print(f"\n\nExtracting {attribute} ({i+1} / {len(attributes)})")
        t0 = time.time()
        num_toks, success = run_profiler(
            run_string,
            args,
            file2chunks,
            file2contents,
            sample_files,
            full_file_groups,
            manifest_sessions,
            attribute,
            profiler_args,
        )
        t1 = time.time()
        total_time = t1 - t0
        total_tokens_prompted += num_toks
        total_time_dict["extract"][f"totalTime_trainSize{train_size}"] = int(total_time)
        if success:
            num_collected += 1
        if num_collected >= num_attr_to_cascade:
            break

    results = evaluate_synthetic_main(
        run_string,
        args,
        profiler_args,
        data_lake,
        gold_attributes=gold_attributes,
        stage="extract",
    )
    results_by_train_size[train_size]["extract"] = results

    if do_end_to_end:
        attributes_to_remove, mappings_names, attributes = determine_attributes_to_remove(
            attributes,
            args,
            run_string,
            num_attr_to_cascade,
        )
        numextractions2results = measure_openie_results(
            attributes,
            args,
            profiler_args,
            run_string,
            gold_attributes,
            attributes_to_remove,
            full_file_groups,
            mappings_names,
        )
        if "openie" not in results_by_train_size[train_size]:
            results_by_train_size[train_size]["openie"] = {}
        results_by_train_size[train_size]["openie"] = numextractions2results

    results_by_train_size[train_size]["total_tokens_prompted"] = total_tokens_prompted
    results_by_train_size[train_size]["num_total_files"] = len(full_file_groups)
    results_by_train_size[train_size]["num_sample_files"] = len(sample_files)

    result_path_dir = os.path.join(profiler_args.base_data_dir, "results_dumps")
    os.makedirs(result_path_dir, exist_ok=True)

    print(run_string)
    with open(f"{result_path_dir}/{run_string}_results_by_train_size.pkl", "wb") as f:
        pickle.dump(results_by_train_size, f)
        print("Saved!")

    print(f"Total tokens prompted: {total_tokens_prompted}")

    tracker = get_global_tracker()
    tracker.print_summary()

    stats_file = f"{result_path_dir}/{run_string}_llm_metrics.json"
    tracker.save_to_file(stats_file)


def main():
    profiler_args = get_experiment_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    finance_dir = os.path.join(base_dir, "data", "finance")
    docs_dir = os.path.join(finance_dir, "docs")
    gold_file = os.path.join(finance_dir, "table.json")

    profiler_args.data_lake = "finance"
    profiler_args.data_dir = docs_dir
    profiler_args.base_data_dir = finance_dir
    profiler_args.gold_extractions_file = gold_file

    profiler_args.MODELS = ["gemini-2.5-flash"]
    profiler_args.EXTRACTION_MODELS = ["gemini-2.5-flash"]
    profiler_args.MODEL2URL = {}
    profiler_args.KEYS = []
    profiler_args.GOLD_KEY = "gemini-2.5-flash"
    profiler_args.combiner_mode = "mv"
    profiler_args.do_end_to_end = False

    profiler_args.train_size = 20
    profiler_args.num_top_k_scripts = 2
    profiler_args.num_attr_to_cascade = len(get_gold_metadata(profiler_args))
    profiler_args.chunk_size = 2000
    profiler_args.max_chunks_per_file = 3
    profiler_args.overwrite_cache = True

    print("data_lake:", profiler_args.data_lake)
    print("data_dir:", profiler_args.data_dir)
    print("gold_extractions_file:", profiler_args.gold_extractions_file)
    print("MODELS:", profiler_args.MODELS)
    print("EXTRACTION_MODELS:", profiler_args.EXTRACTION_MODELS)
    print("GOLD_KEY:", profiler_args.GOLD_KEY)
    print("combiner_mode:", profiler_args.combiner_mode)

    if not os.path.isdir(profiler_args.data_dir):
        raise FileNotFoundError(f"Docs directory not found: {profiler_args.data_dir}")
    if not os.path.isfile(profiler_args.gold_extractions_file):
        raise FileNotFoundError(f"Gold file not found: {profiler_args.gold_extractions_file}")

    run_experiment(profiler_args)


if __name__ == "__main__":
    main()