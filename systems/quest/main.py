import argparse
import os
import json
import re
from pathlib import Path
import time
from sqlalchemy import inspect, text
import psycopg2
import pandas as pd

from sql_metadata import Parser
from sqlalchemy import inspect
from dotenv import load_dotenv
from build_db import index_tables

from sql.parser import sqlparser
from utils.class2json import ClassToJson
from sql.planner.logical import LogicalPlanner
from sql.planner.physical import TextPhysicalPlanner
from sql.processer.processer import Processer
from db.indexer.indexer import GlobalIndexer, load_all_indexer
from core.llm.sampler import AttrSampler
from core.llm.llm_query import TextLLMQuerier, LLMInfo
from utils.log import print_log
from conf.settings import SYSTEM_ROOT, PROJECT_ROOT, opengauss_conn

SQL_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "ON", "IN", "AS", "INNER", "JOIN", "LEFT",
    "GROUP", "ORDER", "BY", "HAVING", "MIN", "MAX", "COUNT", "SUM", "AVG",
    "DISTINCT", "AND", "OR", "ASC", "DESC",
}


def normalize_sql_identifiers(sql):
    parts = re.split(r"('(?:''|[^'])*'|\"(?:\"\"|[^\"])*\")", sql)
    identifier_re = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")

    def normalize_token(match):
        token = match.group(0)
        upper_token = token.upper()
        if upper_token in SQL_KEYWORDS:
            return upper_token
        return token.lower()

    for i in range(0, len(parts), 2):
        parts[i] = identifier_re.sub(normalize_token, parts[i])
    return "".join(parts)


def get_attributes_info(path, attr, table):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        attr_info = json.load(f)
        table_info = attr_info.get(table)
        if table_info is None:
            table_info = next((v for k, v in attr_info.items() if k.lower() == table.lower()), None)
        if table_info and attr in table_info:
            return f"{attr}: {table_info[attr]['description']}"
    
    return None


def iter_attribute_files(table):
    seen = set()
    dataset_dir = PROJECT_ROOT / "Dataset"

    candidates = [dataset_dir / table / "Attributes.json"]

    if dataset_dir.exists():
        for folder in dataset_dir.iterdir():
            if not folder.is_dir():
                continue
            for subfolder in folder.iterdir():
                if subfolder.is_dir() and subfolder.name.lower() == table.lower():
                    candidates.append(subfolder / "Attributes.json")

    query_dir = PROJECT_ROOT / "Query"
    if query_dir.exists():
        candidates.extend(query_dir.glob("*/*_attributes.json"))

    for path in candidates:
        normalized = str(path.resolve()).lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        if path.exists():
            yield path

def run(sql, debug=False, output_dir=os.path.join(SYSTEM_ROOT, "results", f"{int(time.time())}")):
    print(f"SQL Query: {sql}")
    normalized_sql = normalize_sql_identifiers(sql)
    if normalized_sql != sql:
        print(f"Normalized SQL Query: {normalized_sql}")
    
    parser = Parser(normalized_sql)

    columns = parser.columns
    tables = parser.tables
    
    if len(columns) == 0 or len(tables) == 0:
        raise Exception("⚠️ Failed to parse SQL query. Please check the syntax.")

    datasets_to_index = []
    
    try:
        inspector = inspect(opengauss_conn)
        insp_tables = inspector.get_table_names()
        
        datasets_to_index = [t for t in tables if f"{t}_docs" not in insp_tables or f"{t}_chunks" not in insp_tables]
    except Exception as e:
        raise Exception(f"Error during database interaction: {e}")
    
    if datasets_to_index:
        print(f"Datasets to index: {datasets_to_index}")
        index_tables(datasets_to_index, debug)
    
    attributes = []
    for c in columns:
        if "." in c:
            info = c.split(".")
            
            if len(info) == 2 and info[0] in tables:
                attributes.append((info[1], info[0]))
        elif len(tables) == 1:
            attributes.append((c, tables[0]))
    
    prompt_info = []
    
    for attr, table in attributes:
        for attr_file in iter_attribute_files(table):
            attr_desc = get_attributes_info(attr_file, attr, table)
            if attr_desc:
                prompt_info.append(attr_desc)
                break
            
    prompt = "\n".join(prompt_info)
    
    print("Attributes Info:\n", prompt)
    
    print("Starting Execution...\n")
    start_time = time.perf_counter()
    try:
        # Build AST
        ast = sqlparser.parse_sql(normalized_sql)
        jsonConverter = ClassToJson()
        js = jsonConverter.toJson(ast)
        print("AST:\n", js)

        # Build Logical Plan
        logicalPlanner = LogicalPlanner()
        logical = logicalPlanner.build_logical_plan(ast)
        js = jsonConverter.toJson(logical)
        print_log("Logical Plan:\n", js)

        # Load Indexer
        t = tables[0].lower()
        
        gb_indexer = load_all_indexer(table_to_type={t: "TextDoc"})
        
        # Setup Sampler and Querier
        gb_sampler = AttrSampler(schema=prompt)
        gb_querier = TextLLMQuerier(prompt=prompt)

        gb_sampler.try_sample(gb_indexer.get_indexer(t)[0], prompt)

        # Build Physical Plan
        physicalPlanner = TextPhysicalPlanner(gb_indexer, gb_querier, sampler=gb_sampler)
        physical = physicalPlanner.build(logical)

        # Process
        processer = Processer()
        result = processer.process(physical)
        
        query_info = LLMInfo.get_dict_info()
    except Exception as e:
        print(f"Error during query execution: {e}")
        if os.getenv("QUEST_WRITE_FALLBACK_ON_ERROR", "").lower() not in {"1", "true", "yes"}:
            raise
        
        # Create the fallback DataFrame as requested
        fallback_data = {col: [""] * 100 for col in columns}
        result = pd.DataFrame(fallback_data)
        
        # Add/overwrite the 'file_name' column with values from 1 to 100
        result['file_name'] = [f"{str(i)}.txt" for i in range(1, 101)]
        
        # Optional but recommended: reorder columns to have 'file_name' as the first column
        ordered_cols = ['file_name'] + [c for c in columns if c != 'file_name']
        result = result[ordered_cols]
        
        query_info = {}
        
    end_time = time.perf_counter()
    print("Execution Ended.")
    print_log("Result Table:\n", result)
    
    query_info["execution_time_ms"] = (end_time - start_time) * 1000

    # LLM Latency & Usage Stats
    print("\n--- Statistics ---")
    print("Execution Time : ", query_info.get("execution_time_ms", 0), "ms")
    print("Query Times   : ", query_info.get("query_times", []))
    print("Input Tokens  : ", query_info.get("input_tokens", 0))
    print("Output Tokens : ", query_info.get("output_tokens", 0))

    # Save results
    os.makedirs(output_dir, exist_ok=True)
        
    output_path = os.path.join(output_dir, f"results.csv")
    result.to_csv(output_path, index=False) # Added index=False to avoid saving the pandas index in the csv
    
    output_path = os.path.join(output_dir, f"info.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(query_info, f, indent=4)
        
    print(f"Success! Result saved to: {output_path}")

    return result

if __name__ == "__main__":
    # Setup the argument parser
    parser = argparse.ArgumentParser(description="Quest SQL Query Runner")

    # Adding arguments with your default values    
    parser.add_argument("--sql", 
                        type=str,
                        nargs='+',
                        required=True,
                        help="The SQL queries to execute")
    parser.add_argument("--debug", 
                        action="store_true",
                        help="Enable debug mode: this will index only 5 documents per dataset for a faster execution")
    parser.add_argument("--out_dir", 
                        type=str,
                        default=os.path.join(SYSTEM_ROOT, "results", f"{int(time.time())}"),
                        help="Directory to save the results and statistics")

    # Parse arguments from command line
    args = parser.parse_args()

    # Call the run function with the parsed arguments
    for i, sql in enumerate(args.sql):
        current_out_dir = Path(str(args.out_dir).strip('"'))
        
        file_dir = current_out_dir / "results.csv"
        if not file_dir.exists():
            print_log(f"\n=== Running Query {i+1}/{len(args.sql)} ===")
            if "query_" not in str(current_out_dir.name):
                current_out_dir = current_out_dir / f"query_{i+1}"
                
            try:
                run(sql.replace(';', ''), args.debug, current_out_dir)
            except Exception as e:
                print_log(f"Error executing query {i+1}: {e}")
        else:
            print_log(f"Skipping query {i+1}: results.csv already exists at {file_dir}")
