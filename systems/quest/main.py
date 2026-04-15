import argparse
import os
import json
import time
import psycopg2

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

def get_attributes_info(path, attr, table):
    with open(path, "r", encoding="utf-8") as f:
        attr_info = json.load(f)
        if table in attr_info and attr in attr_info[table]:
            return f"{attr}: {attr_info[table][attr]['description']}"
    
    return None

def run(sql, debug=False, output_dir=os.path.join(SYSTEM_ROOT, "results", f"{int(time.time())}")):
    print(f"SQL Query: {sql}")
    
    parser = Parser(sql)

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
    
    DATASET_DIR = PROJECT_ROOT / "Dataset"
    
    for attr, table in attributes:
        if os.path.exists(os.path.join(DATASET_DIR, table)):
            attr_desc = get_attributes_info(DATASET_DIR / table / "Attributes.json", attr, table)
            if attr_desc:
                prompt_info.append(attr_desc)
        else:
            for folder in DATASET_DIR.iterdir():
                for subfolder in folder.iterdir():
                    if subfolder.is_dir() and subfolder.name.lower() == table.lower() and (subfolder / "Attributes.json").exists():
                        attr_desc = get_attributes_info(subfolder / "Attributes.json", attr, table)
                        if attr_desc:
                            prompt_info.append(attr_desc)
                            break
            
    prompt = "\n".join(prompt_info)
    
    print("Attributes Info:\n", prompt)
    
    print("Starting Execution...\n")
    start_time = time.perf_counter()
    
    # Build AST
    ast = sqlparser.parse_sql(sql)
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
    if t in ["cspaper"]:
        gb_indexer = load_all_indexer(table_to_type={t: "ZenDBDoc"})
    else:
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
    end_time = time.perf_counter()
    print("Execution Ended.")
    print_log("Result Table:\n", result)
    
    query_info = LLMInfo.get_dict_info()
    query_info["execution_time_ms"] = (end_time - start_time) * 1000

    # LLM Latency & Usage Stats
    print("\n--- Statistics ---")
    print("Execution Time : ", query_info["execution_time_ms"], "ms")
    print("Query Times   : ", query_info["query_times"])
    print("Input Tokens  : ", query_info["input_tokens"])
    print("Output Tokens : ", query_info["output_tokens"])

    # Save results
    os.makedirs(output_dir, exist_ok=True)
        
    output_path = os.path.join(output_dir, f"results.csv")
    result.to_csv(output_path)
    
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
    parser.add_argument("--output_dir", 
                        type=str,
                        default=os.path.join(SYSTEM_ROOT, "results", f"{int(time.time())}"),
                        help="Directory to save the results and statistics")

    # Parse arguments from command line
    args = parser.parse_args()

    # Call the run function with the parsed arguments
    for i, sql in enumerate(args.sql):
        print_log(f"\n=== Running Query {i+1}/{len(args.sql)} ===")
        try:
            run(sql, args.debug, os.path.join(args.output_dir, str(i)))
        except Exception as e:
            print_log(f"Error executing query {i+1}: {e}")