import sys
import os
import argparse

from pathlib import Path

sys.path.append('../../quest') # Project root path

from quest.sql.parser import sqlparser
from quest.utils.class2json import ClassToJson
from quest.sql.planner.logical import LogicalPlanner
from quest.sql.planner.physical import TextPhysicalPlanner
from quest.sql.processer.processer import Processer
from quest.db.indexer.indexer import GlobalIndexer, load_all_indexer
from quest.core.llm.sampler import AttrSampler
from quest.core.llm.llm_query import TextLLMQuerier, LLMInfo
from quest.utils.log import print_log
from quest.conf.settings import RELATIVE_PROJECT_ROOT_PATH

def run(id, sql, prompt, doc):
    
    print(f"--- Running Execution: {id} ---")
    
    ROOT_PROJECT = Path(RELATIVE_PROJECT_ROOT_PATH).resolve().parent
    
    if not os.path.exists(os.path.join(ROOT_PROJECT, ".built")):
        print("⚠️ Database not built. Please wait for 'build_db.py' to finish building the database and indexes.")
        return

    print(f"SQL Query: {sql}")
    print(f"Target Doc: {doc}")

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
    gb_indexer = load_all_indexer(table_to_type={doc: "TextDoc"})
    
    # Setup Sampler and Querier
    gb_sampler = AttrSampler(schema=prompt)
    gb_querier = TextLLMQuerier(prompt=prompt)

    gb_sampler.try_sample(gb_indexer.get_indexer(doc)[0], prompt)

    # Build Physical Plan
    physicalPlanner = TextPhysicalPlanner(gb_indexer, gb_querier, sampler=gb_sampler)
    physical = physicalPlanner.build(logical)

    # Process
    processer = Processer()
    result = processer.process(physical)
    print_log("Result Table:\n", result)

    # LLM Latency & Usage Stats
    print("\n--- LLM Statistics ---")
    print("Query Times   : ", LLMInfo.tot_query_times)
    print("Input Tokens  : ", LLMInfo.tot_input_tokens)
    print("Output Tokens : ", LLMInfo.tot_output_tokens)

    # Save results
    output_dir = os.path.join(ROOT_PROJECT, "tests/logs")
    os.makedirs(output_dir, exist_ok=True)
        
    output_path = os.path.join(output_dir, f"{id}.csv")
    result.to_csv(output_path)
    print(f"Success! Result saved to: {output_path}")

    return result

if __name__ == "__main__":
    # Setup the argument parser
    parser = argparse.ArgumentParser(description="Quest SQL Query Runner")

    # Adding arguments with your default values
    parser.add_argument("--id", type=str, default="sf1", 
                        help="Execution ID (used for the output filename)")
    
    parser.add_argument("--sql", type=str, 
                        default="SELECT birth_date, olympic_gold_medals FROM player", 
                        help="The SQL query to execute")
    
    parser.add_argument("--doc", type=str, default="player", 
                        help="The name of the table/document to query")
    
    parser.add_argument("--prompt", type=str, 
                        default="birth_date: birth date of the player; use format YYYY/%-m/%-d (e.g., 1984/1/30).\nolympic_gold_medals: number of Olympic gold medals the player has won (e.g., 3).", 
                        help="Schema prompt for the LLM")

    # Parse arguments from command line
    args = parser.parse_args()

    # Call the run function with the parsed arguments
    run(args.id, args.sql, args.prompt, args.doc)