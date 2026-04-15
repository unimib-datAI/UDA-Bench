import os
import re
import subprocess

from pathlib import Path

from download import download_all_datasets

def find_sql_files(base_directory):
    """Recursively searches for all .sql files starting from the base directory."""
    base_path = Path(base_directory)
    # rglob searches deeply in all subfolders
    sql_files = list(base_path.rglob('*.sql'))
    print(f"Found {len(sql_files)} SQL files.")
    return sql_files

def extract_select_operations(sql_file_paths):
    """Extracts ONLY the SELECT queries from the provided files."""
    select_queries = []
    
    # Regex to find blocks starting with SELECT and ending with ;
    # re.IGNORECASE captures both 'SELECT' and 'select'
    # re.DOTALL allows the dot (.) to match newlines as well
    pattern_select = re.compile(r'\bSELECT\b.*?;', re.IGNORECASE | re.DOTALL)
    
    for file_path in sql_file_paths:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
                # Find all occurrences in the file
                matches = pattern_select.findall(content)
                select_queries.extend(matches)
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            
    print(f"Extracted {len(select_queries)} SELECT operations.")
    return select_queries

def run_operations(queries):
    queries = [f"\"{q.strip()}\"" for q in queries]
    
    base_dir = Path(__file__).parent.resolve()
    lotus_dir = base_dir / "systems" / "Lotus"
    quest_dir = base_dir / "systems" / "quest" 

    operations = [
        (lotus_dir, ["python", "main.py", "--sql"] + queries + ["--limit", "5"]),
        (quest_dir, ["docker", "compose", "up", "-d", "--build"]),
        (quest_dir, ["python", "main.py", "--sql"] + queries),
        (quest_dir, ["docker", "compose", "down", "-v"]),
    ]
    
    for cwd, cmd in operations:
        print(f"\n[Execution for: {cwd.name}] -> {' '.join(cmd)}")
        
        success = execute_command(cwd, cmd)
        
        if not success:
            print(f"Error executing command: {cmd[0]}")
            break

def execute_command(cwd, command):
    try:
        subprocess.run(
            command, 
            cwd=cwd,
            check=True
        )
        return True
        
    except subprocess.CalledProcessError as e:
        return False
    except FileNotFoundError as e:
        return False

if __name__ == "__main__":
    SEARCH_DIRECTORY_SQL = Path(__file__).parent / "Dataset"
    
    if not os.path.exists(SEARCH_DIRECTORY_SQL) or len(os.listdir(SEARCH_DIRECTORY_SQL)) < 5:
        download_all_datasets()
    
    sql_paths = find_sql_files(SEARCH_DIRECTORY_SQL)
    
    select_list = extract_select_operations(sql_paths)
    
    select_list = [s for s in select_list if "JOIN" not in s.upper()]
    
    run_operations(select_list[:1])
