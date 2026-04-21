import requests
import json
import argparse

from pathlib import Path
import os

def process_single_query(api_url: str, query: str, user_id, chat_id, request_id, out_dir=None):
    payload = {
        "prompt": query,
        "user_id": user_id,
        "chat_id": chat_id,
        "request_id": request_id,
        "source_id": user_id
    }
    
    try:
        response = requests.post(api_url, json=payload)
        response.raise_for_status()  # Solleva un'eccezione per status code 4xx/5xx
        
        data = response.json()
        
        if out_dir:
            out_path = Path(out_dir)
            out_path.mkdir(parents=True, exist_ok=True)
            with open(out_path / f"results.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        
    except requests.exceptions.RequestException as e:
        print(f"  [ERRORE] -> Fallimento durante la richiesta API: {e}\n")

def main():
    parser = argparse.ArgumentParser(
        description="Script batch per inviare query all'API dell'Orchestrator DQL."
    )
    
    parser.add_argument(
        "--api-url", 
        type=str,
        required=False,
        default="http://0.0.0.0:8000/api/v2/chat", 
        help="URL dell'endpoint API (default: http://0.0.0.0:8000/api/v2/chat)"
    )
    
    parser.add_argument(
        "--user-id", 
        type=str,
        required=True,
        help="ID dell'utente"
    )
    
    parser.add_argument(
        "--out_dir", 
        type=str, 
        required=False,
        default=None,
        help="Cartella di destinazione per i risultati JSON"
    )
    
    parser.add_argument(
        "--queries", 
        type=str, 
        nargs='+', 
        required=True,
        help="Lista di query da eseguire, separate da spazio. Metti le frasi tra virgolette."
    )

    args = parser.parse_args()
    
    queries_to_run = args.queries

    print(f"Inizio elaborazione batch di {len(queries_to_run)} query...")
    print(f"API URL: {args.url}")
    print(f"User ID: {args.user_id}")
    print(f"Output Directory: {args.out_dir}\n")

    for i, query in enumerate(queries_to_run):
        current_out_dir = Path(str(args.out_dir).strip('"')) if args.out_dir else None
        
        if current_out_dir and os.path.exists(current_out_dir):
            if "query_" not in str(current_out_dir.name):
                current_out_dir = current_out_dir / f"query_{i+1}"
        
        process_single_query(
            api_url=args.api_url,
            query=query,
            user_id=args.user_id,
            chat_id=i+1,
            request_id=i+1,
            out_dir=current_out_dir
        )

if __name__ == "__main__":
    main()