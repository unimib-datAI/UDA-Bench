import requests
import json
import argparse

from pathlib import Path
import os
import time


def _load_extra_payload() -> dict:
    """
    Optional extension point:
    - DQL_EXTRA_PAYLOAD_JSON='{"tenant_id":"abc","session_id":"xyz"}'
    """
    raw = os.environ.get("DQL_EXTRA_PAYLOAD_JSON", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        print("[WARN] DQL_EXTRA_PAYLOAD_JSON is not valid JSON; ignoring.")
        return {}


def _payload_variants(query: str, user_id, chat_id, request_id) -> list[dict]:
    base = {
        "user_id": str(user_id),
        "chat_id": str(chat_id),
        "request_id": str(request_id),
        "source_id": str(user_id),
    }
    base.update(_load_extra_payload())
    return [
        {"prompt": query, **base},
        {"query": query, **base},
        {"message": query, **base},
    ]


def _request_timeout() -> int:
    raw = os.environ.get("DQL_REQUEST_TIMEOUT", "180").strip()
    try:
        value = int(raw)
        return value if value > 0 else 180
    except ValueError:
        return 180


def _request_retries() -> int:
    raw = os.environ.get("DQL_REQUEST_RETRIES", "0").strip()
    try:
        value = int(raw)
        return value if value >= 0 else 0
    except ValueError:
        return 0


def process_single_query(api_url: str, query: str, user_id, chat_id, request_id, out_dir=None):
    variants = _payload_variants(query, user_id, chat_id, request_id)
    last_error = None
    timeout_sec = _request_timeout()
    retries = _request_retries()

    for idx, payload in enumerate(variants, start=1):
        for attempt in range(retries + 1):
            try:
                response = requests.post(api_url, json=payload, timeout=timeout_sec)
                response.raise_for_status()  # Solleva un'eccezione per status code 4xx/5xx

                data = response.json()

                if out_dir:
                    out_path = Path(out_dir)
                    out_path.mkdir(parents=True, exist_ok=True)
                    with open(out_path / "results.json", "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4)
                if idx > 1:
                    print(f"[INFO] Query accepted with payload variant #{idx}.")
                return True

            except requests.exceptions.HTTPError as e:
                last_error = e
                status = e.response.status_code if e.response is not None else "unknown"
                if status != 422:
                    break
                # For 422 we may still succeed with a different payload variant.
                break
            except requests.exceptions.Timeout as e:
                last_error = e
                if attempt < retries:
                    wait_s = 2 * (attempt + 1)
                    print(f"[WARN] Timeout query (attempt {attempt+1}/{retries+1}), retry in {wait_s}s...")
                    time.sleep(wait_s)
                    continue
                break
            except requests.exceptions.RequestException as e:
                last_error = e
                break

    print(f"  [ERRORE] -> Fallimento durante la richiesta API: {last_error}")
    if isinstance(last_error, requests.exceptions.HTTPError) and last_error.response is not None:
        body_preview = (last_error.response.text or "").strip()
        if body_preview:
            print(f"  [ERRORE] -> Response body: {body_preview}")
    print()
    return False

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
    print(f"API URL: {args.api_url}")
    print(f"User ID: {args.user_id}")
    print(f"Output Directory: {args.out_dir}\n")

    failures = 0
    for i, query in enumerate(queries_to_run):
        current_out_dir = Path(str(args.out_dir).strip('"')) if args.out_dir else None
        
        if current_out_dir and os.path.exists(current_out_dir):
            if "query_" not in str(current_out_dir.name):
                current_out_dir = current_out_dir / f"query_{i+1}"
        
        ok = process_single_query(
            api_url=args.api_url,
            query=query,
            user_id=args.user_id,
            chat_id=i+1,
            request_id=i+1,
            out_dir=current_out_dir
        )
        if not ok:
            failures += 1

    raise SystemExit(1 if failures else 0)

if __name__ == "__main__":
    main()
