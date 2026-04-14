import argparse

from runner import run_dataset
from evaluate_all import run_evaluation


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Nome dataset, es. Finan")
    parser.add_argument("--rebuild", action="store_true", help="Ricalcola CSV query anche se esistono")
    parser.add_argument(
        "--rebuild-extract",
        action="store_true",
        help="Riesegue estrazione Evaporate da zero",
    )
    parser.add_argument(
        "--rebuild-table",
        action="store_true",
        help="Ricostruisce evaporate_full_table.csv",
    )
    parser.add_argument(
        "--skip-eval",
        action="store_true",
        help="Esegue solo pipeline Evaporate (senza evaluation)",
    )
    parser.add_argument(
        "--rebuild-eval",
        action="store_true",
        help="Riesegue evaluation anche se acc.json e gia presente",
    )
    parser.add_argument("--model", default="gemini-2.5-flash")
    parser.add_argument("--train-size", type=int, default=20)
    parser.add_argument("--num-top-k-scripts", type=int, default=2)
    parser.add_argument("--chunk-size", type=int, default=2000)
    parser.add_argument("--max-chunks-per-file", type=int, default=3)
    args = parser.parse_args()

    run_dataset(
        dataset_name=args.dataset,
        rebuild=args.rebuild,
        rebuild_extract=args.rebuild_extract,
        rebuild_table=args.rebuild_table,
        model=args.model,
        train_size=args.train_size,
        num_top_k_scripts=args.num_top_k_scripts,
        chunk_size=args.chunk_size,
        max_chunks_per_file=args.max_chunks_per_file,
    )

    if not args.skip_eval:
        run_evaluation(args.dataset, rebuild=args.rebuild_eval)


if __name__ == "__main__":
    main()

