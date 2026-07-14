# UQE usage

UQE is a standalone implementation in this checkout; it is not registered in the root meta-orchestrator. Its paths are relative to the current working directory, so run it from `systems/UQE`.

## Run

```powershell
cd systems/UQE

# Show the CLI
python main.py --help

# Run one dataset and override its default query type
python main.py --dataset lcr --query-type SFW

# Run every registered dataset
python main.py --all
```

Supported `--dataset` values are `art`, `art_image`, `disease`, `drug`, `finance`, `institutes`, `lcr` and `player`. `--query-type` is passed unchanged to the selected dataset handler; typical folders are named `SF`, `SFW`, or another workload-specific code.

## Required local data and queries

The repository does not currently version the `systems/UQE/data/` and `systems/UQE/query/` payloads expected by these runners. Supply them before execution:

```text
systems/UQE/
|-- data/<dataset>/dataset.json
`-- query/<dataset>/<query-type>/*.sql
```

Dataset directory casing/names follow the handlers in `main_*.py` (for example `data/Finance` with `query/Finance`, and `data/player` with `query/player`). A dataset JSON record can contain text directly and paths for multimodal inputs, for example:

```json
{
  "id": "1",
  "description": "your text",
  "image": "path/to/image"
}
```

## Configuration

`config_uqe.py` defines the runtime knobs:

- `USE_BART`, `BATCH_SIZE`, `BUDGET`
- `AGGR_STRATEGY`, `N_CENTROIDS`, `N_ITER`
- `GROUP_EXTRACT_SAMPLE_RATIO`, `AGGR_CLUSTER_SAMPLE_RATIO`
- `MODEL`, `OPENAI_KEY`, `BASE_URL`

This folder does not ship a requirements file. The imports require, among other packages, OpenAI's client, pandas, PyTorch/torchvision, Transformers, Sentence Transformers, scikit-learn, sqlparse and tqdm.

## Outputs

Each handler writes beneath `systems/UQE/result/<dataset>/<query-type>/<timestamp>/<query-name>/<query-name>.csv`. The exact dataset component is defined by the handler and may preserve casing (for example `Finance`).
