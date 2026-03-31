# Evaporate – Setup & Usage (UDA-Benchmark)

This guide explains how to run **Evaporate** within the UDA-Benchmark framework to extract structured data from document corpora.

---

## 🧠 What is Evaporate?

Evaporate is a **schema-driven extraction system** that:

- Uses an LLM to generate **Python extraction functions**
- Applies them over document chunks
- Produces **structured attribute-level extractions aligned with the schema**

👉 Important:
- NOT query-driven
- Works **per attribute**, not per query
- Output is **not immediately tabular**

---

## 📂 Expected Inputs

Evaporate requires:

### 1. Corpus (documents)


data/<dataset>/docs/


- Format: `.txt`
- Each file = one document

Example:

data/finance/docs/1.txt
data/finance/docs/2.txt
...


---

### 2. Schema (ground truth structure)


data/<dataset>/table.json


This defines:
- attributes to extract (e.g. `company_name`, `revenue`, etc.)

---

## ⚙️ Environment Setup

### 1. Create virtual environment

```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure API Key

Evaporate uses an LLM (e.g. Gemini via Together AI).

Set your API key:
```bash
set TOGETHER_API_KEY=your_key_here
```
(or use .env if supported)
---

## ▶️ Running Evaporate
From:
```bash
systems/Evaporate/
```
Run:
```bash
python run_profiler.py
```
🔄 What happens during execution?

Evaporate performs the following steps:

Chunk documents
For each attribute:
Generate extraction functions via LLM
Select valid functions
Apply functions over all documents
Aggregate extracted values
📤 Output

Results are saved in:
```bash
data/<dataset>/generative_indexes/<dataset>/
```
Main output:
```bash
*_all_extractions.json
```
🔎 Output format

The output contains:
```json
{
  "company_name": [...],
  "revenue": [...],
  ...
}
```
👉 Each attribute has a list of extracted values

## ⚠️ Important Notes
❗ Not query-based

Evaporate does NOT produce:
```bash
res_q1.csv
res_q2.csv
```
Instead:
```
one extraction per dataset
```
❗ Not directly comparable with UDA outputs

UDA expects:

- structured tables
- aligned with ground truth

Evaporate produces:

- attribute-level extractions

👉 A post-processing step is required:

- normalization
- alignment with GT
- conversion to tabular format

## 🧩 Limitations
Performance depends heavily on:
prompt quality
document structure
Generated functions may:
fail (no return)
be too specific (low generalization)
Works best on semi-structured documents
🚀 Extending to new datasets

To use Evaporate on a new dataset:

Add documents:
```
data/<new_dataset>/docs/
```
Add schema:
```
data/<new_dataset>/table.json
```
Update config in run_profiler.py:
```
data_lake = "<new_dataset>"
```
Run again

## 🧠 Key Insight

Evaporate is best understood as:

a schema-driven extraction engine, not a full end-to-end query system.

It is mainly useful for:

building structured datasets from documents
upstream data extraction pipelines