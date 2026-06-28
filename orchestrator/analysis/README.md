# Finance Analysis Report

Questa cartella contiene il generatore del report HTML riepilogativo per gli output Finance disponibili localmente nei vari sistemi.

## Contenuto

- `finance_outputs_report.py`: legge gli artifact di evaluation presenti in `systems/*/outputs/finan`, aggrega le metriche dagli `acc.json` e genera un report HTML.
- `finance_outputs_report.html`: report standard generato dagli artifact di evaluation deterministica.
- `finance_outputs_report_llm.html`: report generato dagli artifact di evaluation con LLM judge.

Lo script include automaticamente solo i sistemi per cui trova risultati locali. Per i sistemi con task riconoscibili dal nome degli artifact, come DocETL, DQL ed Evaporate, separa `select`, `agg`, `filter` e `mixed`. Gli output Lotus disponibili sono 60 su 86 query Finance e vengono mostrati solo nel riepilogo/grafico globale, non nelle sezioni per-task o nel dettaglio query-level.

## Generare il report

Da root del repository:

```powershell
.\.venv-docetl\Scripts\python.exe orchestrator\analysis\finance_outputs_report.py
```

Output default:

```text
orchestrator\analysis\finance_outputs_report.html
```

Per generare il report basato sulla evaluation LLM:

```powershell
.\.venv-docetl\Scripts\python.exe orchestrator\analysis\finance_outputs_report.py --variant llm
```

Il report LLM mostra sia la metrica raw degli `acc.json` sia la metrica adjusted, che considera corrette le query con risultato gold vuoto e output sistema vuoto (`F1 = 1.0`).

Output default:

```text
orchestrator\analysis\finance_outputs_report_llm.html
```

## Aprire il report

Da PowerShell:

```powershell
start orchestrator\analysis\finance_outputs_report.html
```

Per aprire il report LLM:

```powershell
start orchestrator\analysis\finance_outputs_report_llm.html
```
