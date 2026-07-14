# Finance Analysis Report

Questa cartella contiene il generatore del report HTML riepilogativo per gli output Finance disponibili localmente nei vari sistemi.

## Contenuto

- `finance_outputs_report.py`: legge gli artifact Finance locali, aggrega le metriche dagli `acc.json` e genera il report HTML finale.
- `finance_outputs_report_llm.html`: output predefinito della variante LLM, con metriche raw, adjusted e weighted.
- `finance_outputs_report.html`: output predefinito della variante deterministica (`--variant standard`).

Lo script include automaticamente solo i sistemi del confronto Finance finale: DocETL, DQL, Evaporate e Quest. Per DocETL, DQL ed Evaporate usa gli artifact LLM-assisted (`outputs_llm_*` / `evaluation_llm_*`); per Quest usa gli artifact LLM importati in `systems/quest/outputs/finan/evaluation_llm_quest`, con fallback agli artifact deterministici in `systems/quest/outputs/finan/evaluation` solo se quelli LLM non sono presenti. Il report separa `select`, `agg`, `filter` e `mixed` e non include Lotus.

## Generare il report

Da root del repository:

```powershell
.\.venv-docetl\Scripts\python.exe orchestrator\analysis\finance_outputs_report.py
```

Output default:

```text
orchestrator\analysis\finance_outputs_report_llm.html
```

Il report mostra sia la metrica raw degli `acc.json` sia la metrica adjusted, che considera corrette le query con risultato gold vuoto e output sistema vuoto (`F1 = 1.0`).

Per generare il vecchio report deterministico:

```powershell
.\.venv-docetl\Scripts\python.exe orchestrator\analysis\finance_outputs_report.py --variant standard --output orchestrator\analysis\finance_outputs_report_standard.html
```

## Aprire il report

Da PowerShell:

```powershell
start orchestrator\analysis\finance_outputs_report_llm.html
```
