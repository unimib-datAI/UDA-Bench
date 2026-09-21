# Note sulla valutazione dei risultati Finance

## Contesto

L'esperimento confronta quattro sistemi (DocETL, RVCL, Evaporate, QUEST) sulle 86 query SQL del dataset Finance: ogni risposta è confrontata con il ground truth (GT) e riceve precision, recall e F1.

**I punteggi non sono stati calcolati tutti allo stesso modo.** A parità di risposte, cambiano con due scelte di valutazione: l'uso di un giudice LLM e la versione dell'evaluator che costruisce il GT. Le sorgenti del viewer, e anche i numeri della tesi, mescolano queste scelte. Per questo:

- confronta tra loro solo sorgenti con la stessa etichetta tra parentesi, per esempio tutte `exact match · string`;
- una differenza di punteggio tra etichette diverse non va attribuita al modello: può dipendere solo dalla valutazione.

Le sezioni seguenti spiegano le due scelte con esempi e riassumono chi usa cosa. Appunti del 2026-09-17, ricavati dall'analisi della cartella `UDA outputs backup` e del codice del repo.

## 1. Il giudice: quanto è tollerante il confronto, a parità di dati

Senza giudice due celle valgono 1 solo se sono identiche (a parte spazi e maiuscole), altrimenti 0. Con il giudice, un LLM decide se i due valori dicono la stessa cosa. Nel viewer le due modalità si chiamano **exact match** e **LLM match**: la seconda è in realtà la prima più il giudice, perché il confronto esatto viene fatto comunque per primo e l'LLM interviene solo sulle celle e sulle chiavi che non coincidono.

Esempio, select_8 di QUEST: F1 da **0.1929 a 0.6099**, stesse risposte e stesso GT. Per colonna: `registered_office` 0.41 → 0.86, `executive_profiles` 0.05 → 0.40. Celle tipiche:

```
GT:    1065 East Hillsdale Blvd., Suite 100, Foster City, California
QUEST: 1065 East Hillsdale Blvd., Suite 100 Foster City, California 94404
```

Il giudice fa anche una seconda cosa: **allinea le righe** di agg e mixed, dove la chiave è il valore di raggruppamento e non l'id del documento. In agg_1 le righe allineate passano da 40 a 44:

```
GT: KPMG AG Wirtschaftspr fungsgesellschaft   <->  QUEST: KPMG AG Wirtschaftsprüfungsgesellschaft
GT: PricewaterhouseCoopers LLC                <->  QUEST: PricewaterhouseCoopers, LLP
```

La prima coppia è un accento perso nel GT, giustamente riconosciuta. La seconda è discutibile: LLC e LLP sono due entità diverse. Il giudice alza i punteggi, ma qualche accoppiamento è generoso.

## 2. La versione dell'evaluator: quali righe il GT contiene

Il commit `047aea3` del 2 luglio 2026 ha cambiato la conversione delle colonne numeriche prima del confronto, in `evaluation/tools/utils.py`. Conta perché diverse query filter hanno il numero tra apici:

```sql
SELECT net_assets, board_members, net_profit_or_loss FROM Finance
WHERE net_profit_or_loss > '1460000000'
```

- **Prima:** confronto come testo. `'491955' > '1460000000'` risulta vero, perché `4` viene dopo `1` in ordine alfabetico. GT = **54 righe**, con dentro società da 491 mila e 22 milioni.
- **Dopo:** confronto come numero. GT = **8 righe**, solo quelle sopra 1,46 miliardi.

Succede in 16 query filter (per esempio filter_18 passa da 2 a 81 righe). Le risposte dei modelli non cambiano: cambia il metro, cioè quali righe ci si aspetta. Nel viewer le due versioni si chiamano **string** e **number**.

### In sintesi

- Il **giudice** cambia il punteggio a parità di dati: decide quanto due valori possono essere scritti diversamente restando giusti.
- La **versione dell'evaluator** cambia i dati attesi: il GT di alcune query filter diventa un altro insieme di righe.

QUEST (LLM match · number) ha entrambe le cose, QUEST (exact match · string) nessuna delle due. Per questo il divario tra i due bundle, 0.2534 contro 0.2890 di F1, non è merito del modello.

## 3. Chi usa cosa

Nei file il giudice non è scritto da nessuna parte: i `summary.json` non hanno campi su modello o provider, e i log di evaluation di DocETL sono di aprile e non riportano il comando. La versione dell'evaluator si deduce dal GT (filter_2: 54 righe = vecchio, 8 = nuovo), e per QUEST il bundle ha anche una colonna `eval_variant` che vale `llm`.

**Cosa c'è nel viewer (backup)**

Ogni sorgente si chiama `sistema (giudice · GT)`, così il nome dice da solo perché due righe non sono confrontabili.

| Sorgente | Giudice | Versione evaluator | GT di filter_2 | F1 raw / adj |
|---|---|---|---|---|
| DocETL (exact match · string) | nessuno | prima di `047aea3` | 54 righe | 0.1418 / 0.2465 |
| RVCL (exact match · string) | nessuno | prima | 54 righe | 0.1822 / 0.1938 |
| Evaporate (exact match · string) | nessuno | prima | 54 righe | 0.0620 / 0.1666 |
| QUEST (exact match · string) | nessuno | prima | 54 righe | 0.2534 / 0.3232 |
| QUEST (LLM match · number) | LLM su Azure | dopo | 8 righe | 0.2890 / 0.3588 |

**Cosa dice la tesi (capitolo 8)**

| Modello | Giudice | Versione evaluator | F1 raw / adj |
|---|---|---|---|
| DocETL | LLM su Azure, deployment non registrato | prima | 0.2145 / 0.3192 |
| RVCL | LLM su Azure, deployment non registrato | prima | 0.2733 / 0.2849 |
| Evaporate | LLM su Azure, `gpt-4.1-mini_SIMONE` | prima | 0.0753 / 0.1799 |
| QUEST | LLM su Azure, deployment non registrato | dopo | 0.2890 / 0.3588 |

Tre conseguenze:

1. Nel backup l'unica valutazione con giudice è quella di QUEST. Le altre tre righe della seconda tabella sono esattamente ciò che manca e che è stato chiesto a Simone.
2. Nel viewer sono confrontabili tra loro solo le quattro sorgenti senza giudice. QUEST (LLM match · number) sta in una categoria a parte e va letto come "il numero della tesi", non come "QUEST è migliore".
3. Nella tesi il confronto non è del tutto omogeneo: tutti hanno il giudice, ma QUEST usa la versione nuova dell'evaluator e gli altri tre quella vecchia.

C'è anche un terzo asse, minore: in filter_15 DocETL ed Evaporate hanno un GT di 93 righe invece di 77, perché il loro script riscrive la query. Non dipende né dal giudice né dalla versione.



Simone: 
Mi pare di aver letto che la discrepanza è solo nella valutazione, quindi nel caso non sarebbe da rilanciare tutto ma solo la valutazione ma sono abbastanza sicuro non serve perché la mia era aggiornata
Quindi:
dobbiamo dire allo studente di:
-  capire como attivare/disattivare il giudice LLM e capire bene cosa cambiano le due modalità
-  capire se l'evaluator è ok ora che è committato
-  rilanciare solo la valutazione