# 🚀 UDA-Bench Quest – Quick Start

This guide explains how to start the project and run a test execution.

---

## 📂 Setup & Run

From the root of the repository (UDA-Bench):

### 1. Navigate to the project directory

```bash
cd systems/quest
```

---

### 2. Start Docker containers

```bash
docker compose up --build -d
```

This will build and start all required services in the background.

---

### 3. Wait for initialization

⚠️ Wait until the database and indexes are fully built before running any test.

The system is ready when the `.db_built` file is created.

---

## ▶️ Run a Query

Execute the script inside the container:

```bash
docker exec quest_app python tests/sf1.py \
  --id ... \
  --sql ... \
  --doc ... \
  --prompt ...
```

---

## ⚙️ Parameters Explanation

### `--sql` (SQL Query)

Defines the SQL query to execute.

* Must follow standard SQL syntax
* Specifies which fields to retrieve and from which table

Example:

```sql
SELECT birth_date, olympic_gold_medals FROM player
```

---

### `--doc` (Document / Table Name)

Specifies the target document (or table) used in the query.

* Must match the dataset/index name loaded in the system
* Typically corresponds to the table referenced in the SQL query

Example:

```bash
--doc player
```

---

### `--prompt` (Schema Description for LLM)

Provides a natural language schema description used by the LLM.

* Describes each attribute requested in the query
* Helps the model understand how to interpret and extract data
* Should include formatting instructions when needed

Example:

```
birth_date: birth date of the player; use format YYYY/%-m/%-d (e.g., 1984/1/30).
olympic_gold_medals: number of Olympic gold medals the player has won (e.g., 3).
```

---

## ✅ Full Example

```bash
docker exec quest_app python tests/sf1.py \
  --id sf1 \
  --sql "SELECT birth_date, olympic_gold_medals FROM player" \
  --doc player \
  --prompt "birth_date: birth date of the player; use format YYYY/%-m/%-d (e.g., 1984/1/30).\nolympic_gold_medals: number of Olympic gold medals the player has won (e.g., 3)."
```