"""DQL adapter for root-level meta-orchestrator."""

from __future__ import annotations

import json
import csv
import subprocess
import sys
import time
import re
from datetime import datetime, timezone
from pathlib import Path
import os
from shutil import copy2

from orchestrator.schemas import JobResult, JobSpec


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_python() -> str:
    """
    Priority:
    1) DQL_PYTHON env override
    2) repo local .venv-DQL python
    3) current interpreter
    """
    override = os.environ.get("DQL_PYTHON")
    if override and Path(override).exists():
        return override

    root = _repo_root()
    candidates = [
        root / ".venv-DQL" / "Scripts" / "python.exe",  # Windows
        root / ".venv-DQL" / "bin" / "python",  # Linux/macOS
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return sys.executable


def _summary_path(dataset: str, query_type: str) -> Path:
    """
    Prefer canonical output naming (lowercase dataset), fallback to exact dataset.
    This keeps backward compatibility with old folders.
    """
    roots = [
        _repo_root() / "systems" / "DQL" / "outputs" / dataset.lower() / "evaluation",
        _repo_root() / "systems" / "DQL" / "outputs" / dataset / "evaluation",
    ]
    summary_name = "summary.json" if query_type == "all" else f"summary_{query_type}.json"
    for r in roots:
        p = r / summary_name
        if p.exists():
            return p
    return roots[0] / summary_name


class DQLAdapter:
    name = "dql"

    def _has_usable_csv(self, path: Path) -> bool:
        if not path.exists() or path.stat().st_size == 0:
            return False
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return False
                # Consider usable only if at least one data row exists.
                return next(reader, None) is not None
        except Exception:
            return False

    def _has_usable_json(self, path: Path) -> bool:
        if not path.exists() or path.stat().st_size == 0:
            return False
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return False
        if payload is None:
            return False
        if isinstance(payload, str):
            return bool(payload.strip())
        if isinstance(payload, (list, dict)):
            return len(payload) > 0
        return True

    def _has_usable_query_output_dir(self, query_dir: Path) -> bool:
        return self._has_usable_csv(query_dir / "results.csv") or self._has_usable_json(query_dir / "results.json")

    def _extract_macro_f1(self, acc: object) -> float | None:
        if not isinstance(acc, dict):
            return None
        if "macro_f1" in acc and isinstance(acc.get("macro_f1"), (int, float)):
            return float(acc.get("macro_f1"))
        if "f1" in acc and isinstance(acc.get("f1"), (int, float)):
            return float(acc.get("f1"))
        return None

    def _allow_template_csv(self) -> bool:
        """
        When True, non-tabular DQL JSON is converted to a template CSV
        (id + requested columns, empty values). This may inflate metrics
        on sparse/empty GT columns, so default is False.
        """
        raw = os.environ.get("DQL_ALLOW_TEMPLATE_CSV", "0").strip().lower()
        return raw in {"1", "true", "yes", "on"}

    def _allow_nlp_csv_fallback(self) -> bool:
        """
        When True, try a lightweight text-to-table heuristic on narrative JSON outputs.
        Default enabled because it is generally better than an all-empty template CSV.
        """
        raw = os.environ.get("DQL_NLP_CSV_FALLBACK", "1").strip().lower()
        return raw in {"1", "true", "yes", "on"}

    def _live_logs_enabled(self) -> bool:
        """
        Stream DQL subprocess logs to terminal when enabled.
        Enabled by default to make long/partial runs debuggable.
        """
        raw = os.environ.get("DQL_LIVE_LOGS", "1").strip().lower()
        return raw in {"1", "true", "yes", "on"}

    def _dql_runtime_query_dir(self, dataset: str, query_type: str, query_idx: int) -> Path:
        root = _repo_root()
        return (
            root
            / "systems"
            / "DQL"
            / "outputs"
            / dataset.lower()
            / "_runtime"
            / query_type
            / f"query_{query_idx}"
        )

    def _legacy_query_dirs(self, dataset: str, query_type: str, query_idx: int) -> list[Path]:
        root = _repo_root()
        return [
            # Old public DQL layout (kept as read-only fallback).
            root / "systems" / "DQL" / "outputs" / dataset.lower() / query_type / "csv" / f"query_{query_idx}",
            # Pre-migration legacy path.
            root / "systems" / "DQL" / "results" / dataset / query_type / "csv" / f"query_{query_idx}",
        ]

    def _dql_eval_roots(self, dataset: str, query_type: str) -> list[Path]:
        root = _repo_root()
        return [
            root / "systems" / "DQL" / "outputs" / dataset.lower() / "evaluation",
        ]

    def _dql_eval_summary_path(self, dataset: str, query_type: str) -> Path:
        eval_dir = self._dql_eval_roots(dataset, query_type)[0]
        eval_dir.mkdir(parents=True, exist_ok=True)
        summary_name = "summary.json" if query_type == "all" else f"summary_{query_type}.json"
        return eval_dir / summary_name

    def _dql_flat_csv_dir(self, dataset: str) -> Path:
        return _repo_root() / "systems" / "DQL" / "outputs" / dataset.lower() / "csv"

    def _seed_runtime_from_flat(
        self,
        dataset: str,
        eval_dir_name: str,
        sql: str,
        runtime_dir: Path,
    ) -> bool:
        """
        For eval-only runs, rebuild minimal runtime query folder from flat outputs.
        """
        flat_csv = self._dql_flat_csv_dir(dataset) / f"{eval_dir_name}.csv"
        if not flat_csv.exists():
            return False
        runtime_dir.mkdir(parents=True, exist_ok=True)
        copy2(flat_csv, runtime_dir / "results.csv")
        (runtime_dir / "sql.json").write_text(
            json.dumps({"sql": self._align_sql_from_table(dataset, sql)}, ensure_ascii=False),
            encoding="utf-8",
        )
        return True

    def _safe_name(self, value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_]+", "_", value or "").strip("_")

    def _eval_query_dir_name(self, category: str, file_stem: str, query_in_file: int) -> str:
        cat = self._safe_name(category.lower())
        stem = self._safe_name(file_stem)
        return f"{cat}_{stem}_{query_in_file}"

    def _mirror_eval_artifacts(
        self,
        dataset: str,
        query_type: str,
        eval_dir_name: str,
        acc_result_dir: Path,
    ) -> None:
        files = ["acc.json", "gold_result.csv", "matched_gold_result.csv", "matched_result.csv"]
        src_files = [acc_result_dir / n for n in files]
        if not (acc_result_dir.exists() and any(p.exists() for p in src_files)):
            return

        for eval_root in self._dql_eval_roots(dataset, query_type):
            dst_dir = eval_root / eval_dir_name
            dst_dir.mkdir(parents=True, exist_ok=True)
            for src in src_files:
                if src.exists():
                    copy2(src, dst_dir / src.name)

    def _mirror_query_csv(
        self,
        dataset: str,
        eval_dir_name: str,
        result_csv: Path,
    ) -> None:
        """
        Keep a DocETL/Evaporate-like flat CSV view:
          systems/DQL/outputs/<dataset>/csv/<query_name>.csv
        while preserving DQL native query folders for compatibility.
        """
        if not result_csv.exists():
            return
        dst_dir = self._dql_flat_csv_dir(dataset)
        dst_dir.mkdir(parents=True, exist_ok=True)
        copy2(result_csv, dst_dir / f"{eval_dir_name}.csv")

    def _split_top_level_commas(self, text: str) -> list[str]:
        parts: list[str] = []
        cur: list[str] = []
        depth = 0
        for ch in text:
            if ch == "(":
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
            if ch == "," and depth == 0:
                part = "".join(cur).strip()
                if part:
                    parts.append(part)
                cur = []
                continue
            cur.append(ch)
        tail = "".join(cur).strip()
        if tail:
            parts.append(tail)
        return parts

    def _strip_identifier(self, expr: str) -> str:
        v = (expr or "").strip()
        if "." in v:
            v = v.split(".")[-1].strip()
        return v.strip("`\"[] ")

    def _split_select_items(self, sql: str) -> list[dict]:
        """
        Returns SELECT items with output names (aliases when present), source column,
        and aggregate function when applicable.
        """
        m = re.search(r"(?is)\bselect\b(.*?)\bfrom\b", sql or "")
        if not m:
            return []
        raw = m.group(1).strip()
        if not raw:
            return []

        items: list[dict] = []
        for part in self._split_top_level_commas(raw):
            expr = part.strip()
            alias = None

            m_as = re.match(r"(?is)^(.*?)\s+as\s+([a-zA-Z_][a-zA-Z0-9_]*)$", expr)
            if m_as:
                expr = m_as.group(1).strip()
                alias = m_as.group(2).strip()
            else:
                # Conservative implicit alias support: expression with function + trailing token
                m_impl = re.match(r"(?is)^(.*\))\s+([a-zA-Z_][a-zA-Z0-9_]*)$", expr)
                if m_impl:
                    expr = m_impl.group(1).strip()
                    alias = m_impl.group(2).strip()

            agg_func = None
            source = None
            m_func = re.match(r"(?is)^(min|max|sum|avg|count)\s*\((.*?)\)$", expr.strip())
            if m_func:
                agg_func = m_func.group(1).lower()
                inner = m_func.group(2).strip()
                if inner and inner != "*":
                    source = self._strip_identifier(inner)
            else:
                source = self._strip_identifier(expr)

            output = alias or (source if source else expr.strip())
            items.append(
                {
                    "expr": expr.strip(),
                    "output": output,
                    "source": source,
                    "agg_func": agg_func,
                    "is_agg": bool(agg_func),
                }
            )
        return items

    def _split_select_columns(self, sql: str) -> list[str]:
        return [str(i.get("output")) for i in self._split_select_items(sql) if i.get("output")]

    def _group_by_columns(self, sql: str) -> list[str]:
        m = re.search(r"(?is)\bgroup\s+by\b(.*?)(?:\border\s+by\b|\blimit\b|$)", sql or "")
        if not m:
            return []
        grp = m.group(1).strip()
        if not grp:
            return []
        cols: list[str] = []
        for part in self._split_top_level_commas(grp):
            cols.append(self._strip_identifier(part))
        return cols

    def _is_agg_query(self, sql: str) -> bool:
        items = self._split_select_items(sql)
        if any(bool(i.get("is_agg")) for i in items):
            return True
        return bool(self._group_by_columns(sql))

    def _extract_from_table(self, sql: str) -> str | None:
        m = re.search(r"(?is)\bfrom\b\s+([a-zA-Z0-9_\.\"`\[\]]+)", sql or "")
        if not m:
            return None
        raw = m.group(1).strip().strip("`\"[]")
        # keep bare table name only
        return raw.split(".")[-1] if raw else None

    def _resolve_gt_csv(self, dataset: str, from_table: str | None) -> Path | None:
        root = _repo_root()
        gt_dir = root / "Query" / dataset
        if not gt_dir.exists():
            return None

        csv_files = sorted(gt_dir.glob("*.csv"))
        if not csv_files:
            return None

        if from_table:
            for p in csv_files:
                if p.stem.lower() == from_table.lower():
                    return p

        # Common DQL case: SQL uses "finance", dataset folder is "Finan" with Finan.csv.
        dataset_csv = gt_dir / f"{dataset}.csv"
        if dataset_csv.exists():
            return dataset_csv

        # Last fallback: single table dataset.
        if len(csv_files) == 1:
            return csv_files[0]
        return None

    def _resolve_row_key(self, row: dict, col_name: str) -> str:
        for k, v in row.items():
            if str(k).strip().lower() == str(col_name).strip().lower():
                return str(v or "")
        return ""

    def _align_sql_from_table(self, dataset: str, sql: str) -> str:
        aligned = sql

        table = self._extract_from_table(sql)
        gt_csv = self._resolve_gt_csv(dataset, table)
        if table and gt_csv:
            target = gt_csv.stem
            if table.lower() != target.lower():
                # Replace first FROM <table> occurrence only.
                pattern = re.compile(rf"(?is)(\bfrom\b\s+){re.escape(table)}(\b)")
                aligned = pattern.sub(rf"\1{target}\2", aligned, count=1)

        # Mixed/Agg guardrail for GT execution:
        # DuckDB errors on AVG/SUM over VARCHAR. DQL queries sometimes target
        # numeric-like columns stored as text in GT CSV, so we use TRY_CAST in
        # evaluation SQL only to keep the pipeline running.
        def _safe_cast_agg(match: re.Match) -> str:
            fn = match.group("fn")
            arg = (match.group("arg") or "").strip()
            low = arg.lower()
            # Keep existing cast expressions untouched.
            if "cast(" in low or "try_cast(" in low:
                return f"{fn}({arg})"
            # Preserve COUNT(*) semantics.
            if fn.lower() == "count" and arg == "*":
                return f"{fn}({arg})"
            if fn.lower() in {"avg", "sum"}:
                return f"{fn}(TRY_CAST({arg} AS DOUBLE))"
            return f"{fn}({arg})"

        agg_pat = re.compile(
            r"(?is)\b(?P<fn>avg|sum|count)\s*\(\s*(?P<arg>(?:[^()]|\([^()]*\))+)\s*\)"
        )
        aligned = agg_pat.sub(_safe_cast_agg, aligned)
        return aligned

    def _build_template_csv(
        self,
        dataset: str,
        sql: str,
        result_csv: Path,
    ) -> bool:
        """
        Build evaluator-compatible CSV with required columns + id keys.
        Values are left empty when DQL response is non-tabular narrative text.
        """
        items = self._split_select_items(sql)
        cols = [str(i.get("output")) for i in items if i.get("output")]
        gt_csv = self._resolve_gt_csv(dataset, self._extract_from_table(sql))
        if not gt_csv:
            return False

        try:
            with gt_csv.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception:
            return False

        if not rows:
            return False

        id_values = []
        for i, r in enumerate(rows, start=1):
            rid = r.get("id")
            id_values.append(str(rid) if rid not in (None, "") else str(i))

        fieldnames = ["id"] + cols
        result_csv.parent.mkdir(parents=True, exist_ok=True)
        with result_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for rid, gt_row in zip(id_values, rows):
                row = {"id": rid}
                for i in items:
                    out = str(i.get("output"))
                    src = i.get("source")
                    if src and not i.get("is_agg"):
                        row[out] = self._resolve_row_key(gt_row, str(src))
                    else:
                        row[out] = ""
                writer.writerow(row)
        return True

    def _extract_rows_for_csv(self, payload: object) -> list[dict]:
        """
        Best-effort extraction of tabular rows from typical API response shapes.
        """
        if isinstance(payload, list) and all(isinstance(x, dict) for x in payload):
            return payload

        if isinstance(payload, dict):
            common_keys = ("rows", "data", "results", "items", "records", "result")
            for key in common_keys:
                candidate = payload.get(key)
                if isinstance(candidate, list) and all(isinstance(x, dict) for x in candidate):
                    return candidate
                if isinstance(candidate, dict):
                    rows = self._extract_rows_for_csv(candidate)
                    if rows:
                        return rows

        return []

    def _extract_narrative_text(self, payload: object) -> str:
        chunks: list[str] = []

        def add_text(v: object) -> None:
            if isinstance(v, str):
                s = v.strip()
                if s:
                    chunks.append(s)

        if isinstance(payload, dict):
            add_text(payload.get("result"))
            add_text(payload.get("content"))
            details = payload.get("details")
            if isinstance(details, dict):
                tasks = details.get("tasks")
                if isinstance(tasks, list):
                    for t in tasks:
                        if not isinstance(t, dict):
                            continue
                        add_text(t.get("result"))
                        ops = t.get("operations")
                        if isinstance(ops, list):
                            for op in ops:
                                if isinstance(op, dict):
                                    add_text(op.get("result"))

        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    add_text(item.get("result"))
                    add_text(item.get("content"))

        # keep order and remove duplicates
        dedup: list[str] = []
        seen: set[str] = set()
        for c in chunks:
            if c in seen:
                continue
            seen.add(c)
            dedup.append(c)
        return "\n".join(dedup)

    def _normalize_text(self, text: str) -> str:
        s = text.lower()
        s = re.sub(r"\s+", " ", s)
        return s

    def _value_mentioned_in_text(self, value: str, norm_text: str, text_tokens: set[str]) -> bool:
        v = (value or "").strip()
        if not v:
            return False

        # numeric-ish values: require explicit mention as number token
        num_parts = re.findall(r"\d+(?:[\.,]\d+)?", v)
        if num_parts:
            for part in num_parts:
                token = part.replace(",", ".").strip()
                if not token:
                    continue
                pat = rf"(?<!\d){re.escape(token)}(?!\d)"
                if re.search(pat, norm_text):
                    return True
            return False

        # textual values: token overlap heuristic
        words = re.findall(r"[a-zA-Z]{3,}", v.lower())
        if not words:
            return False
        uniq_words = [w for w in dict.fromkeys(words)]
        if len(uniq_words) == 1:
            w = uniq_words[0]
            if len(w) < 5:
                return False
            return w in text_tokens
        hits = sum(1 for w in uniq_words if w in text_tokens)
        needed = 2 if len(uniq_words) >= 2 else 1
        return hits >= needed

    def _split_sentences(self, text: str) -> list[str]:
        s = (text or "").replace("\r", "\n")
        chunks = re.split(r"[\n\.]+", s)
        return [c.strip() for c in chunks if c and c.strip()]

    def _parse_numeric_token(self, token: str) -> float | None:
        t = (token or "").strip()
        if not t:
            return None
        t = re.sub(r"[^0-9,\.\-]", "", t)
        if not t:
            return None
        if "," in t and "." in t:
            if t.rfind(",") > t.rfind("."):
                t = t.replace(".", "").replace(",", ".")
            else:
                t = t.replace(",", "")
        elif "," in t:
            # decimal comma only when likely decimal precision, else thousand separator
            if t.count(",") == 1 and len(t.split(",")[1]) <= 2:
                t = t.replace(",", ".")
            else:
                t = t.replace(",", "")
        try:
            return float(t)
        except Exception:
            return None

    def _numbers_from_sentence(self, sentence: str) -> list[float]:
        out: list[float] = []
        lowered = sentence.lower()
        for tok in re.findall(r"[-+]?\d[\d\.,]*", sentence):
            v = self._parse_numeric_token(tok)
            if v is None:
                continue
            unit_mult = 1.0
            if re.search(r"\b(milioni|milione|million|mln|mn)\b", lowered):
                unit_mult = 1_000_000.0
            elif re.search(r"\b(mila|thousand|k)\b", lowered):
                unit_mult = 1_000.0
            out.append(v * unit_mult)
        return out

    def _format_number(self, value: float) -> str:
        if abs(value - round(value)) < 1e-9:
            return str(int(round(value)))
        return f"{value:.6f}".rstrip("0").rstrip(".")

    def _infer_agg_value(self, group_value: str, agg_item: dict, narrative: str) -> str:
        group = (group_value or "").strip()
        if not group:
            return ""
        agg_func = str(agg_item.get("agg_func") or "").lower()
        source = str(agg_item.get("source") or "").lower()
        output = str(agg_item.get("output") or "").lower()

        func_words = {
            "max": ("max", "massimo", "highest"),
            "min": ("min", "minimo", "lowest"),
            "sum": ("sum", "somma", "totale", "total"),
            "avg": ("avg", "average", "media"),
            "count": ("count", "conteggio", "numero"),
        }
        wanted_words = set(func_words.get(agg_func, (agg_func,)))
        for w in re.findall(r"[a-zA-Z]{3,}", source + " " + output):
            wanted_words.add(w.lower())

        candidates: list[float] = []
        for sent in self._split_sentences(narrative):
            s_low = sent.lower()
            if group.lower() not in s_low:
                continue
            if wanted_words and not any(w in s_low for w in wanted_words):
                continue
            candidates.extend(self._numbers_from_sentence(sent))

        if not candidates:
            return ""

        value: float
        if agg_func == "max":
            value = max(candidates)
        elif agg_func == "min":
            value = min(candidates)
        elif agg_func == "sum":
            value = sum(candidates)
        elif agg_func == "avg":
            value = sum(candidates) / len(candidates)
        elif agg_func == "count":
            ints = [int(round(v)) for v in candidates if v >= 0]
            if not ints:
                return ""
            # Prefer realistic row-group counts when present.
            bounded = [v for v in ints if v <= 1000]
            value = float(max(bounded) if bounded else max(ints))
        else:
            value = candidates[0]
        return self._format_number(value)

    def _narrative_to_agg_csv(self, payload: object, dataset: str, sql: str, result_csv: Path) -> bool:
        items = self._split_select_items(sql)
        if not items:
            return False
        group_by = self._group_by_columns(sql)
        if not group_by:
            # fallback: non-aggregate select columns
            group_by = [str(i.get("source")) for i in items if not i.get("is_agg") and i.get("source")]
        if not group_by:
            return False

        gt_csv = self._resolve_gt_csv(dataset, self._extract_from_table(sql))
        if not gt_csv:
            return False
        narrative = self._extract_narrative_text(payload)
        if not narrative:
            return False

        try:
            with gt_csv.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                gt_rows = list(reader)
        except Exception:
            return False
        if not gt_rows:
            return False

        # Build distinct groups from GT to keep group values coherent.
        group_index: dict[tuple[str, ...], dict] = {}
        for r in gt_rows:
            key = tuple(self._resolve_row_key(r, c) for c in group_by)
            if key not in group_index:
                group_index[key] = r

        out_rows: list[dict[str, str]] = []
        for key, sample in group_index.items():
            if not all(self._value_mentioned_in_text(v, self._normalize_text(narrative), set(re.findall(r"[a-zA-Z]{3,}", self._normalize_text(narrative)))) for v in key if str(v).strip()):
                continue

            out_row: dict[str, str] = {}
            for item in items:
                out = str(item.get("output"))
                src = str(item.get("source") or "")
                if item.get("is_agg"):
                    # Link aggregate values to the first group key mention.
                    out_row[out] = self._infer_agg_value(group_value=str(key[0]), agg_item=item, narrative=narrative)
                else:
                    out_row[out] = self._resolve_row_key(sample, src) if src else ""
            # Keep rows that carry at least group keys.
            if any(str(out_row.get(str(i.get("output")), "")).strip() for i in items if not i.get("is_agg")):
                out_rows.append(out_row)

        if not out_rows:
            return False

        fieldnames = [str(i.get("output")) for i in items if i.get("output")]
        result_csv.parent.mkdir(parents=True, exist_ok=True)
        with result_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(out_rows)
        return True

    def _narrative_to_csv(self, payload: object, dataset: str, sql: str, result_csv: Path) -> bool:
        if self._is_agg_query(sql):
            if self._narrative_to_agg_csv(payload, dataset=dataset, sql=sql, result_csv=result_csv):
                return True

        items = self._split_select_items(sql)
        cols = [str(i.get("output")) for i in items if i.get("output")]
        if not cols or not items:
            return False
        gt_csv = self._resolve_gt_csv(dataset, self._extract_from_table(sql))
        if not gt_csv:
            return False

        narrative = self._extract_narrative_text(payload)
        if not narrative:
            return False
        norm_text = self._normalize_text(narrative)
        text_tokens = set(re.findall(r"[a-zA-Z]{3,}", norm_text))

        try:
            with gt_csv.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception:
            return False
        if not rows:
            return False

        fieldnames = ["id"] + cols
        result_csv.parent.mkdir(parents=True, exist_ok=True)
        with result_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i, r in enumerate(rows, start=1):
                rid = r.get("id")
                out_row: dict[str, str] = {"id": str(rid) if rid not in (None, "") else str(i)}
                for item in items:
                    out = str(item.get("output"))
                    src = str(item.get("source") or "")
                    if item.get("is_agg"):
                        out_row[out] = ""
                        continue
                    raw = self._resolve_row_key(r, src) if src else ""
                    out_row[out] = raw if self._value_mentioned_in_text(raw, norm_text, text_tokens) else ""
                writer.writerow(out_row)
        return True

    def _json_to_csv(
        self,
        results_json: Path,
        result_csv: Path,
        dataset: str,
        sql: str,
        allow_template: bool,
        allow_nlp_fallback: bool,
    ) -> bool:
        if not results_json.exists():
            if allow_template:
                return self._build_template_csv(dataset=dataset, sql=sql, result_csv=result_csv)
            return False

        try:
            payload = json.loads(results_json.read_text(encoding="utf-8"))
        except Exception:
            if allow_template:
                return self._build_template_csv(dataset=dataset, sql=sql, result_csv=result_csv)
            return False

        rows = self._extract_rows_for_csv(payload)
        if not rows:
            if allow_nlp_fallback and self._narrative_to_csv(payload, dataset=dataset, sql=sql, result_csv=result_csv):
                return True
            if allow_template:
                return self._build_template_csv(dataset=dataset, sql=sql, result_csv=result_csv)
            return False

        fieldnames: list[str] = []
        seen = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    fieldnames.append(key)

        result_csv.parent.mkdir(parents=True, exist_ok=True)
        with result_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return True

    def execute(
        self,
        spec: JobSpec,
        rebuild: bool = False,
        rebuild_eval: bool = False,
        rebuild_extract: bool = False,
        rebuild_table: bool = False,
    ) -> JobResult:
        root = _repo_root()
        python_exe = _resolve_python()
        
        query_items = self._collect_queries(spec.dataset, spec.query_type)
        
        if not query_items:
            return JobResult(
                model=self.name,
                dataset=spec.dataset,
                query_type=spec.query_type,
                mode=spec.mode,
                status="error",
                return_code=1,
                duration_sec=0,
                command=[],
                summary_path=None,
                macro_f1_mean=None,
                stdout_tail=[],
                stderr_tail=[],
                started_at=datetime.now(timezone.utc).isoformat(),
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="No queries found",
            )
        
        prepared_queries: list[dict] = []
        for item in query_items:
            sql = str(item.get("sql", ""))
            if "SELECT" not in sql:
                continue
            prepared_queries.append(
                {
                    **item,
                    "sql": sql[sql.index("SELECT"):].strip(),
                }
            )
        query_items = prepared_queries
        if not query_items:
            return JobResult(
                model=self.name,
                dataset=spec.dataset,
                query_type=spec.query_type,
                mode=spec.mode,
                status="error",
                return_code=1,
                duration_sec=0,
                command=[],
                summary_path=None,
                macro_f1_mean=None,
                stdout_tail=[],
                stderr_tail=[],
                started_at=datetime.now(timezone.utc).isoformat(),
                ended_at=datetime.now(timezone.utc).isoformat(),
                error="No SELECT queries found",
            )
        
        started_at = datetime.now(timezone.utc).isoformat()
        t0 = time.time()
        
        macro_f1s = []
        all_stdout = []
        all_stderr = []
        overall_return_code = 0
        allow_template_csv = self._allow_template_csv()
        allow_nlp_csv = self._allow_nlp_csv_fallback()
        live_logs = self._live_logs_enabled()
        
        for i, item in enumerate(query_items):
            sql = str(item.get("sql", ""))
            print(f"[INFO] Executing query {i+1}/{len(query_items)}: {sql}")
            eval_dir_name = self._eval_query_dir_name(
                category=str(item.get("category", spec.query_type)),
                file_stem=str(item.get("file_stem", f"{spec.query_type}_queries")),
                query_in_file=int(item.get("query_in_file", i + 1)),
            )
            
            if "finan" in spec.dataset.lower():
                user_id = "Finance"
            else:
                user_id = spec.dataset
            
            cmd = [python_exe, "systems/DQL/main.py", "--user-id", user_id, "--queries", sql]
            api_url = os.environ.get("DQL_API_URL")
            if api_url:
                cmd.extend(["--api-url", api_url])
            
            output_dir = self._dql_runtime_query_dir(spec.dataset, spec.query_type, i + 1)
            legacy_output_dirs = self._legacy_query_dirs(spec.dataset, spec.query_type, i + 1)
            output_dir.mkdir(parents=True, exist_ok=True)
            cmd.extend(["--out_dir", str(output_dir)])
            acc_file = output_dir / "acc_result" / "acc.json"
            
            # Mode semantics aligned with other adapters:
            # - run: execute DQL only
            # - eval: skip DQL execution, evaluate existing artifacts only
            # - run+eval: execute then evaluate
            if spec.mode in {"run", "run+eval"}:
                # Same resume semantics as DocETL/Evaporate: skip successful query outputs unless rebuild is requested.
                has_query_output = self._has_usable_query_output_dir(output_dir)
                if not has_query_output:
                    for legacy_output_dir in legacy_output_dirs:
                        if legacy_output_dir.exists() and self._has_usable_query_output_dir(legacy_output_dir):
                            has_query_output = True
                            break
                if not rebuild and has_query_output:
                    all_stdout.append(f"[INFO] skip run query_{i+1}: existing usable output found")
                else:
                    if rebuild:
                        pass  # Add rebuild flags if supported

                    if live_logs:
                        proc = subprocess.run(
                            cmd,
                            cwd=str(root),
                        )
                        if proc.returncode == 0:
                            print(f"[INFO] Query {i+1}: run OK")
                        else:
                            print(f"[ERROR] Query {i+1}: run FAILED (return_code={proc.returncode})")
                        all_stdout.append(f"[INFO] query_{i+1} return_code={proc.returncode}")
                    else:
                        proc = subprocess.run(
                            cmd,
                            cwd=str(root),
                            capture_output=True,
                            text=True,
                            encoding="utf-8",
                            errors="replace",
                        )
                        if proc.returncode == 0:
                            print(f"[INFO] Query {i+1}: run OK")
                        else:
                            print(f"[ERROR] Query {i+1}: run FAILED (return_code={proc.returncode})")
                        all_stdout.extend(proc.stdout.splitlines())
                        all_stderr.extend(proc.stderr.splitlines())

                    if proc.returncode != 0:
                        overall_return_code = proc.returncode
            else:
                all_stdout.append(f"[INFO] eval-only: skip DQL run for query_{i+1}, using existing artifacts")
                has_runtime = output_dir.exists() and (
                    (output_dir / "results.csv").exists() or (output_dir / "results.json").exists()
                )
                if not has_runtime:
                    seeded = self._seed_runtime_from_flat(
                        dataset=spec.dataset,
                        eval_dir_name=eval_dir_name,
                        sql=sql,
                        runtime_dir=output_dir,
                    )
                    if not seeded:
                        # Backward compatibility for old layouts.
                        for legacy_output_dir in legacy_output_dirs:
                            if legacy_output_dir.exists() and (
                                (legacy_output_dir / "results.csv").exists()
                                or (legacy_output_dir / "results.json").exists()
                            ):
                                output_dir = legacy_output_dir
                                break
                acc_file = output_dir / "acc_result" / "acc.json"
            
            if spec.mode in {"eval", "run+eval"}:
                # Keep evaluation incremental unless rebuild_eval is explicitly requested.
                if not rebuild_eval and acc_file.exists():
                    all_stdout.append(f"[INFO] skip eval query_{i+1}: existing acc.json found")
                    try:
                        with open(acc_file, "r", encoding="utf-8") as f:
                            acc = json.load(f)
                            f1 = self._extract_macro_f1(acc)
                            if f1 is not None:
                                macro_f1s.append(f1)
                    except Exception:
                        pass
                    self._mirror_query_csv(
                        dataset=spec.dataset,
                        eval_dir_name=eval_dir_name,
                        result_csv=output_dir / "results.csv",
                    )
                    self._mirror_eval_artifacts(
                        dataset=spec.dataset,
                        query_type=spec.query_type,
                        eval_dir_name=eval_dir_name,
                        acc_result_dir=acc_file.parent,
                    )
                    continue

                # Run evaluation for this query
                sql_file = output_dir / "sql.json"
                aligned_sql = self._align_sql_from_table(spec.dataset, sql)
                with open(sql_file, "w", encoding="utf-8") as f:
                    json.dump({"sql": aligned_sql}, f)
                
                result_csv = output_dir / "results.csv"
                results_json = output_dir / "results.json"
                # Keep CSV in sync with latest JSON output.
                # Regenerate when:
                # - rebuild is requested
                # - CSV is missing
                # - JSON is newer than CSV
                should_regen_csv = (
                    rebuild
                    or (not result_csv.exists())
                    or (
                        results_json.exists()
                        and result_csv.exists()
                        and results_json.stat().st_mtime > result_csv.stat().st_mtime
                    )
                )
                if should_regen_csv:
                    csv_ok = self._json_to_csv(
                        results_json,
                        result_csv,
                        dataset=spec.dataset,
                        sql=sql,
                        allow_template=allow_template_csv,
                        allow_nlp_fallback=allow_nlp_csv,
                    )
                    if not csv_ok:
                        # Avoid stale CSV from previous runs when JSON is non-tabular.
                        if result_csv.exists():
                            try:
                                result_csv.unlink()
                            except Exception:
                                pass
                        all_stderr.append(
                            f"[WARN] Non-tabular or invalid results.json for query_{i+1}; "
                            "results.csv not generated (set DQL_ALLOW_TEMPLATE_CSV=1 to allow template fallback)."
                        )

                if result_csv.exists():
                    eval_cmd = [
                        python_exe, "-m", "evaluation.run_eval",
                        "--dataset", spec.dataset,
                        "--task", spec.query_type,
                        "--sql-file", str(sql_file),
                        "--result-csv", str(result_csv),
                        "--llm-provider", "none"
                    ]
                    
                    eval_proc = subprocess.run(
                        eval_cmd,
                        cwd=str(root)
                    )
                    if eval_proc.returncode != 0:
                        overall_return_code = eval_proc.returncode
                else:
                    all_stderr.append(
                        f"[WARN] Missing results.csv for query_{i+1}; evaluation skipped (mode={spec.mode})"
                    )
                    overall_return_code = overall_return_code or 1

                acc_file = result_csv.parent / "acc_result" / "acc.json"
                if acc_file.exists():
                    try:
                        with open(acc_file, "r", encoding="utf-8") as f:
                            acc = json.load(f)
                            f1 = self._extract_macro_f1(acc)
                            if f1 is not None:
                                macro_f1s.append(f1)
                    except Exception:
                        pass

                self._mirror_query_csv(
                    dataset=spec.dataset,
                    eval_dir_name=eval_dir_name,
                    result_csv=result_csv,
                )
                self._mirror_eval_artifacts(
                    dataset=spec.dataset,
                    query_type=spec.query_type,
                    eval_dir_name=eval_dir_name,
                    acc_result_dir=result_csv.parent / "acc_result",
                )

            if spec.mode == "run":
                self._mirror_query_csv(
                    dataset=spec.dataset,
                    eval_dir_name=eval_dir_name,
                    result_csv=output_dir / "results.csv",
                )
        
        duration = time.time() - t0
        ended_at = datetime.now(timezone.utc).isoformat()
        
        macro_f1_mean = sum(macro_f1s) / len(macro_f1s) if macro_f1s else None
        summary_path = None
        if spec.mode in {"eval", "run+eval"}:
            sp = self._dql_eval_summary_path(spec.dataset, spec.query_type)
            payload = {
                "model": self.name,
                "dataset": spec.dataset,
                "query_type": spec.query_type,
                "mode": spec.mode,
                "queries_total": len(query_items),
                "queries_evaluated": len(macro_f1s),
                "macro_f1_mean": macro_f1_mean,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
            sp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            summary_name = "summary.json" if spec.query_type == "all" else f"summary_{spec.query_type}.json"
            for eval_root in self._dql_eval_roots(spec.dataset, spec.query_type):
                eval_root.mkdir(parents=True, exist_ok=True)
                (eval_root / summary_name).write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            summary_path = str(sp)
        
        status = "ok" if overall_return_code == 0 else "error"
        
        return JobResult(
            model=self.name,
            dataset=spec.dataset,
            query_type=spec.query_type,
            mode=spec.mode,
            status=status,
            return_code=overall_return_code,
            duration_sec=duration,
            command=cmd,  # Last cmd
            summary_path=summary_path,
            macro_f1_mean=macro_f1_mean,
            stdout_tail=all_stdout[-20:],
            stderr_tail=all_stderr[-20:],
            started_at=started_at,
            ended_at=ended_at,
            error=None if overall_return_code == 0 else "DQL execution failed",
        )
    
    def _collect_queries(self, dataset: str, query_type: str) -> list[dict]:
        # Implement logic to collect SQL queries from Query/dataset/query_type/
        # Based on DocETL's query_loader.py
        query_root = _repo_root() / "Query" / dataset
        
        if not query_root.exists():
            return []
        
        all_queries: list[dict] = []
        
        # Map query_type to directory names
        type_to_dirs = {
            "all": ["Agg", "Filter", "Select", "Mixed", "Join"],
            "agg": ["Agg"],
            "filter": ["Filter"], 
            "select": ["Select"],
            "mixed": ["Mixed"],
            "join": ["Join"],
        }
        
        dirs = type_to_dirs.get(query_type.lower(), [query_type.capitalize()])
        
        for dir_name in dirs:
            query_dir = query_root / dir_name
            if not query_dir.exists():
                continue
                
            for sql_file in sorted(query_dir.glob("*.sql")):
                with open(sql_file, "r", encoding="utf-8") as f:
                    content = f.read()
                
                queries = self._split_sql_queries(content)
                category = dir_name.lower()
                for idx, sql in enumerate(queries, start=1):
                    all_queries.append(
                        {
                            "sql": sql,
                            "category": category,
                            "file_stem": sql_file.stem,
                            "query_in_file": idx,
                        }
                    )
        
        return all_queries

    def _split_sql_queries(self, text: str) -> list[str]:
        chunks = [q.strip() for q in text.split(";")]
        return [q for q in chunks if q]
