"""DQL adapter for root-level meta-orchestrator."""

from __future__ import annotations

import json
import csv
import subprocess
import sys
import time
import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import os
from shutil import copy2, rmtree
from urllib import request as urlrequest
from urllib import error as urlerror

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

    def __init__(self) -> None:
        self._last_llm_diag: dict[str, object] | None = None
        self._doc_index_cache: dict[str, list[dict[str, str]]] = {}

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
        When True, non-tabular DQL JSON is converted to a schema-only CSV.
        Default is True so failed conversions are evaluated as empty predictions
        instead of being skipped, without fabricating document id alignments.
        """
        raw = os.environ.get("DQL_ALLOW_TEMPLATE_CSV")
        if raw is None or not str(raw).strip():
            raw = self._dotenv_value("DQL_ALLOW_TEMPLATE_CSV")
        if raw is None or not str(raw).strip():
            raw = "1"
        raw = str(raw).strip().lower()
        return raw in {"1", "true", "yes", "on"}

    def _allow_nlp_csv_fallback(self) -> bool:
        """
        When True, enable NL -> CSV conversion via LLM for narrative JSON outputs.
        """
        raw = os.environ.get("DQL_LLM_CSV_FALLBACK")
        if raw is None:
            raw = os.environ.get("DQL_NLP_CSV_FALLBACK", "1")
        raw = raw.strip().lower()
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

    def _norm_key(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())

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

    def _file_sha256(self, path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    def _is_duplicate_acc_result(self, runtime_acc_dir: Path, eval_acc_dir: Path) -> bool:
        files = ["acc.json", "gold_result.csv", "matched_gold_result.csv", "matched_result.csv"]
        if not runtime_acc_dir.exists() or not eval_acc_dir.exists():
            return False
        for name in files:
            rp = runtime_acc_dir / name
            ep = eval_acc_dir / name
            if not (rp.exists() and ep.exists()):
                return False
            if rp.stat().st_size != ep.stat().st_size:
                return False
            if self._file_sha256(rp) != self._file_sha256(ep):
                return False
        return True

    def _prune_runtime_acc_if_duplicated(self, runtime_acc_dir: Path, eval_acc_dir: Path) -> None:
        try:
            if self._is_duplicate_acc_result(runtime_acc_dir, eval_acc_dir):
                rmtree(runtime_acc_dir)
        except Exception:
            # Best-effort cleanup only.
            pass

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

    def _row_level_value_columns(self, cols: list[str]) -> list[str]:
        value_cols: list[str] = []
        seen = {"id"}
        for col in cols:
            norm = self._norm_key(col)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            value_cols.append(col)
        return value_cols

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
        target = str(col_name or "").strip().lower()
        for k, v in row.items():
            if str(k).strip().lower() == target:
                return str(v or "")
        target_norm = re.sub(r"[^a-z0-9]+", "", target)
        if target_norm:
            for k, v in row.items():
                key_norm = re.sub(r"[^a-z0-9]+", "", str(k).strip().lower())
                if key_norm == target_norm:
                    return str(v or "")
        return ""

    def _resolve_dataset_txt_dir(self, dataset: str) -> Path | None:
        data_root = _repo_root() / "Data"
        if not data_root.exists():
            return None

        raw = (dataset or "").strip()
        candidates = [
            data_root / raw / "txt",
            data_root / raw.lower() / "txt",
            data_root / raw.capitalize() / "txt",
            data_root / raw.upper() / "txt",
        ]
        for c in candidates:
            if c.exists() and c.is_dir():
                return c

        wanted = raw.lower()
        for ds_dir in data_root.iterdir():
            if not ds_dir.is_dir():
                continue
            if ds_dir.name.lower() != wanted:
                continue
            txt_dir = ds_dir / "txt"
            if txt_dir.exists() and txt_dir.is_dir():
                return txt_dir
        return None

    def _sort_ids(self, values: list[str]) -> list[str]:
        unique = list(dict.fromkeys([str(v).strip() for v in values if str(v).strip()]))

        def _key(v: str) -> tuple[int, int | str]:
            if re.fullmatch(r"\d+", v):
                return (0, int(v))
            return (1, v.lower())

        return sorted(unique, key=_key)

    def _dataset_doc_ids(self, dataset: str) -> list[str]:
        txt_dir = self._resolve_dataset_txt_dir(dataset)
        if not txt_dir:
            return []
        ids = [p.stem.strip() for p in txt_dir.glob("*.txt") if p.stem.strip()]
        return self._sort_ids(ids)

    def _dataset_doc_texts(self, dataset: str) -> dict[str, str]:
        txt_dir = self._resolve_dataset_txt_dir(dataset)
        if not txt_dir:
            return {}
        texts: dict[str, str] = {}
        for path in sorted(txt_dir.glob("*.txt"), key=lambda p: p.name.lower()):
            try:
                texts[path.stem.strip()] = path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
        return texts

    def _dataset_doc_index(self, dataset: str) -> list[dict[str, str]]:
        key = dataset.lower()
        if key not in self._doc_index_cache:
            self._doc_index_cache[key] = [
                {
                    "id": doc_id,
                    "norm_text": self._norm_key(text),
                    "digits_text": re.sub(r"\D+", "", text or ""),
                }
                for doc_id, text in self._dataset_doc_texts(dataset).items()
            ]
        return self._doc_index_cache[key]

    def _load_selected_attributes(self, dataset: str, cols: list[str]) -> dict[str, dict[str, object]]:
        root = _repo_root()
        candidates = [
            root / "Query" / dataset / f"{dataset}_attributes.json",
            root / "Query" / dataset.capitalize() / f"{dataset.capitalize()}_attributes.json",
            root / "Query" / dataset.lower() / f"{dataset.lower()}_attributes.json",
        ]
        attr_path = next((p for p in candidates if p.exists()), None)
        if not attr_path:
            return {}
        try:
            payload = json.loads(attr_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(payload, dict):
            return {}

        all_attrs: dict[str, object] = {}
        for table_attrs in payload.values():
            if isinstance(table_attrs, dict):
                all_attrs.update(table_attrs)
        by_norm = {self._norm_key(k): v for k, v in all_attrs.items() if isinstance(v, dict)}

        selected: dict[str, dict[str, object]] = {}
        for col in cols:
            meta = by_norm.get(self._norm_key(col))
            if not isinstance(meta, dict):
                continue
            selected[col] = {
                "value_type": meta.get("value_type", ""),
                "description": meta.get("description", ""),
                "is_fixed": bool(meta.get("is_fixed", False)),
            }
        return selected

    def _project_select_row(self, row: dict, items: list[dict]) -> dict[str, str]:
        projected: dict[str, str] = {}
        for item in items:
            out = str(item.get("output") or "").strip()
            if not out:
                continue
            val = self._resolve_row_key(row, out)
            if not val:
                src = str(item.get("source") or "").strip()
                if src:
                    val = self._resolve_row_key(row, src)
            projected[out] = str(val or "")
        return projected

    def _normalize_row_id_value(self, value: object, valid_ids: set[str] | None = None) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        if not text:
            return ""

        normalized_path = text.replace("\\", "/")
        basename = normalized_path.rsplit("/", 1)[-1].strip()
        stem = basename.rsplit(".", 1)[0].strip() if "." in basename else basename

        candidates: list[str] = []
        for cand in (text, basename, stem):
            cand = str(cand or "").strip().strip('"').strip("'")
            if cand:
                candidates.append(cand)

        # Common corpus references: finance_001.txt, D1, doc 001, source: 001.
        for cand in list(candidates):
            candidates.extend(self._extract_doc_refs(cand))
            for m in re.finditer(r"(?<!\d)(\d{1,6})(?!\d)", cand):
                candidates.append(m.group(1))
            m_suffix = re.search(r"(?i)(?:^|[_\-\s])(?:finance|doc|document|source)[_\-\s]*(\d{1,6})$", cand)
            if m_suffix:
                candidates.append(m_suffix.group(1))

        dedup = [c for c in dict.fromkeys(candidates) if str(c or "").strip()]
        if valid_ids:
            for cand in dedup:
                if cand in valid_ids:
                    return cand

            by_norm = {self._norm_key(v): v for v in valid_ids}
            for cand in dedup:
                match = by_norm.get(self._norm_key(cand))
                if match:
                    return match

            numeric_matches: dict[int, str] = {}
            for vid in valid_ids:
                if re.fullmatch(r"\d+", str(vid)):
                    numeric_matches.setdefault(int(str(vid)), str(vid))
            for cand in dedup:
                if re.fullmatch(r"\d+", str(cand)):
                    match = numeric_matches.get(int(str(cand)))
                    if match:
                        return match

        # Without a dataset id index, prefer the file stem because UDA ids are
        # commonly derived from document names.
        return stem or basename or text

    def _extract_row_id(self, row: dict, valid_ids: set[str] | None = None) -> str:
        candidates = (
            "id",
            "doc_id",
            "document_id",
            "file_id",
            "row_id",
            "source_ref",
            "source_id",
            "source_name",
            "file_name",
            "filename",
        )
        for key in candidates:
            val = self._resolve_row_key(row, key)
            if val:
                return self._normalize_row_id_value(val, valid_ids=valid_ids)
        for k, v in row.items():
            key_norm = re.sub(r"[^a-z0-9]+", "", str(k).strip().lower())
            if key_norm in {
                "id",
                "docid",
                "documentid",
                "fileid",
                "rowid",
                "sourceref",
                "sourceid",
                "sourcename",
                "filename",
            }:
                return self._normalize_row_id_value(v, valid_ids=valid_ids)
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
        Build a schema-only CSV. This keeps the artifact readable without
        fabricating document rows or id alignments not present in DQL output.
        """
        items = self._split_select_items(sql)
        cols = [str(i.get("output")) for i in items if i.get("output")]
        if not cols:
            return False

        fieldnames = cols if self._is_agg_query(sql) else ["id"] + self._row_level_value_columns(cols)
        result_csv.parent.mkdir(parents=True, exist_ok=True)
        with result_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        return True

    def _extract_narrative_text(self, payload: object) -> str:
        if isinstance(payload, dict):
            result = payload.get("result")
            if isinstance(result, str) and result.strip():
                return result.strip()

            details = payload.get("details")
            if isinstance(details, dict):
                tasks = details.get("tasks")
                if isinstance(tasks, list):
                    # Prefer latest integrated answer if top-level result is missing.
                    for t in reversed(tasks):
                        if not isinstance(t, dict):
                            continue
                        ops = t.get("operations")
                        if isinstance(ops, list):
                            for op in reversed(ops):
                                if not isinstance(op, dict):
                                    continue
                                op_res = op.get("result")
                                if isinstance(op_res, str) and op_res.strip():
                                    return op_res.strip()
                        t_res = t.get("result")
                        if isinstance(t_res, str) and t_res.strip():
                            return t_res.strip()

            content = payload.get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()

        if isinstance(payload, list):
            for item in reversed(payload):
                if not isinstance(item, dict):
                    continue
                for key in ("result", "content"):
                    txt = item.get(key)
                    if isinstance(txt, str) and txt.strip():
                        return txt.strip()

        return ""

    def _strip_code_fences(self, text: str) -> str:
        s = (text or "").strip()
        if s.startswith("```"):
            s = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s)
            if s.endswith("```"):
                s = s[:-3]
        return s.strip()

    def _extract_json_payload(self, text: str) -> object | None:
        raw = self._strip_code_fences(text)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            pass

        # Fallback: first JSON object/array in text.
        candidates: list[str] = []
        obj_match = re.search(r"\{[\s\S]*\}", raw)
        if obj_match:
            candidates.append(obj_match.group(0))
        arr_match = re.search(r"\[[\s\S]*\]", raw)
        if arr_match:
            candidates.append(arr_match.group(0))

        for cand in candidates:
            try:
                return json.loads(cand)
            except Exception:
                continue
        return None

    def _gemini_api_key(self) -> str:
        key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or ""
        if not key:
            key = self._dotenv_value("GEMINI_API_KEY")
        return key.strip().strip('"').strip("'")

    def _dotenv_value(self, key: str) -> str:
        env_file = _repo_root() / ".env"
        if not env_file.exists():
            return ""
        try:
            for line in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
                raw = line.strip()
                if not raw or raw.startswith("#") or "=" not in raw:
                    continue
                k, v = raw.split("=", 1)
                if k.strip() == key:
                    return v.strip().strip('"').strip("'")
        except Exception:
            return ""
        return ""

    def _gemini_model(self) -> str:
        model = (os.environ.get("DQL_GEMINI_MODEL") or "gemini-2.0-flash").strip()
        return model or "gemini-2.0-flash"

    def _llm_backend(self) -> str:
        raw = (os.environ.get("DQL_LLM_BACKEND") or "").strip().lower()
        if not raw:
            raw = self._dotenv_value("DQL_LLM_BACKEND").strip().lower()
        if raw:
            return raw
        if self._llm_openai_api_base():
            return "openai"
        return "gemini"

    def _llm_openai_api_base(self) -> str:
        base = (os.environ.get("DQL_LLM_API_BASE") or "").strip()
        if not base:
            base = self._dotenv_value("DQL_LLM_API_BASE")
        return base.rstrip("/")

    def _llm_openai_api_key(self) -> str:
        key = os.environ.get("DQL_LLM_API_KEY") or os.environ.get("OPENAI_API_KEY") or ""
        if not key:
            key = self._dotenv_value("DQL_LLM_API_KEY") or self._dotenv_value("OPENAI_API_KEY")
        return key.strip().strip('"').strip("'")

    def _llm_openai_model(self) -> str:
        model = (os.environ.get("DQL_LLM_MODEL") or "").strip()
        if not model:
            model = self._dotenv_value("DQL_LLM_MODEL")
        if not model:
            model = "Qwen3-8B-Q5"
        return model or "Qwen3-8B-Q5"

    def _call_openai_compatible(self, prompt: str) -> str:
        api_base = self._llm_openai_api_base()
        api_key = self._llm_openai_api_key()
        if not api_base or not api_key:
            return ""

        timeout_sec = int(os.environ.get("DQL_LLM_TIMEOUT_SEC", "60"))
        model = self._llm_openai_model()
        url = f"{api_base}/chat/completions"
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
        }

        req = urlrequest.Request(
            url=url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except (urlerror.URLError, TimeoutError, ValueError):
            return ""
        except Exception:
            return ""

        try:
            parsed = json.loads(raw)
        except Exception:
            return ""

        choices = parsed.get("choices")
        if not isinstance(choices, list) or not choices:
            return ""
        first = choices[0]
        if not isinstance(first, dict):
            return ""
        msg = first.get("message")
        if not isinstance(msg, dict):
            return ""
        content = msg.get("content")
        if isinstance(content, str):
            return content.strip()
        return ""

    def _call_gemini(self, prompt: str) -> str:
        api_key = self._gemini_api_key()
        if not api_key:
            return ""

        model = self._gemini_model()
        timeout_sec = int(os.environ.get("DQL_LLM_TIMEOUT_SEC", "60"))
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            f"?key={api_key}"
        )
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0,
                "responseMimeType": "application/json",
            },
        }

        req = urlrequest.Request(
            url=url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except (urlerror.URLError, TimeoutError, ValueError):
            return ""
        except Exception:
            return ""

        try:
            parsed = json.loads(raw)
        except Exception:
            return ""

        candidates = parsed.get("candidates")
        if not isinstance(candidates, list):
            return ""
        for c in candidates:
            if not isinstance(c, dict):
                continue
            content = c.get("content")
            if not isinstance(content, dict):
                continue
            parts = content.get("parts")
            if not isinstance(parts, list):
                continue
            texts: list[str] = []
            for p in parts:
                if isinstance(p, dict):
                    t = p.get("text")
                    if isinstance(t, str) and t.strip():
                        texts.append(t.strip())
            if texts:
                return "\n".join(texts)
        return ""

    def _call_llm(self, prompt: str) -> str:
        backend = self._llm_backend()
        if backend == "openai":
            return self._call_openai_compatible(prompt)
        return self._call_gemini(prompt)

    def _value_in_text(self, value: str, text: str) -> bool:
        v = str(value or "").strip()
        if not v:
            return False
        nv = self._norm_key(v)
        nt = self._norm_key(text)
        if len(nv) < 2:
            return False
        if nv in nt:
            return True

        # Numeric fallback: tolerate locale/currency formatting differences
        # (e.g., 1,920,000,000 vs 1.920.000.000).
        vd = re.sub(r"\D+", "", v)
        td = re.sub(r"\D+", "", text or "")
        if len(vd) >= 3 and vd in td:
            return True
        return False

    def _value_in_normalized_text(self, value: str, norm_text: str, digits_text: str) -> bool:
        v = str(value or "").strip()
        if not v:
            return False
        nv = self._norm_key(v)
        if len(nv) >= 2 and nv in norm_text:
            return True
        vd = re.sub(r"\D+", "", v)
        return len(vd) >= 3 and vd in digits_text

    def _extract_doc_refs(self, text: str) -> list[str]:
        s = str(text or "")
        out: list[str] = []
        # Common DQL citation style: D1, D.1, d 1, etc.
        for m in re.finditer(r"(?i)\bd\s*[\.\-_:]?\s*(\d{1,4})\b", s):
            out.append(m.group(1))
        # Other textual mentions: doc 1, document #2, etc.
        for m in re.finditer(r"(?i)\bdoc(?:ument)?\s*#?\s*(\d{1,4})\b", s):
            out.append(m.group(1))
        uniq = list(dict.fromkeys([v.strip() for v in out if v and v.strip()]))
        return self._sort_ids(uniq)

    def _row_id_candidates(self, row: dict, valid_ids: set[str]) -> list[str]:
        raw_candidates: list[str] = []

        # Prefer explicit id-like fields first.
        id_key_norms = {
            "id",
            "ids",
            "docid",
            "documentid",
            "doc",
            "document",
            "source",
            "sourceid",
            "sourceref",
            "sourcerefs",
            "sourcename",
            "docref",
            "docrefs",
            "documentref",
            "documentrefs",
            "docids",
            "documentids",
            "fileid",
            "filename",
            "filenames",
            "filepath",
        }
        for key, val in row.items():
            if self._norm_key(key) not in id_key_norms:
                continue
            if val is None:
                continue
            if isinstance(val, list):
                for item in val:
                    if item is None:
                        continue
                    sitem = str(item).strip()
                    if not sitem:
                        continue
                    norm_id = self._normalize_row_id_value(sitem, valid_ids=valid_ids)
                    if norm_id:
                        raw_candidates.append(norm_id)
                    raw_candidates.extend(self._extract_doc_refs(sitem))
                continue
            sval = str(val).strip()
            if not sval:
                continue
            norm_id = self._normalize_row_id_value(sval, valid_ids=valid_ids)
            if norm_id:
                raw_candidates.append(norm_id)
            raw_candidates.extend(self._extract_doc_refs(sval))

        # Fallback: scan whole row textual payload (including evidence/source notes).
        if not raw_candidates:
            row_text = " ".join([str(v or "") for v in row.values()])
            raw_candidates.extend(self._extract_doc_refs(row_text))

        uniq = self._sort_ids(list(dict.fromkeys([c for c in raw_candidates if c])))
        if valid_ids:
            uniq = [c for c in uniq if c in valid_ids]
        return uniq

    def _llm_narrative_rows(self, payload: object, dataset: str, sql: str) -> tuple[list[dict], dict[str, object]]:
        diag: dict[str, object] = {
            "llm_backend": self._llm_backend(),
            "llm_called": 0,
            "llm_second_pass": 0,
            "llm_no_response": 0,
            "llm_invalid_json": 0,
            "llm_no_rows_payload": 0,
            "llm_rows_candidate": 0,
            "llm_rows_kept": 0,
            "llm_rows_unlocalized": 0,
            "llm_rows_linked": 0,
            "llm_rows_ambiguous": 0,
            "llm_rows_unlinked": 0,
            "llm_rows_scartate_per_id": 0,
            "llm_rows_too_many_ids": 0,
            "llm_rows_scartate_per_value": 0,
            "llm_cells_scartate_per_value": 0,
        }
        narrative = self._extract_narrative_text(payload)
        if not narrative:
            diag["llm_no_rows_payload"] = 1
            return [], diag
        max_chars = int(os.environ.get("DQL_LLM_MAX_CHARS", "60000"))
        if max_chars > 0 and len(narrative) > max_chars:
            narrative = narrative[:max_chars]

        items = self._split_select_items(sql)
        cols = [str(i.get("output") or "").strip() for i in items if str(i.get("output") or "").strip()]
        if not cols:
            diag["llm_no_rows_payload"] = 1
            return [], diag

        attr_schema = self._load_selected_attributes(dataset, cols)
        attr_hint = ""
        if attr_schema:
            attr_hint = (
                "Schema descrittivo delle colonne richieste. Usalo solo per capire il significato dei campi; "
                "non usarlo per inventare, completare o correggere valori assenti dal testo:\n"
                f"{json.dumps(attr_schema, ensure_ascii=False)}\n\n"
            )

        is_agg = self._is_agg_query(sql)
        requires_id = not is_agg
        schema_hint_strict = (
            '{"rows":[{"id":"<explicit_doc_id_or_empty>","<col1>":"<value>","<col2>":"<value>","evidence":"<exact quote from text>"}]}'
            if requires_id
            else '{"rows":[{"<col1>":"<value>","<col2>":"<value>","evidence":"<exact quote from text>"}]}'
        )
        schema_hint_relaxed = (
            '{"rows":[{"id":"<doc_id_or_empty>","doc_refs":["D1","D2"],"<col1>":"<value_or_empty>","<col2>":"<value_or_empty>","evidence":"<quote>"}]}'
            if requires_id
            else '{"rows":[{"<col1>":"<value_or_empty>","<col2>":"<value_or_empty>","evidence":"<quote>"}]}'
        )

        def _build_prompt(relaxed: bool) -> str:
            base = (
                "Converti il testo in output JSON per una query SQL.\n"
                "Regole obbligatorie:\n"
                "1) Usa SOLO valori espliciti presenti nel testo fornito.\n"
                "2) Non inventare valori e non usare conoscenza esterna.\n"
                "3) Produci SOLO JSON valido.\n"
                "4) Se non trovi dati, restituisci {\"rows\": []}.\n"
                "5) Includi solo righe con almeno una colonna non vuota.\n"
                f"6) Colonne richieste: {cols}.\n"
                "7) Per ogni riga aggiungi 'evidence' con una breve frase/copiastralcio del testo che supporta i valori.\n"
                "8) Estrai candidati in modo completo: se il testo contiene piu' valori plausibili, restituiscili tutti (senza duplicati identici).\n"
                "9) Non lasciare vuoto un campo se nel testo e' presente un valore letterale per quella colonna.\n"
                "10) Mantieni i valori il piu' possibile letterali (non normalizzare, non tradurre, non convertire unita').\n"
            )
            if requires_id:
                if relaxed:
                    base += (
                        "11) Query non aggregata: compila 'id' solo se nel testo compare un riferimento esplicito al documento/riga.\n"
                        "12) Se l'id non e' esplicito, lascia id vuoto; non dedurlo da ordine, posizione, valori o conoscenza esterna.\n"
                        "13) Compila 'doc_refs' solo con riferimenti presenti nel testo (es. D1, D2, doc 3).\n"
                    )
                else:
                    base += (
                        "11) Query non aggregata: includi 'id' solo quando il testo contiene riferimenti tipo D1, D2, doc 3.\n"
                        "12) Se una frase contiene valori ma nessun id esplicito, restituisci comunque i valori con id vuoto.\n"
                        "13) Non inventare id e non assegnare documenti per somiglianza o per ordine di apparizione.\n"
                    )
            schema = schema_hint_relaxed if relaxed else schema_hint_strict
            return base + (
                f"SQL:\n{sql}\n\n"
                f"{attr_hint}"
                f"Formato JSON richiesto:\n{schema}\n\n"
                f"TESTO:\n{narrative}\n"
            )

        def _extract_rows(raw_text: str) -> tuple[list[object], bool]:
            parsed = self._extract_json_payload(raw_text)
            rows_local: list[object] = []
            if isinstance(parsed, dict):
                for k in ("rows", "data", "items", "results"):
                    cand = parsed.get(k)
                    if isinstance(cand, list):
                        rows_local = cand
                        break
                return rows_local, True
            if isinstance(parsed, list):
                return parsed, True
            return [], False

        diag["llm_called"] = 1
        raw = self._call_llm(_build_prompt(relaxed=False))
        if not str(raw or "").strip():
            diag["llm_no_response"] = 1
            return [], diag
        rows, ok_shape = _extract_rows(raw)
        if (not ok_shape) or (not rows):
            diag["llm_second_pass"] = 1
            raw2 = self._call_llm(_build_prompt(relaxed=True))
            if str(raw2 or "").strip():
                rows2, ok_shape2 = _extract_rows(raw2)
                if ok_shape2:
                    rows = rows2
                    ok_shape = True

        if not ok_shape:
            diag["llm_invalid_json"] = 1
            return [], diag
        if not rows:
            diag["llm_no_rows_payload"] = 1
            return [], diag
        diag["llm_rows_candidate"] = len(rows)

        out_rows: list[dict] = []
        valid_ids = set(self._dataset_doc_ids(dataset))
        for row in rows:
            if not isinstance(row, dict):
                continue

            cleaned_values: dict[str, str] = {}
            non_empty = 0
            for c in cols:
                v = str(self._resolve_row_key(row, c) or "").strip()
                if not v:
                    cleaned_values[c] = ""
                    continue
                # Keep only values traceable to narrative text.
                if not self._value_in_text(v, narrative):
                    cleaned_values[c] = ""
                    diag["llm_cells_scartate_per_value"] = int(diag["llm_cells_scartate_per_value"]) + 1
                    continue
                cleaned_values[c] = v
                non_empty += 1

            if non_empty == 0:
                diag["llm_rows_scartate_per_value"] = int(diag["llm_rows_scartate_per_value"]) + 1
                continue

            id_candidates: list[str] = [""]
            if requires_id:
                id_candidates = self._row_id_candidates(row, valid_ids)
                if not id_candidates:
                    diag["llm_rows_unlocalized"] = int(diag["llm_rows_unlocalized"]) + 1
                    id_candidates = [""]
                # Avoid exploding ambiguous rows to too many ids.
                if len(id_candidates) > 20:
                    diag["llm_rows_too_many_ids"] = int(diag["llm_rows_too_many_ids"]) + 1
                    continue

            for rid in id_candidates:
                cleaned: dict[str, str] = {}
                if requires_id:
                    cleaned["id"] = rid
                for c in cols:
                    cleaned[c] = cleaned_values.get(c, "")
                evidence = str(self._resolve_row_key(row, "evidence") or "").strip()
                if evidence:
                    cleaned["evidence"] = evidence
                out_rows.append(cleaned)

        diag["llm_rows_kept"] = len(out_rows)
        return out_rows, diag

    def _field_match(
        self,
        col: str,
        value: str,
        norm_text: str,
        digits_text: str,
        attr_schema: dict[str, dict[str, object]],
    ) -> dict[str, object] | None:
        raw = str(value or "").strip()
        if not raw:
            return None

        parts = [p.strip() for p in raw.split("||") if p.strip()] if "||" in raw else [raw]
        matched = [p for p in parts if self._value_in_normalized_text(p, norm_text, digits_text)]
        if not matched and self._value_in_normalized_text(raw, norm_text, digits_text):
            matched = [raw]
        if not matched:
            return None

        meta = attr_schema.get(col, {})
        value_type = str(meta.get("value_type", "") if isinstance(meta, dict) else "")
        generic = {"yes", "no", "other", "fixed", "mixed", "notdisclosed", "0"}
        all_generic = all(self._norm_key(p) in generic for p in matched)

        if self._norm_key(col) == "companyname":
            weight = 3.0
        elif value_type in {"int", "float"}:
            weight = 1.0
        elif all_generic:
            weight = 0.5
        else:
            weight = 2.0

        coverage = len(matched) / max(1, len(parts))
        return {
            "field": col,
            "score": max(0.5, weight * coverage),
            "matched_values": matched[:5],
        }

    def _link_row_to_docs(
        self,
        row: dict,
        dataset: str,
        cols: list[str],
        attr_schema: dict[str, dict[str, object]],
    ) -> dict[str, object]:
        docs = self._dataset_doc_index(dataset)
        values = {c: str(row.get(c, "") or "").strip() for c in cols if str(row.get(c, "") or "").strip()}
        if not docs or not values:
            return {"status": "unlocalized", "id": None, "candidate_docs": [], "reason": "no_documents_or_values"}

        candidates: list[dict[str, object]] = []
        for doc in docs:
            doc_id = doc["id"]
            norm_text = doc["norm_text"]
            digits_text = doc["digits_text"]
            matches: list[dict[str, object]] = []
            score = 0.0
            for col, value in values.items():
                match = self._field_match(col, value, norm_text, digits_text, attr_schema)
                if match:
                    matches.append(match)
                    score += float(match["score"])
            if score > 0:
                candidates.append(
                    {
                        "id": doc_id,
                        "score": round(score, 3),
                        "matched_fields": [str(m["field"]) for m in matches],
                        "matches": matches,
                    }
                )

        candidates.sort(key=lambda item: (-float(item["score"]), str(item["id"])))
        top = candidates[:5]
        if not top:
            return {"status": "unlocalized", "id": None, "candidate_docs": [], "reason": "no_tuple_match"}

        best = top[0]
        second_score = float(top[1]["score"]) if len(top) > 1 else 0.0
        matched_fields = set(str(f) for f in best.get("matched_fields", []))
        strong_enough = "company_name" in matched_fields or len(matched_fields) >= 2
        separated = len(top) == 1 or (float(best["score"]) - second_score) >= 1.0

        if float(best["score"]) >= 2.0 and strong_enough and separated:
            return {
                "status": "linked",
                "id": str(best["id"]),
                "candidate_docs": top,
                "reason": "unique_tuple_match",
            }

        return {
            "status": "ambiguous",
            "id": None,
            "candidate_docs": top,
            "reason": "weak_or_non_unique_tuple_match",
        }

    def _localize_rows(
        self,
        rows: list[dict],
        dataset: str,
        sql: str,
        cols: list[str],
        attr_schema: dict[str, dict[str, object]],
        diag: dict[str, object],
    ) -> list[dict]:
        if self._is_agg_query(sql):
            for row in rows:
                row["_localization"] = {"status": "not_required", "id": None, "candidate_docs": []}
            diag["llm_rows_linked"] = len(rows)
            return rows

        valid_ids = set(self._dataset_doc_ids(dataset))
        linked_rows: list[dict] = []
        for row in rows:
            rid = self._extract_row_id(row, valid_ids=valid_ids)
            if rid and (not valid_ids or rid in valid_ids):
                row["_localization"] = {"status": "explicit", "id": rid, "candidate_docs": []}
                linked_rows.append(row)
                diag["llm_rows_linked"] = int(diag.get("llm_rows_linked", 0)) + 1
                continue

            loc = self._link_row_to_docs(row, dataset, cols, attr_schema)
            row["_localization"] = loc
            if loc.get("status") == "linked" and loc.get("id"):
                row["id"] = str(loc["id"])
                linked_rows.append(row)
                diag["llm_rows_linked"] = int(diag.get("llm_rows_linked", 0)) + 1
            elif loc.get("status") == "ambiguous":
                diag["llm_rows_ambiguous"] = int(diag.get("llm_rows_ambiguous", 0)) + 1
                diag["llm_rows_scartate_per_id"] = int(diag.get("llm_rows_scartate_per_id", 0)) + 1
            else:
                diag["llm_rows_unlinked"] = int(diag.get("llm_rows_unlinked", 0)) + 1
                diag["llm_rows_scartate_per_id"] = int(diag.get("llm_rows_scartate_per_id", 0)) + 1
        return linked_rows

    def _write_intermediate_rows(
        self,
        result_csv: Path,
        dataset: str,
        sql: str,
        cols: list[str],
        attr_schema: dict[str, dict[str, object]],
        rows: list[dict],
        diag: dict[str, object],
    ) -> None:
        payload = {
            "dataset": dataset,
            "sql": sql,
            "query_type": "aggregation" if self._is_agg_query(sql) else "row_level",
            "columns": attr_schema,
            "rows": [],
            "diagnostics": diag,
        }
        for row in rows:
            loc = row.get("_localization")
            payload["rows"].append(
                {
                    "values": {c: str(row.get(c, "") or "") for c in cols},
                    "evidence": str(row.get("evidence", "") or ""),
                    "localization": loc if isinstance(loc, dict) else {"status": "unlocalized", "id": None},
                }
            )

        path = result_csv.with_name("intermediate_rows.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _narrative_to_csv(self, payload: object, dataset: str, sql: str, result_csv: Path) -> bool:
        rows, diag = self._llm_narrative_rows(payload=payload, dataset=dataset, sql=sql)
        self._last_llm_diag = diag
        items = self._split_select_items(sql)
        cols = [str(i.get("output")) for i in items if i.get("output")]
        attr_schema = self._load_selected_attributes(dataset, cols)
        if not rows:
            self._write_intermediate_rows(result_csv, dataset, sql, cols, attr_schema, [], diag)
            return False
        linked_rows = self._localize_rows(rows, dataset, sql, cols, attr_schema, diag)
        self._last_llm_diag = diag
        self._write_intermediate_rows(result_csv, dataset, sql, cols, attr_schema, rows, diag)
        if not linked_rows:
            return False
        return self._rows_to_csv(linked_rows, dataset=dataset, sql=sql, result_csv=result_csv)

    def _rows_to_csv(self, rows: list[dict], dataset: str, sql: str, result_csv: Path) -> bool:
        if not rows:
            return False
        items = self._split_select_items(sql)
        cols = [str(i.get("output")) for i in items if i.get("output")]
        if not items or not cols:
            return False

        result_csv.parent.mkdir(parents=True, exist_ok=True)

        if self._is_agg_query(sql):
            fieldnames = cols
            out_rows: list[dict[str, str]] = []
            for r in rows:
                projected = self._project_select_row(r, items)
                if any(str(projected.get(c, "")).strip() for c in cols):
                    out_rows.append({c: str(projected.get(c, "")) for c in cols})
            if not out_rows:
                return False
            with result_csv.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(out_rows)
            return True

        value_cols = self._row_level_value_columns(cols)
        valid_ids = set(self._dataset_doc_ids(dataset))
        id_to_row: dict[str, dict[str, str]] = {}
        for r in rows:
            rid = self._extract_row_id(r, valid_ids=valid_ids)
            if not rid:
                continue
            projected = self._project_select_row(r, items)
            if rid not in id_to_row:
                id_to_row[rid] = {"id": rid, **{c: "" for c in value_cols}}
            for c in value_cols:
                val = str(projected.get(c, "")).strip()
                if val:
                    id_to_row[rid][c] = val

        if not id_to_row:
            return False

        ordered_ids = self._sort_ids(list(id_to_row.keys()))

        fieldnames = ["id"] + value_cols
        with result_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for rid in ordered_ids:
                row = {"id": rid, **{c: "" for c in value_cols}}
                if rid in id_to_row:
                    for c in value_cols:
                        row[c] = id_to_row[rid].get(c, "")
                writer.writerow(row)
        return True

    def _json_needs_clarification(self, payload: object) -> bool:
        if not isinstance(payload, dict):
            return False
        details = payload.get("details")
        if not isinstance(details, dict):
            return False
        sql_details = details.get("sql")
        if not isinstance(sql_details, dict):
            return False
        return str(sql_details.get("mode") or "").strip().lower() == "needs_clarification"

    def _coerce_table_row(self, row: object, columns: list[str]) -> dict[str, object] | None:
        if isinstance(row, dict):
            return dict(row)
        if isinstance(row, (list, tuple)):
            return {columns[i]: row[i] if i < len(row) else "" for i in range(len(columns))}
        return None

    def _select_json_table(self, tables: object) -> dict | None:
        if not isinstance(tables, list):
            return None

        candidates: list[dict] = []
        for table in tables:
            if not isinstance(table, dict):
                continue
            columns = table.get("columns")
            rows = table.get("rows")
            if isinstance(columns, list) and isinstance(rows, list):
                candidates.append(table)

        if not candidates:
            return None

        for table in candidates:
            if str(table.get("id") or "").strip().lower() == "sql_result":
                return table
        for table in candidates:
            if table.get("rows"):
                return table
        return candidates[0]

    def _write_empty_csv_for_sql(self, sql: str, result_csv: Path) -> bool:
        items = self._split_select_items(sql)
        cols = [str(i.get("output")) for i in items if i.get("output")]
        if not cols:
            return False
        fieldnames = cols if self._is_agg_query(sql) else ["id"] + self._row_level_value_columns(cols)
        result_csv.parent.mkdir(parents=True, exist_ok=True)
        with result_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        return True

    def _table_json_to_csv(
        self,
        payload: object,
        dataset: str,
        sql: str,
        result_csv: Path,
    ) -> bool:
        if self._json_needs_clarification(payload):
            print("[DQL-TABLE] status=needs_clarification; generated empty CSV template")
            return self._write_empty_csv_for_sql(sql, result_csv)

        if not isinstance(payload, dict):
            return False
        if "tables" not in payload:
            return False

        table = self._select_json_table(payload.get("tables"))
        if table is None:
            tables = payload.get("tables")
            if isinstance(tables, list) and not tables:
                print("[DQL-TABLE] status=empty_tables; generated empty CSV template")
                return self._write_empty_csv_for_sql(sql, result_csv)
            return False

        raw_columns = table.get("columns")
        raw_rows = table.get("rows")
        if not isinstance(raw_columns, list) or not isinstance(raw_rows, list):
            return False

        columns = [str(c).strip() for c in raw_columns if str(c or "").strip()]
        if not columns:
            return self._write_empty_csv_for_sql(sql, result_csv)

        rows: list[dict] = []
        for raw_row in raw_rows:
            row = self._coerce_table_row(raw_row, columns)
            if row is not None:
                rows.append(row)

        if not rows:
            print("[DQL-TABLE] status=empty_rows; generated empty CSV template")
            return self._write_empty_csv_for_sql(sql, result_csv)

        valid_ids = set(self._dataset_doc_ids(dataset))
        is_agg = self._is_agg_query(sql)
        normalized_rows: list[dict] = []
        skipped_no_id = 0

        for row in rows:
            out_row = dict(row)
            if not is_agg:
                rid = self._extract_row_id(out_row, valid_ids=valid_ids)
                if not rid:
                    name_val = self._resolve_row_key(out_row, "name")
                    if name_val and re.search(r"(?i)\.(txt|pdf|docx?|csv|xlsx?)$", str(name_val).strip()):
                        rid = self._normalize_row_id_value(name_val, valid_ids=valid_ids)
                if not rid:
                    skipped_no_id += 1
                    continue
                out_row["id"] = rid
            normalized_rows.append(out_row)

        if not normalized_rows:
            print(
                "[DQL-TABLE] "
                f"status=no_usable_rows skipped_no_id={skipped_no_id}; generated empty CSV template"
            )
            return self._write_empty_csv_for_sql(sql, result_csv)

        ok = self._rows_to_csv(normalized_rows, dataset=dataset, sql=sql, result_csv=result_csv)
        if ok:
            print(
                "[DQL-TABLE] "
                f"status=ok rows={len(normalized_rows)} skipped_no_id={skipped_no_id}"
            )
            return True

        print("[DQL-TABLE] status=conversion_failed; generated empty CSV template")
        return self._write_empty_csv_for_sql(sql, result_csv)

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

        self._last_llm_diag = None
        # Prefer structured DQL tables when present. Narrative extraction remains
        # a fallback for legacy/non-tabular DQL responses.
        if self._table_json_to_csv(payload, dataset=dataset, sql=sql, result_csv=result_csv):
            return True

        if allow_nlp_fallback and self._narrative_to_csv(payload, dataset=dataset, sql=sql, result_csv=result_csv):
            diag = self._last_llm_diag or {}
            print(
                "[DQL-LLM] "
                f"{results_json.parent.name}: "
                f"backend={diag.get('llm_backend', '')} "
                f"status=ok "
                f"second_pass={diag.get('llm_second_pass', 0)} "
                f"rows_candidate={diag.get('llm_rows_candidate', 0)} "
                f"rows_kept={diag.get('llm_rows_kept', 0)} "
                f"unlocalized={diag.get('llm_rows_unlocalized', 0)} "
                f"linked={diag.get('llm_rows_linked', 0)} "
                f"ambiguous={diag.get('llm_rows_ambiguous', 0)} "
                f"unlinked={diag.get('llm_rows_unlinked', 0)} "
                f"drop_id={diag.get('llm_rows_scartate_per_id', 0)} "
                f"drop_value_row={diag.get('llm_rows_scartate_per_value', 0)} "
                f"drop_value_cell={diag.get('llm_cells_scartate_per_value', 0)}"
            )
            return True

        if allow_nlp_fallback:
            diag = self._last_llm_diag or {}
            print(
                "[DQL-LLM] "
                f"{results_json.parent.name}: "
                f"backend={diag.get('llm_backend', '')} "
                f"status=empty "
                f"second_pass={diag.get('llm_second_pass', 0)} "
                f"no_response={diag.get('llm_no_response', 0)} "
                f"invalid_json={diag.get('llm_invalid_json', 0)} "
                f"no_rows_payload={diag.get('llm_no_rows_payload', 0)} "
                f"rows_candidate={diag.get('llm_rows_candidate', 0)} "
                f"rows_kept={diag.get('llm_rows_kept', 0)} "
                f"unlocalized={diag.get('llm_rows_unlocalized', 0)} "
                f"linked={diag.get('llm_rows_linked', 0)} "
                f"ambiguous={diag.get('llm_rows_ambiguous', 0)} "
                f"unlinked={diag.get('llm_rows_unlinked', 0)} "
                f"drop_id={diag.get('llm_rows_scartate_per_id', 0)} "
                f"drop_value_row={diag.get('llm_rows_scartate_per_value', 0)} "
                f"drop_value_cell={diag.get('llm_cells_scartate_per_value', 0)}"
            )

        if allow_template:
            return self._build_template_csv(dataset=dataset, sql=sql, result_csv=result_csv)
        return False

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
        category_query_idx: dict[str, int] = {}
        
        for i, item in enumerate(query_items):
            sql = str(item.get("sql", ""))
            item_category = str(item.get("category", spec.query_type)).lower()
            category_query_idx[item_category] = category_query_idx.get(item_category, 0) + 1
            query_idx_in_category = category_query_idx[item_category]
            print(f"[INFO] Executing query {i+1}/{len(query_items)}: {sql}")
            eval_dir_name = self._eval_query_dir_name(
                category=item_category,
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
            
            runtime_query_type = item_category
            output_dir = self._dql_runtime_query_dir(spec.dataset, runtime_query_type, query_idx_in_category)
            legacy_output_dirs = self._legacy_query_dirs(spec.dataset, runtime_query_type, query_idx_in_category)
            output_dir.mkdir(parents=True, exist_ok=True)
            cmd.extend(["--out_dir", str(output_dir)])
            runtime_acc_dir = output_dir / "acc_result"
            runtime_acc_file = runtime_acc_dir / "acc.json"
            eval_query_dir = self._dql_eval_roots(spec.dataset, spec.query_type)[0] / eval_dir_name
            eval_acc_file = eval_query_dir / "acc.json"
            
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
            
            if spec.mode in {"eval", "run+eval"}:
                # Keep evaluation incremental unless rebuild_eval is explicitly requested.
                if not rebuild_eval and eval_acc_file.exists():
                    all_stdout.append(f"[INFO] skip eval query_{i+1}: existing acc.json found")
                    try:
                        with open(eval_acc_file, "r", encoding="utf-8") as f:
                            acc = json.load(f)
                            f1 = self._extract_macro_f1(acc)
                            if f1 is not None:
                                macro_f1s.append(f1)
                    except Exception:
                        pass
                    self._prune_runtime_acc_if_duplicated(runtime_acc_dir, eval_query_dir)
                    self._mirror_query_csv(
                        dataset=spec.dataset,
                        eval_dir_name=eval_dir_name,
                        result_csv=output_dir / "results.csv",
                    )
                    continue
                elif not rebuild_eval and runtime_acc_file.exists():
                    # Backward-compatibility path for older runs that stored acc_result under _runtime.
                    self._mirror_eval_artifacts(
                        dataset=spec.dataset,
                        query_type=spec.query_type,
                        eval_dir_name=eval_dir_name,
                        acc_result_dir=runtime_acc_dir,
                    )
                    if eval_acc_file.exists():
                        all_stdout.append(f"[INFO] skip eval query_{i+1}: reused runtime acc_result")
                        try:
                            with open(eval_acc_file, "r", encoding="utf-8") as f:
                                acc = json.load(f)
                                f1 = self._extract_macro_f1(acc)
                                if f1 is not None:
                                    macro_f1s.append(f1)
                        except Exception:
                            pass
                        self._prune_runtime_acc_if_duplicated(runtime_acc_dir, eval_query_dir)
                        self._mirror_query_csv(
                            dataset=spec.dataset,
                            eval_dir_name=eval_dir_name,
                            result_csv=output_dir / "results.csv",
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
                            "results.csv not generated (set DQL_ALLOW_TEMPLATE_CSV=1 to allow schema-only fallback)."
                        )

                if result_csv.exists():
                    eval_cmd = [
                        python_exe, "-m", "evaluation.run_eval",
                        "--dataset", spec.dataset,
                        "--task", item_category,
                        "--sql-file", str(sql_file),
                        "--result-csv", str(result_csv),
                        "--output-dir", str(eval_query_dir),
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

                if eval_acc_file.exists():
                    try:
                        with open(eval_acc_file, "r", encoding="utf-8") as f:
                            acc = json.load(f)
                            f1 = self._extract_macro_f1(acc)
                            if f1 is not None:
                                macro_f1s.append(f1)
                    except Exception:
                        pass
                self._prune_runtime_acc_if_duplicated(runtime_acc_dir, eval_query_dir)

                self._mirror_query_csv(
                    dataset=spec.dataset,
                    eval_dir_name=eval_dir_name,
                    result_csv=result_csv,
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

