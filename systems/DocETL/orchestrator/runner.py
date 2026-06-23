from dataclasses import dataclass
import re
import subprocess
import sys
from pathlib import Path
import os


@dataclass
class DocETLRunResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    docetl_reported_cost_usd: float | None
    token_usage: dict[str, dict[str, int]]


def _safe_decode(data: bytes | None) -> str:
    if not data:
        return ""
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("utf-8", errors="replace")


def _clean_console_text(text: str) -> str:
    # Remove ANSI escape sequences and common Rich markup left in captured logs.
    text = re.sub(r"\x1b\[[0-9;?]*[ -/]*[@-~]", "", text)
    text = re.sub(r"\[/?[a-zA-Z][^\]]*\]", "", text)
    return text


def _parse_int_token(value: str | None) -> int:
    if not value:
        return 0
    return int(value.replace(",", ""))


def _parse_docetl_metrics(stdout: str, stderr: str) -> tuple[float | None, dict[str, dict[str, int]]]:
    text = _clean_console_text(stdout + "\n" + stderr)

    costs = re.findall(r"Cost:\s*\$([0-9][0-9,]*(?:\.[0-9]+)?)", text)
    reported_cost = float(costs[-1].replace(",", "")) if costs else None

    usage: dict[str, dict[str, int]] = {}
    token_re = re.compile(
        r"(?P<model>[A-Za-z0-9_./:-]+):\s*"
        r"(?P<prompt>[\d,]+)\s+input"
        r"(?:\s*\((?P<cached>[\d,]+)\s+cached\))?,\s*"
        r"(?P<completion>[\d,]+)\s+output",
        re.IGNORECASE,
    )
    for match in token_re.finditer(text):
        model = match.group("model").strip()
        if model.lower() == "total":
            continue
        usage[model] = {
            "prompt_tokens": _parse_int_token(match.group("prompt")),
            "completion_tokens": _parse_int_token(match.group("completion")),
        }
        cached = _parse_int_token(match.group("cached"))
        if cached:
            usage[model]["cached_tokens"] = cached

    return reported_cost, usage


def _build_run_result(cmd: list[str], returncode: int, stdout: str, stderr: str) -> DocETLRunResult:
    reported_cost, token_usage = _parse_docetl_metrics(stdout, stderr)
    return DocETLRunResult(
        command=cmd,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        docetl_reported_cost_usd=reported_cost,
        token_usage=token_usage,
    )


def run_docetl(yaml_path: str) -> DocETLRunResult:
    commands = []
    docetl_exe = Path(sys.executable).with_name("docetl.exe")
    if docetl_exe.exists():
        commands.append([str(docetl_exe), "run", yaml_path])

    commands.append([sys.executable, "-X", "utf8", "-m", "docetl.cli", "run", yaml_path])

    attempts = []
    for cmd in commands:
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        env["RICH_FORCE_TERMINAL"] = "0"
        # Ensure DocETL loads .env from project root.
        cwd = str(Path(__file__).resolve().parents[3])
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=False,
            env=env,
            cwd=cwd,
        )
        stdout = _safe_decode(result.stdout)
        stderr = _safe_decode(result.stderr)
        attempts.append((cmd, result.returncode, stdout, stderr))
        if result.returncode == 0:
            return _build_run_result(cmd, result.returncode, stdout, stderr)

    details = []
    for cmd, returncode, stdout, stderr in attempts:
        details.append(
            f"CMD: {' '.join(cmd)}\n"
            f"STDOUT:\n{stdout}\n"
            f"STDERR:\n{stderr}\n"
            f"EXIT: {returncode}"
        )

    full_errors = "\n\n".join(details)
    if (
        "OPENAI_API_KEY" in full_errors
        or "GEMINI_API_KEY" in full_errors
        or "GOOGLE_API_KEY" in full_errors
        or "AZURE_API_KEY" in full_errors
        or "AZURE_API_BASE" in full_errors
        or "AuthenticationError" in full_errors
    ):
        raise RuntimeError(
            f"DocETL failed for {yaml_path}\n"
            "Autenticazione LLM fallita: imposta una chiave valida nel file .env "
            "(AZURE_API_KEY/AZURE_API_BASE, OPENAI_API_KEY oppure GEMINI_API_KEY/GOOGLE_API_KEY)."
        )

    raise RuntimeError(
        f"DocETL failed for {yaml_path}\n"
        + full_errors
    )
