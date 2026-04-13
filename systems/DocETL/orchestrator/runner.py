import subprocess
import sys
from pathlib import Path
import os


def _safe_decode(data: bytes | None) -> str:
    if not data:
        return ""
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("utf-8", errors="replace")


def run_docetl(yaml_path: str):
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
            return stdout

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
        or "AuthenticationError" in full_errors
    ):
        raise RuntimeError(
            f"DocETL failed for {yaml_path}\n"
            "Autenticazione LLM fallita: imposta una chiave valida nel file .env "
            "(OPENAI_API_KEY oppure GEMINI_API_KEY/GOOGLE_API_KEY)."
        )

    raise RuntimeError(
        f"DocETL failed for {yaml_path}\n"
        + full_errors
    )
