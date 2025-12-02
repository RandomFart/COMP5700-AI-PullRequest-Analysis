#!/usr/bin/env python3
"""
Task 5

Creates a CSV file with:
    ID, AGENT, TYPE, CONFIDENCE, SECURITY

SECURITY = 1 if any security-related keywords appear in the
TITLE or BODYSTRING of the pull request, else 0.

Inputs:
    output/task1_output.csv  (from Task 1)
    output/task3_output.csv  (from Task 3)

Output:
    output/task5_output.csv
"""

import sys
import pandas as pd
from pathlib import Path

# ---------- Paths ----------

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"

TASK1_CSV = OUTPUT_DIR / "task1_output.csv"
TASK3_CSV = OUTPUT_DIR / "task3_output.csv"
TASK5_OUTPUT_CSV = OUTPUT_DIR / "task5_output.csv"

# ---------- Security keywords ----------

SECURITY_KEYWORDS = [
    "race",
    "racy",
    "buffer",
    "overflow",
    "stack",
    "integer",
    "signedness",
    "underflow",
    "improper",
    "unauthenticated",
    "gain access",
    "permission",
    "cross site",
    "css",
    "xss",
    "denial service",
    "dos",
    "crash",
    "deadlock",
    "injection",
    "request forgery",
    "csrf",
    "xsrf",
    "forged",
    "security",
    "vulnerability",
    "vulnerable",
    "exploit",
    "attack",
    "bypass",
    "backdoor",
    "threat",
    "expose",
    "breach",
    "violate",
    "fatal",
    "blacklist",
    "overrun",
    "insecure",
]


def compute_security_flag(title: str, body: str) -> int:
    """Return 1 if any security keyword is found in title or body."""
    text = f"{title or ''} {body or ''}".lower()
    for kw in SECURITY_KEYWORDS:
        if kw.lower() in text:
            return 1
    return 0


def create_task5_output(
    task1_path: Path = TASK1_CSV,
    task3_path: Path = TASK3_CSV,
    output_path: Path = TASK5_OUTPUT_CSV,
) -> None:
    # --- Load inputs ---
    if not task1_path.exists():
        raise FileNotFoundError(f"Task 1 output not found: {task1_path}")
    if not task3_path.exists():
        raise FileNotFoundError(f"Task 3 output not found: {task3_path}")

    t1 = pd.read_csv(task1_path)
    t3 = pd.read_csv(task3_path)

    # --- Merge on ID / PRID ---
    merged = t1.merge(t3, left_on="ID", right_on="PRID", how="inner")

    # --- Compute SECURITY column ---
    merged["SECURITY"] = merged.apply(
        lambda row: compute_security_flag(
            row.get("TITLE", ""), row.get("BODYSTRING", "")
        ),
        axis=1,
    )

    # --- Final output ---
    final_df = merged[["ID", "AGENTNAME", "PRTYPE", "CONFIDENCE", "SECURITY"]].rename(
        columns={
            "AGENTNAME": "AGENT",
            "PRTYPE": "TYPE",
        }
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Task 5 complete — CSV written to: {output_path.resolve()}")


def main(argv: list[str]) -> None:
    if len(argv) == 1:
        create_task5_output()
    elif len(argv) == 4:
        create_task5_output(Path(argv[1]), Path(argv[2]), Path(argv[3]))
    else:
        print(
            "Usage:\n"
            "  python src/task5.py\n"
            "  python src/task5.py <task1_output.csv> <task3_output.csv> <task5_output.csv>"
        )
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)
