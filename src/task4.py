#!/usr/bin/env python3
"""
Task 4
Convert pr_commit_details.parquet → output/task4_output.csv

INPUT columns (from pr_commit_details.parquet):
    pr_id, sha, message, filename, status, additions, deletions, changes, patch

OUTPUT columns:
    PRID, PRSHA, PRCOMMITMESSAGE, PRFILE, PRSTATUS,
    PRADDS, PRDELSS, PRCHANGECOUNT, PRDIFF

Note: PRDELSS is spelled exactly like that per project spec.
"""

import sys
import pandas as pd
from pathlib import Path

# ---------- Paths ----------

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"

INPUT_FILE = DATA_DIR / "pr_commit_details.parquet"
OUTPUT_FILE = OUTPUT_DIR / "task4_output.csv"

# ---------- Helpers ----------

def clean_diff(value):
    """
    Clean the diff/patch text to avoid encoding issues:
    - Treat NaN as empty string
    - Force to string
    - Strip non-ASCII characters
    """
    if pd.isna(value):
        return ""
    text = str(value)
    return text.encode("ascii", "ignore").decode("ascii")


def generate_task4_csv(input_path: Path = INPUT_FILE,
                       output_path: Path = OUTPUT_FILE,
                       engine: str = "pyarrow") -> None:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    cols = [
        "pr_id",
        "sha",
        "message",
        "filename",
        "status",
        "additions",
        "deletions",
        "changes",
        "patch",
    ]

    df = pd.read_parquet(input_path, engine=engine)

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in pr_commit_details dataset: {', '.join(missing)}")

    df_out = df[cols].copy()

    # Clean patch/diff text
    df_out["patch"] = df_out["patch"].apply(clean_diff)

    # Rename columns
    df_out = df_out.rename(
        columns={
            "pr_id": "PRID",
            "sha": "PRSHA",
            "message": "PRCOMMITMESSAGE",
            "filename": "PRFILE",
            "status": "PRSTATUS",
            "additions": "PRADDS",
            "deletions": "PRDELSS",      # yes, double S
            "changes": "PRCHANGECOUNT",
            "patch": "PRDIFF",
        }
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"Task 4 complete. CSV generated at: {output_path.resolve()}")


def main(argv: list[str]) -> None:
    """
    Usage:
        python src/task4.py
        python src/task4.py <input.parquet> <output.csv>
    """
    if len(argv) == 1:
        generate_task4_csv()
    elif len(argv) == 3:
        generate_task4_csv(Path(argv[1]), Path(argv[2]))
    else:
        print("Usage:\n  python task4.py\n  python task4.py <input> <output>")
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)
