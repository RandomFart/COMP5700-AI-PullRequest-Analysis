#!/usr/bin/env python3
"""
Task 1
Convert all_pull_request.parquet → output/task1_output.csv

INPUT (Parquet):
    title, id, agent, body, repo_id, repo_url

OUTPUT CSV (headers required by project):
    TITLE, ID, AGENTNAME, BODYSTRING, REPOID, REPOURL
"""

import sys
import pandas as pd
from pathlib import Path

# Repository-relative paths
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"

INPUT_FILE = DATA_DIR / "all_pull_request.parquet"
OUTPUT_FILE = OUTPUT_DIR / "task1_output.csv"

# Mapping Parquet column → CSV header
COLUMN_MAPPING = {
    "title": "TITLE",
    "id": "ID",
    "agent": "AGENTNAME",
    "body": "BODYSTRING",
    "repo_id": "REPOID",
    "repo_url": "REPOURL",
}


def generate_task1_csv(input_path: Path = INPUT_FILE,
                       output_path: Path = OUTPUT_FILE,
                       engine: str = "pyarrow") -> None:

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_parquet(input_path, engine=engine)

    # Ensure required columns exist
    missing = [col for col in COLUMN_MAPPING if col not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns in Parquet file: {', '.join(missing)}")

    df_out = df[list(COLUMN_MAPPING.keys())].rename(columns=COLUMN_MAPPING)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"Task 1 complete. CSV generated at: {output_path.resolve()}")


def main(argv: list[str]) -> None:
    """
    Optional CLI:
        python task1.py
        python task1.py <input.parquet> <output.csv>
    """
    if len(argv) == 1:
        generate_task1_csv()
    elif len(argv) == 3:
        generate_task1_csv(Path(argv[1]), Path(argv[2]))
    else:
        print("Usage:\n  python task1.py\n  python task1.py <input.parquet> <output.csv>")
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)
