#!/usr/bin/env python3
"""
Task 2
Convert all_repository.parquet → output/task2_output.csv

INPUT columns:
    id, language, stars, url

OUTPUT columns:
    REPOID, LANG, STARS, REPOURL
"""

import sys
import pandas as pd
from pathlib import Path

# Repository-relative paths
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"

INPUT_FILE = DATA_DIR / "all_repository.parquet"
OUTPUT_FILE = OUTPUT_DIR / "task2_output.csv"

COLUMN_MAPPING = {
    "id": "REPOID",
    "language": "LANG",
    "stars": "STARS",
    "url": "REPOURL",
}

def generate_task2_csv(input_path: Path = INPUT_FILE,
                       output_path: Path = OUTPUT_FILE,
                       engine: str = "pyarrow") -> None:

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_parquet(input_path, engine=engine)

    missing = [col for col in COLUMN_MAPPING if col not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in repository dataset: {', '.join(missing)}")

    df_out = df[list(COLUMN_MAPPING.keys())].rename(columns=COLUMN_MAPPING)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"Task 2 complete. CSV generated at: {output_path.resolve()}")


def main(argv: list[str]) -> None:
    if len(argv) == 1:
        generate_task2_csv()
    elif len(argv) == 3:
        generate_task2_csv(Path(argv[1]), Path(argv[2]))
    else:
        print("Usage:\n  python task2.py\n  python task2.py <input> <output>")
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)

