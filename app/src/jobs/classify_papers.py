import pandas as pd
from cso_classifier import CSOClassifier
from pathlib import Path

def classify_file(csv_path: Path, output_path: Path, limit: int = None):
    print(f"Processing {csv_path}...")

    df = pd.read_csv(
        csv_path,
        header=None,
        names=["eid", "title", "abstract", "keywords"],
        on_bad_lines='warn'
    )

    if limit:
        df = df.head(limit)

    papers = {
        row["eid"]: {
            "title": str(row["title"]) if pd.notna(row["title"]) else "",
            "abstract": str(row["abstract"]) if pd.notna(row["abstract"]) else "",
            "keywords": str(row["keywords"]) if pd.notna(row["keywords"]) else ""
        }
        for _, row in df.iterrows()
    }

    print(f"→ {len(papers)} papers ready for classification.")

    cc = CSOClassifier(workers=8, modules="semantic", enhancement="first", explanation=False, silent=True)
    cc.setup()
    results = cc.batch_run(papers)

    output_rows = []
    for eid, res in results.items():
        if isinstance(res, dict):
            domains = res.get("enhanced", [])[:5]
        else:
            domains = []
        output_rows.append({
            "eid": eid,
            "result_cso": ", ".join(domains)
        })

    output_df = pd.DataFrame(output_rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False)

    print(f"Classification complete for {csv_path.name} → saved to {output_path.name}")

def classify_directory(input_root: Path, output_root: Path, limit_per_file: int = None):
    all_csv_files = list(input_root.glob("**/*.csv"))

    if not all_csv_files:
        print("No CSV files found.")
        return

    for csv_file in all_csv_files:
        relative_path = csv_file.relative_to(input_root)
        output_file = output_root / relative_path
        output_file = output_file.with_name(f"{output_file.stem}_classified.csv")
        classify_file(csv_file, output_file, limit=limit_per_file)

if __name__ == "__main__":
    input_dir = Path("../../data/silver_layer/scopus/preprocessed_papers")
    output_dir = Path("../../data/gold_layer/scopus/classified_papers")

    classify_directory(input_dir, output_dir, limit_per_file=20000)
