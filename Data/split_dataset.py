import pandas as pd
import os

def split_dataset(input_file, output_folder, rows_per_file):

    # Load dataset
    df = pd.read_csv(input_file)
    total_rows = len(df)

    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Calculate how many output files we need
    num_files = (total_rows // rows_per_file) + (1 if total_rows % rows_per_file else 0)

    # Split and save
    for i in range(num_files):
        start = i * rows_per_file
        end = min(start + rows_per_file, total_rows)

        subset = df.iloc[start:end]

        out_path = os.path.join(output_folder, f"raw_split_{i+1}.csv")
        subset.to_csv(out_path, index=False)

        print(f"[OK] Saved {out_path} ({len(subset)} rows)")

    print("\n🎉 Done! All split files generated.")


# ---------------------------------------------------------
# Git-SAFE relative paths (NO absolute paths)
# ---------------------------------------------------------
if __name__ == "__main__":
    split_dataset(
        input_file="../Errors/dataset_with_errors.csv",
        output_folder="./raw-data",
        rows_per_file=10
    )
