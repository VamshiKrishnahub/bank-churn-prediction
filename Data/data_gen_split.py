import pandas as pd
import os
import argparse
import numpy as np
import random

# ---------------------------------------------------------
# Inject 1–3 RANDOM small errors inside existing rows
# ---------------------------------------------------------
def inject_errors(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    if df.empty:
        return df

    # Columns by type
    numeric_cols = ["CreditScore", "Age", "Tenure", "Balance",
                    "NumOfProducts", "HasCrCard", "IsActiveMember",
                    "EstimatedSalary"]

    cat_cols = ["Geography", "Gender", "Surname"]

    safe_index = lambda: random.randint(0, len(df) - 1)

    # Number of random errors this split will get
    n_errors = random.randint(1, 3)

    for _ in range(n_errors):

        error_type = random.choice([
            "missing_value",
            "invalid_country",
            "invalid_gender",
            "negative_age",
            "string_in_numeric",
            "numeric_in_categorical",
            "corrupted_text"
        ])

        i = safe_index()

        # 1) Missing cell value
        if error_type == "missing_value":
            col = random.choice(df.columns)
            df.loc[i, col] = None

        # 2) Invalid Geography
        elif error_type == "invalid_country" and "Geography" in df.columns:
            df.loc[i, "Geography"] = random.choice(["UnknownLand", "Mars", "Atlantis"])

        # 3) Invalid Gender
        elif error_type == "invalid_gender" and "Gender" in df.columns:
            df.loc[i, "Gender"] = random.choice(["child", "none", "X"])

        # 4) Negative Age
        elif error_type == "negative_age" and "Age" in df.columns:
            df.loc[i, "Age"] = -random.randint(1, 30)

        # 5) Put string in numeric field
        elif error_type == "string_in_numeric":
            col = random.choice(numeric_cols)
            if col in df.columns:
                df.loc[i, col] = "not_a_number"

        # 6) Put numeric inside categorical field
        elif error_type == "numeric_in_categorical":
            col = random.choice(cat_cols)
            if col in df.columns:
                df.loc[i, col] = random.randint(1000, 9999)

        # 7) Corrupted strange text inside a string column
        elif error_type == "corrupted_text":
            col = random.choice(cat_cols)
            if col in df.columns:
                df.loc[i, col] = "###ERROR###"

    return df


# ---------------------------------------------------------
# Main Splitter (NO empty rows, NO added rows)
# ---------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset_path", type=str, required=True)
    parser.add_argument("--raw_data_dir", type=str, required=True)
    parser.add_argument("--num_files", type=int, default=1000)
    args = parser.parse_args()

    df = pd.read_csv(args.dataset_path)
    if df.empty:
        raise ValueError("Dataset empty!")

    os.makedirs(args.raw_data_dir, exist_ok=True)

    rows = len(df)
    rows_per_file = max(1, rows // args.num_files)

    for i in range(args.num_files):
        start = i * rows_per_file
        end = min(start + rows_per_file, rows)

        split_df = df.iloc[start:end].copy()
        if split_df.empty:
            continue

        # Add random 1–3 safe errors
        split_df = inject_errors(split_df)

        out_path = os.path.join(args.raw_data_dir, f"raw_split_{i+1}.csv")
        split_df.to_csv(out_path, index=False)

        print(f"[OK] Created {out_path} ({len(split_df)} rows)")

    print("\n🎉 All files generated safely!")


if __name__ == "__main__":
    main()
