# Data/data_gen_split.py
import pandas as pd
import os
import argparse
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('--dataset_path', type=str, required=True, help='Path to full CSV dataset')
parser.add_argument('--raw_data_dir', type=str, required=True, help='Folder to save split CSV files')
parser.add_argument('--num_files', type=int, default=5, help='Number of files to split into')
args = parser.parse_args()

dataset_path = args.dataset_path
raw_data_dir = args.raw_data_dir
num_files = args.num_files

# Make sure dataset exists
if not os.path.exists(dataset_path):
    raise FileNotFoundError(f"Dataset not found: {dataset_path}")

# Read dataset
df = pd.read_csv(dataset_path)
if df.empty:
    raise ValueError(f"Dataset is empty: {dataset_path}")


os.makedirs(raw_data_dir, exist_ok=True)

# Split dataset
rows_per_file = int(np.ceil(len(df) / num_files))

for i in range(num_files):
    start = i * rows_per_file
    end = start + rows_per_file
    split_df = df.iloc[start:end]
    split_file = os.path.join(raw_data_dir, f'split_{i+1}.csv')
    split_df.to_csv(split_file, index=False)
    print(f"Created {split_file} with {len(split_df)} rows")

print("All files created successfully!")
