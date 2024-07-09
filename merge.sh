#!/bin/bash
 
# Define the input and output file paths
input_file1="1.csv"
input_file2="2.csv"
output_file="merged_file.csv"
 
# Python script to merge the CSV files
python3 - <<EOF
import pandas as pd
 
# Define the input and output file paths
input_file1 = "$input_file1"
input_file2 = "$input_file2"
output_file = "$output_file"
 
# Read the CSV files into DataFrames
df1 = pd.read_csv(input_file1)
df2 = pd.read_csv(input_file2)
 
# Merge the DataFrames on the common column 'rr_id'
merged_df = pd.merge(df1, df2, on='rr_id')
 
# Write the merged DataFrame to a CSV file
merged_df.to_csv(output_file, index=False)
 
print(f"Merged file saved as '{output_file}'.")
EOF
 
echo "Merge completed. The merged file is saved as '${output_file}'."
