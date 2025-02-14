import pandas as pd

# Load your TSV file into a pandas DataFrame
file_path = "E:\\2015q4\\pre.txt"  # Example path:
df = pd.read_csv(file_path, sep='\t')

# 1. General Profiling: Show basic info and statistics
print("Basic Info:")
print(df.info())  # Show column types and non-null counts

print("\nSummary Statistics:")
print(df.describe())  # Get basic stats for numerical columns

# 2. Check for missing values
print("\nMissing Values:")
print(df.isnull().sum())  # Count missing values for each column

# 3. Check for duplicates
print("\nDuplicate Rows:")
print(df.duplicated().sum())  # Count duplicate rows

# 4. Data Types of Columns
print("\nData Types:")
print(df.dtypes)  # Show data types of each column

# 5. Sample of data to manually inspect
print("\nData Sample (first 5 rows):")
print(df.head())  # Preview first 5 rows

# Example of Data Cleaning:

# - Remove duplicates
df_cleaned = df.drop_duplicates()

# - Handle missing values (example: fill with mean for numerical columns)
df_cleaned = df_cleaned.fillna(df_cleaned.mean())  # Use column mean to fill missing numerical values

# - Convert data types (example: convert a column to string)
df_cleaned['your_column'] = df_cleaned['your_column'].astype(str)

# 6. Save cleaned dataset
df_cleaned.to_csv("cleaned_file.tsv", sep='\t', index=False)
