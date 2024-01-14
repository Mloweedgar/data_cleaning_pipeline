import pandas as pd
from db import DataCleaningPipeline

# Initialize DataCleaningPipeline for handling data export/import
pipeline = DataCleaningPipeline()

# Configure tables and file paths for data cleaning process
source_table = 'distribution_points'  # Source table with original data
new_clean_table = 'clean_distribution_points'  # Destination table for cleaned data
dirty_csv_file = pipeline.export_dirty_data_to_csv(source_table, 'csv/dirty_distribution_points.csv')  # Export data to CSV
clean_csv_file = 'csv/clean_distribution_points.csv'  # CSV file path for cleaned data
df = pd.read_csv(dirty_csv_file)


################### Start Data Cleaning Process #####################
# Note: Modify below code per your specific data cleaning requirements

rows_before = len(df)
print(rows_before)

# General Issues
df = df.drop_duplicates()
print(df.dtypes)

df = df.drop(columns = ["Inst Area","Inst Category"])

# Clean string variables 
df[['Region', 'District', 'LGA', 'Ward', 'Village', 'DP']] = df[['Region', 'District', 'LGA', 'Ward', 'Village', 'DP']].astype(str)
df["Region"] = df["Region"].str.capitalize()

# Clean Latitude and Longitude variable
df = df.dropna(subset=['Latitude','Longitude'])
df = df[df['Longitude'].astype(str).str[0] == "3"]
df = df[df['Longitude'].astype(str).str[2] == "."]
df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
df = df[df['Latitude'].astype(str).str[0] == "-"]
df = df[df['Latitude'].astype(str).str[2] == "."]
df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
df = df.dropna(subset=['Latitude','Longitude'])

# Clean Served Population
df['Served Pop'] = pd.to_numeric(df['Served Pop'], errors='coerce')
df = df.dropna(subset=['Served Pop'])

# Clean Construction Year
print(df['Construction Year'].dtype)
df['Construction Year'] = df['Construction Year'].astype(str).str[:4]
df['Construction Year'] = pd.to_numeric(df['Construction Year'], errors='coerce')
df['Age'] = 2024 - df['Construction Year'] 

# Clean Scheme Type
df = df[df['Scheme Type'].isin(['piped', 'unpiped'])]
rows_after = len(df)
dropped_n = rows_before - rows_after
print(dropped_n)
print(f"\n{dropped_n} out of {rows_before} observations were deleted due to not accurate data.")
df.to_csv(clean_csv_file, index=False)

################### Finish Data Cleaning Process #####################

# Post-Cleaning: Importing cleaned data back to database
#create new table 
pipeline.create_table_with_columns(new_clean_table, df.dtypes)
# import new clean data to the new table
pipeline.import_clean_data_to_table(new_clean_table,clean_csv_file)
