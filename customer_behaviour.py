import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Load dataset
df = pd.read_csv("customer_shopping_behavior.csv")

# Preview data
print("\n--- HEAD ---")
print(df.head())

print("\n--- INFO ---")
df.info()

# Summary statistics
print("\n--- DESCRIBE ---")
print(df.describe(include="all"))

# Check missing values
print("\n--- MISSING VALUES (BEFORE) ---")
print(df.isnull().sum())

# Impute missing Review Rating with median by Category
df['Review Rating'] = (
    df.groupby('Category')['Review Rating']
      .transform(lambda x: x.fillna(x.median()))
)

print("\n--- MISSING VALUES (AFTER) ---")
print(df.isnull().sum())

# Rename columns to snake_case
df.columns = df.columns.str.lower().str.replace(" ", "_")
df = df.rename(columns={'purchase_amount_(usd)': 'purchase_amount'})

print("\n--- COLUMNS AFTER RENAME ---")
print(df.columns)

# Create age_group column
labels = ['Young Adult', 'Adult', 'Middle-aged', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels=labels)

print("\n--- AGE GROUP SAMPLE ---")
print(df[['age', 'age_group']].head(10))

# Purchase frequency mapping
frequency_mapping = {
    'Fortnightly': 14,
    'Weekly': 7,
    'Monthly': 30,
    'Quarterly': 90,
    'Bi-Weekly': 14,
    'Annually': 365,
    'Every 3 Months': 90
}

df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)

print("\n--- PURCHASE FREQUENCY SAMPLE ---")
print(df[['purchase_frequency_days', 'frequency_of_purchases']].head(10))

# Check discount vs promo code
print("\n--- DISCOUNT vs PROMO ---")
print(df[['discount_applied', 'promo_code_used']].head(10))
print("Are both columns identical?",
      (df['discount_applied'] == df['promo_code_used']).all())

# Drop redundant column
df = df.drop('promo_code_used', axis=1)

print("\n--- FINAL COLUMNS ---")
print(df.columns)

# Save cleaned data
df.to_csv("customer_cleaned.csv", index=False)
print("\nCleaned dataset saved as customer_cleaned.csv")

#My SQL Connection
username = "root"
password = quote_plus("Root@123")
host = "localhost"
port = 3306
database = "customer_behavior"

engine = create_engine(
    f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}"
)

# Write DataFrame to MySQL
table_name = "customer"   # choose any table name
df.to_sql(table_name, engine, if_exists="replace", index=False)
print("Data written to MySQL successfully")

# Read back sample
df_check = pd.read_sql("SELECT * FROM customer LIMIT 5;", engine)
print(df_check)