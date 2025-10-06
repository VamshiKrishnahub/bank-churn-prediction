import pandas as pd

# give full path
df = pd.read_csv("/Users/sujith/Desktop/Defence/Data/Churn_Modelling_Cleaned.csv")

# print first 5 rows
print(df.head())

print(df.info())

print(df.isnull().sum())