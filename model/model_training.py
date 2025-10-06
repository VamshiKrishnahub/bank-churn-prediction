# model/model_training.py

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score, confusion_matrix
import joblib
import os

# Load dataset (already cleaned, no nulls)
data_path = "/Users/sujith/Desktop/Defence/Data/Churn_Modelling_Cleaned.csv"
data = pd.read_csv(data_path)
print("Dataset loaded successfully!")
print(data.head())

# Encode categorical variables
label_encoders = {}
for col in ['Geography', 'Gender']:
    le = LabelEncoder()
    data[col] = le.fit_transform(data[col])
    label_encoders[col] = le

# Features and target
X = data.drop(['RowNumber', 'CustomerId', 'Surname', 'Exited'], axis=1)
y = data['Exited']

# Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Scale features
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Define models
models = {
    "LogisticRegression": LogisticRegression(),
    "DecisionTree": DecisionTreeClassifier(),
    "RandomForest": RandomForestClassifier(n_estimators=100),
    "KNN": KNeighborsClassifier(),
    "SVM": SVC()
}

# Train and evaluate
best_model_name = None
best_accuracy = 0
best_model = None

for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    print(f"{name} Accuracy: {acc * 100:.2f}%")
    print(f"{name} Confusion Matrix:\n{confusion_matrix(y_test, y_pred)}\n")

    if acc > best_accuracy:
        best_accuracy = acc
        best_model_name = name
        best_model = model

print(f"Best Model: {best_model_name} with accuracy {best_accuracy * 100:.2f}%")

# Save best model, scaler, and encoders
model_dir = os.path.dirname(__file__)
joblib.dump(best_model, os.path.join(model_dir, 'churn_model.pkl'))
joblib.dump(scaler, os.path.join(model_dir, 'scaler.pkl'))
for col, le in label_encoders.items():
    joblib.dump(le, os.path.join(model_dir, f"{col}_encoder.pkl"))

print("Best model, scaler, and encoders saved successfully!")
