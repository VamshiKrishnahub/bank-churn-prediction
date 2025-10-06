# model_training.py
import os
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

# ------------------------------
# Load dataset
# ------------------------------
data_path = "/Users/sujith/Desktop/Defence/Data/Churn_Modelling_Cleaned.csv"
data = pd.read_csv(data_path)
print("Dataset loaded successfully!")
print(data.head())

# ------------------------------
# Preprocessing
# ------------------------------
# Encode categorical columns
geo_enc = LabelEncoder()
gender_enc = LabelEncoder()
data['Geography'] = geo_enc.fit_transform(data['Geography'])
data['Gender'] = gender_enc.fit_transform(data['Gender'])

# Features and target
X = data[
    ['CreditScore', 'Geography', 'Gender', 'Age', 'Tenure', 'Balance', 'NumOfProducts', 'HasCrCard', 'IsActiveMember',
     'EstimatedSalary']]
y = data['Exited']

# Split dataset
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Scale numeric features
numeric_cols = ['CreditScore', 'Age', 'Tenure', 'Balance', 'NumOfProducts', 'HasCrCard', 'IsActiveMember',
                'EstimatedSalary']
scaler = StandardScaler()
X_train[numeric_cols] = scaler.fit_transform(X_train[numeric_cols])
X_test[numeric_cols] = scaler.transform(X_test[numeric_cols])

# ------------------------------
# Train multiple models
# ------------------------------
models = {
    "LogisticRegression": LogisticRegression(max_iter=1000),
    "DecisionTree": DecisionTreeClassifier(random_state=42),
    "RandomForest": RandomForestClassifier(random_state=42),
    "KNN": KNeighborsClassifier(),
    "SVM": SVC(probability=True)
}

best_model = None
best_accuracy = 0

for name, model in models.items():
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds) * 100  # accuracy in %
    print(f"{name} Accuracy: {acc:.2f}%")
    print(f"{name} Confusion Matrix:\n{confusion_matrix(y_test, preds)}\n")

    if acc > best_accuracy:
        best_accuracy = acc
        best_model = model

print(f"Best Model: {best_model.__class__.__name__} with accuracy {best_accuracy:.2f}%")

# ------------------------------
# Save model, scaler, encoders
# ------------------------------
# Ensure model directory exists (relative to this script)
base_dir = os.path.dirname(os.path.abspath(__file__))
model_dir = os.path.join(base_dir, "model")
os.makedirs(model_dir, exist_ok=True)

joblib.dump(best_model, os.path.join(model_dir, "churn_model.pkl"))
joblib.dump(scaler, os.path.join(model_dir, "scaler.pkl"))
joblib.dump(geo_enc, os.path.join(model_dir, "geo_enc.pkl"))
joblib.dump(gender_enc, os.path.join(model_dir, "gender_enc.pkl"))

print("Best model, scaler, and encoders saved successfully!")
