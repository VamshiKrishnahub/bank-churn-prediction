import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import joblib
import os

# === 1️⃣ Load dataset ===
data_path = os.path.join(os.path.dirname(__file__), "../Data/raw/Churn_Modelling_Cleaned.csv")
df = pd.read_csv(data_path)

print(" Data loaded successfully. Shape:", df.shape)

# === 2️⃣ Encode categorical features ===
geo_enc = LabelEncoder()
gen_enc = LabelEncoder()

df["Geography"] = geo_enc.fit_transform(df["Geography"])
df["Gender"] = gen_enc.fit_transform(df["Gender"])

# === 3️⃣ Define features and target ===
features = [
    "CreditScore", "Geography", "Gender", "Age", "Tenure",
    "Balance", "NumOfProducts", "HasCrCard", "IsActiveMember", "EstimatedSalary"
]
target = "Exited"

X = df[features]
y = df[target]

# === 4️⃣ Scale numerical features ===
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# === 5️⃣ Split data ===
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

# === 6️⃣ Train model (RandomForest Classifier) ===
model = RandomForestClassifier(
    n_estimators=200,       # number of trees
    max_depth=None,         # let it grow fully
    random_state=42,
    n_jobs=-1
)
model.fit(X_train, y_train)

# === 7️⃣ Evaluate model ===
y_pred_train = model.predict(X_train)
y_pred_test = model.predict(X_test)

train_acc = accuracy_score(y_train, y_pred_train)
test_acc = accuracy_score(y_test, y_pred_test)

print(f"\n Model Training Complete!")
print(f"Training Accuracy: {train_acc * 100:.2f}%")
print(f"Testing Accuracy:  {test_acc * 100:.2f}%\n")

print("📊 Classification Report:\n", classification_report(y_test, y_pred_test))
print("📈 Confusion Matrix:\n", confusion_matrix(y_test, y_pred_test))

# === 8️⃣ Save model and encoders ===
save_dir = os.path.dirname(__file__)

joblib.dump(model, os.path.join(save_dir, "churn_model.pkl"))
joblib.dump(scaler, os.path.join(save_dir, "scaler.pkl"))
joblib.dump(geo_enc, os.path.join(save_dir, "Geography_encoder.pkl"))
joblib.dump(gen_enc, os.path.join(save_dir, "Gender_encoder.pkl"))

print("\n All .pkl files updated successfully in:", save_dir)
