import pandas as pd
import os
from sklearn.ensemble import RandomForestClassifier
import joblib

# ==============================
# 1. Load Junk File Only
# ==============================

junk_path = "outputs/site_survey_checklist_junk.xlsx"

df = pd.read_excel(junk_path)

print("Loaded shape:", df.shape)

# ==============================
# 2. Add Target = 1 (All Junk)
# ==============================

df["target"] = 1

print("\nTarget Distribution:")
print(df["target"].value_counts())

# ==============================
# 3. Drop validation column
# ==============================

drop_cols = []

if "validation_errors" in df.columns:
    drop_cols.append("validation_errors")

X = df.drop(columns=drop_cols + ["target"])
y = df["target"]

# Convert all to string
X = X.astype(str)

# One-hot encode
X = pd.get_dummies(X)

# ==============================
# 4. Train Model (Single Class)
# ==============================

model = RandomForestClassifier(
    n_estimators=100,
    random_state=42
)

model.fit(X, y)

# ==============================
# 5. Save Model
# ==============================

os.makedirs("ml_model/saved_models", exist_ok=True)

joblib.dump(model, "ml_model/saved_models/site_survey_checklist_model.pkl")

print("\n⚠ Model trained with ONLY class 1 (junk).")
print("⚠ This model will predict EVERYTHING as junk.")
print("✅ Model saved successfully.")