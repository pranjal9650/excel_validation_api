import pandas as pd
import os
import joblib

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

# ==============================
# 1. LOAD FILES
# ==============================

valid_path = "outputs/eb_meter_valid.xlsx"
junk_path = "outputs/eb_meter_junk.xlsx"

valid_df = pd.read_excel(valid_path)
junk_df = pd.read_excel(junk_path)

print("Valid shape:", valid_df.shape)
print("Junk shape:", junk_df.shape)

# ==============================
# 2. ADD TARGET COLUMN
# ==============================

valid_df["target"] = 0
junk_df["target"] = 1

df = pd.concat([valid_df, junk_df], ignore_index=True)

print("\nTarget Distribution:")
print(df["target"].value_counts())

# ==============================
# 3. DROP NON ML COLUMNS
# ==============================

drop_cols = []

if "validation_errors" in df.columns:
    drop_cols.append("validation_errors")

# Remove target accidentally
drop_cols = [c for c in drop_cols if c != "target"]

# ==============================
# 4. SPLIT FEATURES + TARGET
# ==============================

X = df.drop(columns=drop_cols + ["target"])
y = df["target"]

# ==============================
# 5. DATA CLEANING
# ==============================

# Convert everything to string (helps with messy Excel data)
X = X.astype(str)

# One hot encoding (Better than label encoding)
X = pd.get_dummies(X)

# Fill missing values
X = X.fillna(0)

# ==============================
# 6. TRAIN TEST SPLIT
# ==============================

if len(y.unique()) < 2:
    print("❌ ERROR: Need both valid + junk samples for training")
    exit()

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

# ==============================
# 7. TRAIN MODEL
# ==============================

model = RandomForestClassifier(
    n_estimators=150,
    random_state=42,
    class_weight="balanced"
)

model.fit(X_train, y_train)

# ==============================
# 8. EVALUATE MODEL
# ==============================

y_pred = model.predict(X_test)

print("\nModel Performance:")
print(classification_report(y_test, y_pred))

# ==============================
# 9. SAVE MODEL + FEATURES ⭐ VERY IMPORTANT
# ==============================

os.makedirs("ml_model/saved_models", exist_ok=True)

joblib.dump(model, "ml_model/saved_models/eb_meter_model.pkl")

# Save feature columns for prediction alignment
joblib.dump(
    X.columns.tolist(),
    "ml_model/saved_models/eb_columns.pkl"
)

print("\n✅ EB Meter Model + Features Saved Successfully!")