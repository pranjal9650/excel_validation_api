import pandas as pd
import numpy as np
import joblib
import os

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

# =====================================================
# LOAD DATA
# =====================================================

valid_path = "outputs/ftth_acquisition_valid.xlsx"
junk_path = "outputs/ftth_acquisition_junk.xlsx"

valid_df = pd.read_excel(valid_path)
junk_df = pd.read_excel(junk_path)

print("Valid shape:", valid_df.shape)
print("Junk shape:", junk_df.shape)

# =====================================================
# ADD TARGET COLUMN
# =====================================================

valid_df["target"] = 0   # Valid
junk_df["target"] = 1    # Junk

# Combine datasets
df = pd.concat([valid_df, junk_df], ignore_index=True)

print("\nTarget Distribution:")
print(df["target"].value_counts())

# =====================================================
# CLEAN DATA
# =====================================================

drop_columns = [
    "validation_errors",
    "createduser",
    "modifieduser"
]

df = df.drop(columns=[col for col in drop_columns if col in df.columns])

# Convert numeric-like columns
for col in df.columns:
    if col != "target":
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Encode categorical columns safely
categorical_cols = df.select_dtypes(include=["object"]).columns.tolist()

for col in categorical_cols:
    if col != "target":
        df[col] = df[col].astype("category").cat.codes

# Fill missing values
df = df.fillna(0)

# =====================================================
# SPLIT FEATURES + TARGET
# =====================================================

X = df.drop("target", axis=1)
y = df["target"]

print("\nTraining Features:")
print(X.columns.tolist())

# Safety check
if len(y.unique()) < 2:
    print("❌ ERROR: Need both valid and junk samples")
    exit()

# =====================================================
# TRAIN TEST SPLIT
# =====================================================

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

# =====================================================
# TRAIN MODEL
# =====================================================

model = RandomForestClassifier(
    n_estimators=200,
    random_state=42,
    class_weight="balanced"
)

model.fit(X_train, y_train)

# =====================================================
# EVALUATE MODEL
# =====================================================

y_pred = model.predict(X_test)

print("\nModel Performance:")
print(classification_report(y_test, y_pred))

# =====================================================
# SAVE MODEL + FEATURES ⭐ VERY IMPORTANT
# =====================================================

os.makedirs("ml_model/saved_models", exist_ok=True)

joblib.dump(
    model,
    "ml_model/saved_models/ftth_random_forest.pkl"
)

# Save feature columns for prediction alignment
joblib.dump(
    X.columns.tolist(),
    "ml_model/saved_models/ftth_columns.pkl"
)

print("\n✅ FTTH Model + Features Saved Successfully!")