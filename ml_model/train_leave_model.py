import pandas as pd
import joblib
import os

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

# =====================================================
# LOAD DATA
# =====================================================

valid_path = "outputs/leave_form_valid.xlsx"
junk_path = "outputs/leave_form_junk.xlsx"

valid_df = pd.read_excel(valid_path)
junk_df = pd.read_excel(junk_path)

print("Valid rows:", len(valid_df))
print("Junk rows:", len(junk_df))

# =====================================================
# SAFETY CHECK (DO NOT REMOVE)
# =====================================================

if valid_df.empty or junk_df.empty:
    print("❌ Cannot train ML model. Need both valid and junk data.")
    exit()

# =====================================================
# ADD TARGET LABELS
# =====================================================

valid_df["target"] = 0
junk_df["target"] = 1

df = pd.concat([valid_df, junk_df], ignore_index=True)

print("\nTarget Distribution:")
print(df["target"].value_counts())

# =====================================================
# DROP NON ML COLUMNS
# =====================================================

drop_columns = [
    "validation_errors"
]

df = df.drop(columns=[c for c in drop_columns if c in df.columns])

# =====================================================
# DATA CLEANING
# =====================================================

for col in df.columns:
    if col != "target":
        df[col] = df[col].astype(str)

# One hot encoding (VERY IMPORTANT FOR STABILITY)
df = pd.get_dummies(df)

df = df.fillna(0)

# =====================================================
# SPLIT FEATURES & TARGET
# =====================================================

X = df.drop("target", axis=1)
y = df["target"]

# Extra safety
if len(y.unique()) < 2:
    print("❌ Need both classes for ML training")
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
# EVALUATE
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
    "ml_model/saved_models/leave_random_forest.pkl"
)

joblib.dump(
    X.columns.tolist(),
    "ml_model/saved_models/leave_columns.pkl"
)

print("\n✅ Leave Model Training Completed")