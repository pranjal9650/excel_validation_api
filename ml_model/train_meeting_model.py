import pandas as pd
import os
import joblib

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

# ==============================
# LOAD DATA
# ==============================

valid_path = "outputs/meeting_valid.xlsx"
junk_path = "outputs/meeting_junk.xlsx"

valid_df = pd.read_excel(valid_path)
junk_df = pd.read_excel(junk_path)

# ==============================
# ADD TARGET
# ==============================

valid_df["target"] = 0
junk_df["target"] = 1

df = pd.concat([valid_df, junk_df], ignore_index=True)

print("Target distribution:")
print(df["target"].value_counts())

# ==============================
# DROP NON ML COLUMNS (Same as before)
# ==============================

drop_columns = []

if "validation_errors" in df.columns:
    drop_columns.append("validation_errors")

df = df.drop(columns=[c for c in drop_columns if c in df.columns])

# ==============================
# CLEAN + ENCODE (Same Simple Style as Your Other Models)
# ==============================

# Convert everything except target to string
for col in df.columns:
    if col != "target":
        df[col] = df[col].astype(str)

# One hot encoding
df = pd.get_dummies(df)

df = df.fillna(0)

# ==============================
# SPLIT FEATURES & TARGET
# ==============================

X = df.drop("target", axis=1)
y = df["target"]

# Safety check
if len(y.unique()) < 2:
    print("❌ Need both valid and junk data")
    exit()

# ==============================
# TRAIN TEST SPLIT
# ==============================

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

# ==============================
# TRAIN MODEL
# ==============================

model = RandomForestClassifier(
    n_estimators=100,
    random_state=42
)

model.fit(X_train, y_train)

# ==============================
# EVALUATE
# ==============================

y_pred = model.predict(X_test)

print(classification_report(y_test, y_pred))

# ==============================
# SAVE MODEL + COLUMNS ⭐ VERY IMPORTANT
# ==============================

os.makedirs("ml_model/saved_models", exist_ok=True)

joblib.dump(
    model,
    "ml_model/saved_models/meeting_model.pkl"
)

joblib.dump(
    X.columns.tolist(),
    "ml_model/saved_models/meeting_columns.pkl"
)

print("✅ Meeting model trained + columns saved")