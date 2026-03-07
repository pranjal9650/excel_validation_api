# train_od_model.py

import pandas as pd
import joblib
import os

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

# ======================================
# 1️⃣ Load Valid & Junk Files
# ======================================

valid_path = "outputs/od_operation_valid.xlsx"
junk_path = "outputs/od_operation_junk.xlsx"

valid_df = pd.read_excel(valid_path)
junk_df = pd.read_excel(junk_path)

print("Valid shape:", valid_df.shape)
print("Junk shape:", junk_df.shape)

# ======================================
# 2️⃣ Assign Target
# ======================================
# As per YOUR definition:
# 0 = VALID
# 1 = INVALID (JUNK)

valid_df["target"] = 0
junk_df["target"] = 1

# ======================================
# 3️⃣ Combine Both
# ======================================

df = pd.concat([valid_df, junk_df], ignore_index=True)

print("\nCombined Target Distribution:")
print(df["target"].value_counts())

# ======================================
# 4️⃣ Drop Leakage Columns
# ======================================

columns_to_remove = [
    "Name",
    "Id",
    "Owner name",
    "Owner Number ",
    "User Name",
    "CreatedDate",
    "ModifiedDate",
    "CreatedUser",
    "ModifiedUser"
]

df.drop(columns=[col for col in columns_to_remove if col in df.columns], inplace=True)

# ======================================
# 5️⃣ Convert Numeric Where Possible
# ======================================

for col in df.columns:
    if col != "target":
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ======================================
# 6️⃣ One-Hot Encode Categoricals
# ======================================

df = pd.get_dummies(df, drop_first=True)

# ======================================
# 7️⃣ Split Features & Target
# ======================================

X = df.drop("target", axis=1)
y = df["target"]

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

# ======================================
# 8️⃣ Train Model
# ======================================

model = RandomForestClassifier(
    n_estimators=200,
    random_state=42,
    class_weight="balanced"  # important for imbalance
)

model.fit(X_train, y_train)

# ======================================
# 9️⃣ Evaluate
# ======================================

y_pred = model.predict(X_test)

print("\nModel Performance:\n")
print(classification_report(y_test, y_pred))

# ======================================
# 🔟 Save Model & Columns
# ======================================

os.makedirs("ml_model/saved_models", exist_ok=True)

joblib.dump(model, "ml_model/saved_models/od_model.pkl")
joblib.dump(X.columns.tolist(), "ml_model/saved_models/od_columns.pkl")

print("\n✅ OD Operation Model Trained Successfully!")