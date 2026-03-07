import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib

# ==============================
# 1. Load Files
# ==============================

valid_path = "outputs/od_survey_valid.xlsx"
junk_path = "outputs/od_survey_junk.xlsx"

valid_df = pd.read_excel(valid_path)
junk_df = pd.read_excel(junk_path)

print("Valid shape:", valid_df.shape)
print("Junk shape:", junk_df.shape)

# ==============================
# 2. Add Target Column
# ==============================

valid_df["target"] = 0   # valid
junk_df["target"] = 1    # invalid

# Combine
df = pd.concat([valid_df, junk_df], ignore_index=True)

print("\nTarget Distribution:")
print(df["target"].value_counts())

# ==============================
# 3. Remove validation column
# ==============================

drop_cols = []

if "validation_errors" in df.columns:
    drop_cols.append("validation_errors")

X = df.drop(columns=drop_cols + ["target"])
y = df["target"]

# ==============================
# 4. Convert all to string
# ==============================

X = X.astype(str)

# One-hot encode
X = pd.get_dummies(X)

# ==============================
# 5. Train Test Split
# ==============================

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y   # IMPORTANT for imbalanced data
)

# ==============================
# 6. Train Model
# ==============================

model = RandomForestClassifier(
    n_estimators=150,
    random_state=42,
    class_weight="balanced"  # IMPORTANT for imbalance
)

model.fit(X_train, y_train)

# ==============================
# 7. Evaluate
# ==============================

y_pred = model.predict(X_test)

print("\nModel Performance:\n")
print(classification_report(y_test, y_pred))

# ==============================
# 8. Save Model
# ==============================

os.makedirs("ml_model/saved_models", exist_ok=True)

joblib.dump(model, "ml_model/saved_models/od_survey_model.pkl")

print("\n✅ OD Survey Model Trained Successfully!")