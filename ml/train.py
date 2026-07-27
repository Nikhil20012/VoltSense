"""
Reads the training dataset from Snowflake, trains a LightGBM model,
evaluates it, generates a SHAP plot, and saves the model artifact.

Usage:
    export SNOWFLAKE_PASSWORD='...'
    python ml/train.py
"""

import os
import sys

import lightgbm as lgb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
import snowflake.connector
from sklearn.metrics import mean_absolute_error, mean_squared_error
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    SNOWFLAKE_CONFIG, TARGET, FEATURES,
    CATEGORICAL_FEATURES, LGBM_PARAMS,
)

MODEL_DIR = "ml/model"
MODEL_PATH = os.path.join(MODEL_DIR, "voltsense_lgbm.txt")
SHAP_PATH = os.path.join(MODEL_DIR, "shap_summary.png")


def load_data() -> pd.DataFrame:
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    query = "SELECT * FROM VOLTSENSE.DEV_ML_FEATURES.ML_TRAINING_DATASET"
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def prepare_features(df: pd.DataFrame):
    """Handle categoricals and split train/test by time."""
    df = df.sort_values("TIMESTAMP_15MIN").reset_index(drop=True)

    # Drop rows only where the target is null
    df = df.dropna(subset=[TARGET])

    # Fill null features with 0 (stations with limited history)
    # Force numeric types on all non-categorical features
    for col in FEATURES:
        if col not in CATEGORICAL_FEATURES:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Encode categoricals
    for col in CATEGORICAL_FEATURES:
        df[col] = df[col].astype("category")

    # Time-based split: first 80% train, last 20% test
    split_idx = int(len(df) * 0.8)
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]

    print(f"Train: {len(train)} rows, Test: {len(test)} rows")
    return train, test


def train_model(train: pd.DataFrame, test: pd.DataFrame):
    X_train = train[FEATURES]
    y_train = train[TARGET]
    X_test = test[FEATURES]
    y_test = test[TARGET]

    train_data = lgb.Dataset(
        X_train, label=y_train,
        categorical_feature=CATEGORICAL_FEATURES,
    )
    valid_data = lgb.Dataset(
        X_test, label=y_test,
        categorical_feature=CATEGORICAL_FEATURES,
        reference=train_data,
    )

    print("\nTraining LightGBM...")
    callbacks = [lgb.log_evaluation(50), lgb.early_stopping(30)]
    model = lgb.train(
        LGBM_PARAMS,
        train_data,
        valid_sets=[train_data, valid_data],
        valid_names=["train", "test"],
        num_boost_round=LGBM_PARAMS["n_estimators"],
        callbacks=callbacks,
    )

    # Evaluate
    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))

    # MAPE only on rows where utilization > 5% (avoids division by near-zero)
    mask = y_test > 5
    if mask.sum() > 0:
        mape = np.mean(np.abs((y_test[mask] - preds[mask]) / y_test[mask])) * 100
    else:
        mape = float("nan")

    print(f"\nTest metrics:")
    print(f"  MAE:  {mae:.2f} pct points")
    print(f"  RMSE: {rmse:.2f} pct points")
    print(f"  MAPE: {mape:.1f}% (on rows with utilization > 5%)")
    print(f"  Test rows used for MAPE: {mask.sum()} of {len(y_test)}")

    return model, X_test, y_test, preds


def save_model(model):
    os.makedirs(MODEL_DIR, exist_ok=True)
    model.save_model(MODEL_PATH)
    print(f"\nModel saved to {MODEL_PATH}")


def generate_shap_plot(model, X_test):
    print("Generating SHAP plot...")
    explainer = shap.TreeExplainer(model)

    # Use a sample if test set is large
    sample = X_test.sample(min(500, len(X_test)), random_state=42)
    shap_values = explainer.shap_values(sample)

    plt.figure(figsize=(10, 8))
    shap.summary_plot(shap_values, sample, show=False, max_display=15)
    plt.tight_layout()
    plt.savefig(SHAP_PATH, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"SHAP plot saved to {SHAP_PATH}")


def main():
    df = load_data()
    train, test = prepare_features(df)
    model, X_test, y_test, preds = train_model(train, test)
    save_model(model)
    generate_shap_plot(model, X_test)
    print("\nDone.")


if __name__ == "__main__":
    main()