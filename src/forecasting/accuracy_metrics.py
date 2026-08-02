import numpy as np
import pandas as pd

MODELS = {
    "MA_4w":   "ma_4w",
    "MA_8w":   "ma_8w",
    "MA_12w":  "ma_12w",
    "Prophet": "prophet_forecast",
}


def compute_kpis(actual: pd.Series, forecast: pd.Series) -> dict:
    """Compute RMSE, MAE, WMAPE, Bias — NaN pairs are excluded."""
    mask = forecast.notna() & actual.notna()
    a, f = actual[mask].astype(float), forecast[mask].astype(float)

    if len(a) == 0:
        return {"rmse": None, "mae": None, "wmape": None, "bias": None, "n_weeks": 0}

    errors = f - a
    rmse   = float(np.sqrt((errors ** 2).mean()))
    mae    = float(errors.abs().mean())
    wmape  = float(errors.abs().sum() / a.sum() * 100) if a.sum() > 0 else None
    bias   = float(errors.mean())

    return {
        "rmse":    round(rmse, 4),
        "mae":     round(mae, 4),
        "wmape":   round(wmape, 4) if wmape is not None else None,
        "bias":    round(bias, 4),
        "n_weeks": int(mask.sum()),
    }


def build_accuracy_summary(test_df: pd.DataFrame) -> pd.DataFrame:
    """
    test_df: pandas DF with columns market, category_name, actual_quantity,
             ma_4w, ma_8w, ma_12w, prophet_forecast (test split only).
    Returns one row per (market, category_name, model).
    """
    rows = []
    for (market, category), grp in test_df.groupby(["market", "category_name"]):
        actual = grp["actual_quantity"]
        for model_name, col_name in MODELS.items():
            if col_name not in grp.columns:
                continue
            kpis = compute_kpis(actual, grp[col_name])
            rows.append({
                "market":        market,
                "category_name": category,
                "model":         model_name,
                **kpis,
            })

    return pd.DataFrame(rows)
