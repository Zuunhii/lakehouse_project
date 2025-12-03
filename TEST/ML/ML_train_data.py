import trino
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    confusion_matrix
)
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier


# =========================
# 1) HÀM KẾT NỐI & LOAD DATA
# =========================

def load_data_from_trino(table_name: str) -> pd.DataFrame:
    """
    Đọc dữ liệu từ bảng ML trong Trino:
    - table_name: vd 'ml_dim_customer_raw_vs_dw' hoặc 'ml_dim_customer_clean_vs_dw'
    Trả về: pandas DataFrame
    """
    conn = trino.dbapi.connect(
        host='localhost',
        port=8443,
        user='data_engineer',
        catalog='iceberg',
        schema='silver',
        http_scheme='https',
        auth=trino.auth.BasicAuthentication("data_engineer", "engineer123"),
        verify=False,  # nếu mày không dùng cert thật
    )

    cursor = conn.cursor()

    query = f"""
            SELECT
                has_full_name_concat,
                num_nulls,
                has_double_space,
                name_length,
                is_all_caps_name,
                num_special_chars,
                has_address_line2,
                label
            FROM {table_name}
        """

    cursor.execute(query)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]

    df = pd.DataFrame(rows, columns=cols)
    return df


# =========================
# 2) HÀM CHẠY 1 THÍ NGHIỆM ML
# =========================

def run_experiment(table_name: str, display_name: str):
    print("=" * 80)
    print(f"🔥 RUN EXPERIMENT: {display_name}  (table = {table_name})")
    print("=" * 80)

    # 1. Load data
    df = load_data_from_trino(table_name)
    print("Shape dữ liệu:", df.shape)
    print(df.head())

    # 2. Tách X, y
    # label: 0 = DW, 1 = RAW/CLEAN
    y = df["label"].astype(int)
    X = df.drop(columns=["label"])

    # Fill NaN nếu có
    X = X.fillna(0)

    # 3. train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )

    print("\nSố bản ghi train:", X_train.shape[0])
    print("Số bản ghi test :", X_test.shape[0])

    # =========================
    # 3) MODEL 1 – LOGISTIC REGRESSION
    # =========================
    log_clf = LogisticRegression(max_iter=1000)
    log_clf.fit(X_train, y_train)

    y_pred_log = log_clf.predict(X_test)
    y_prob_log = log_clf.predict_proba(X_test)[:, 1]

    print("\n===== Logistic Regression =====")
    print(classification_report(y_test, y_pred_log))
    try:
        print("ROC-AUC:", roc_auc_score(y_test, y_prob_log))
    except ValueError:
        print("ROC-AUC: không tính được (có thể do chỉ có 1 class trong y_test)")

    print("Confusion matrix:\n", confusion_matrix(y_test, y_pred_log))

    # =========================
    # 4) MODEL 2 – RANDOM FOREST
    # =========================
    rf_clf = RandomForestClassifier(
        n_estimators=300,
        random_state=42,
        n_jobs=-1
    )
    rf_clf.fit(X_train, y_train)

    y_pred_rf = rf_clf.predict(X_test)
    y_prob_rf = rf_clf.predict_proba(X_test)[:, 1]

    print("\n===== Random Forest =====")
    print(classification_report(y_test, y_pred_rf))
    try:
        print("ROC-AUC:", roc_auc_score(y_test, y_prob_rf))
    except ValueError:
        print("ROC-AUC: không tính được (có thể do chỉ có 1 class trong y_test)")

    print("Confusion matrix:\n", confusion_matrix(y_test, y_pred_rf))

    print("\n✅ DONE:", display_name)
    print()


# =========================
# 5) MAIN: CHẠY HAI CASE
# =========================

if __name__ == "__main__":
    # Case 1: RAW vs DW
    run_experiment(
        table_name="ml_dim_customer_raw_vs_dw",
        display_name="RAW vs DW"
    )

    # Case 2: CLEAN vs DW
    run_experiment(
        table_name="ml_dim_customer_clean_vs_dw",
        display_name="CLEAN vs DW"
    )
