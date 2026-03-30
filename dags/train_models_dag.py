# /opt/airflow/dags/train_two_models_s3.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import joblib
import io
import logging

logger = logging.getLogger("airflow")

S3_BUCKET = "ou-datalake-ecommerce-2251050026"

MONTHLY_STATS_INPUT_KEY = "data/gold/statistic_data/monthly_stats/part-00000-ca7dce32-68ab-4223-9e41-c3ddd74fcf9a-c000.snappy.parquet"
MONTHLY_STATS_MODEL_KEY = "athena_staging/monthly_stats_model.pkl"

SALES_REPORT_INPUT_KEY = "data/gold/sales_report/part-00000-2921b744-205a-4b91-97c6-f3655d3d20d8-c000.snappy.parquet"
SALES_REPORT_MODEL_KEY = "athena_staging/sales_report_model.pkl"


def load_parquet_from_s3(key):
    """Load parquet file từ S3."""
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key=key, bucket_name=S3_BUCKET)
    df = pd.read_parquet(io.BytesIO(obj.get()["Body"].read()))
    return df


def train_monthly_stats_model():
    """Train model cho monthly_stats: dự đoán total_revenue."""
    df = load_parquet_from_s3(MONTHLY_STATS_INPUT_KEY).dropna()

    target_col = "total_revenue"
    # bỏ partition_0 nếu có
    features = [c for c in df.columns if c not in [target_col, "partition_0"]]
    X = df[features]
    y = df[target_col]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    buffer = io.BytesIO()
    joblib.dump(model, buffer)
    buffer.seek(0)

    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file_obj(buffer, key=MONTHLY_STATS_MODEL_KEY, bucket_name=S3_BUCKET, replace=True)
    logger.info("Đã train xong monthly stats model và upload lên S3.")


def train_sales_report_model():
    """
    Train model cho sales_report:
    dataset có các cột unit_price, quantity, total_price, rating…
    → ta dự đoán rating dựa trên unit_price, quantity, total_price.
    """
    df = load_parquet_from_s3(SALES_REPORT_INPUT_KEY).dropna()

    target_col = "rating"
    features = ["unit_price", "quantity", "total_price"]
    X = df[features]
    y = df[target_col]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    buffer = io.BytesIO()
    joblib.dump(model, buffer)
    buffer.seek(0)

    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file_obj(buffer, key=SALES_REPORT_MODEL_KEY, bucket_name=S3_BUCKET, replace=True)
    logger.info("Đã train xong sales report model và upload lên S3.")


def predict_monthly_stats(**context):
    """Dự đoán với monthly_stats model từ params."""
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key=MONTHLY_STATS_MODEL_KEY, bucket_name=S3_BUCKET)
    model = joblib.load(io.BytesIO(obj.get()["Body"].read()))

    # Lấy params từ Airflow
    params = context.get('params', {})
    logger.info(f"Params monthly_stats nhận được: {params}")

    # Nếu không truyền thì dùng default
    if not params:
        params = {
            "year": 2025,
            "month": 9,
            "total_orders": 500,
            "total_items": 1500
        }

    new_data = pd.DataFrame([params])
    pred = model.predict(new_data)
    logger.info(f"Monthly Stats prediction: {pred[0]}")
    return float(pred[0])  # XCom

def predict_sales_report(**context):
    """Dự đoán với sales_report model từ params."""
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key=SALES_REPORT_MODEL_KEY, bucket_name=S3_BUCKET)
    model = joblib.load(io.BytesIO(obj.get()["Body"].read()))

    # Lấy params từ Airflow
    params = context.get('params', {})
    logger.info(f"Params sales_report nhận được: {params}")

    if not params:
        params = {
            "unit_price": 25.5,
            "quantity": 3,
            "total_price": 76.5
        }

    new_data = pd.DataFrame([params])
    pred = model.predict(new_data)
    logger.info(f"Sales Report prediction: {pred[0]}")
    return float(pred[0])

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "train_two_models_s3",
    default_args=default_args,
    description="Train hai mô hình từ S3 và predict",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 9),
    catchup=False,

) as dag:

    train_monthly_task = PythonOperator(
        task_id="train_monthly_stats_model",
        python_callable=train_monthly_stats_model,
    )

    train_sales_task = PythonOperator(
        task_id="train_sales_report_model",
        python_callable=train_sales_report_model,
    )

    predict_monthly_task = PythonOperator(
        task_id="predict_monthly_stats",
        python_callable=predict_monthly_stats,
        provide_context=True
    )

    predict_sales_task = PythonOperator(
        task_id="predict_sales_report",
        python_callable=predict_sales_report,
        provide_context=True
    )

    # Chạy train xong rồi predict
    train_monthly_task >> predict_monthly_task
    train_sales_task >> predict_sales_task
