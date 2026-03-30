from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import joblib
import io

S3_BUCKET = "ou-datalake-ecommerce-2251050026"
S3_INPUT_KEY = "data/gold/feature_engineered_data/delivered_time_data/delivery_features/part-00000-f67e433d-f172-48ab-8cab-52f082888b9c-c000.snappy.parquet"
S3_OUTPUT_KEY = "athena_staging/delivery_time_model.pkl"

def load_data_from_s3():
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key=S3_INPUT_KEY, bucket_name=S3_BUCKET)
    df = pd.read_parquet(io.BytesIO(obj.get()["Body"].read()))
    return df

def train_and_save_model():
    df = load_data_from_s3()
    df = df.dropna(subset=["delivery_duration_days"])

    X = df[["customer_id", "product_id", "is_late_delivery"]]
    y = df["delivery_duration_days"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # save model vào buffer rồi upload lên S3
    buffer = io.BytesIO()
    joblib.dump(model, buffer)
    buffer.seek(0)

    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file_obj(buffer, key=S3_OUTPUT_KEY, bucket_name=S3_BUCKET, replace=True)
    print(f"Model saved to s3://{S3_BUCKET}/{S3_OUTPUT_KEY}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def predict_new_order():
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key=S3_OUTPUT_KEY, bucket_name=S3_BUCKET)
    model = joblib.load(io.BytesIO(obj.get()["Body"].read()))

    # Dữ liệu mới cần dự đoán
    new_order = pd.DataFrame([{
        "customer_id": 6696,
        "product_id": 1282,
        "is_late_delivery": 0
    }])

    prediction = model.predict(new_order)
    print("Dự đoán thời gian giao hàng (ngày):", prediction[0])

with DAG(
    "delivery_time_prediction_dag",
    default_args=default_args,
    description="DAG train và dự đoán thời gian giao hàng",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 9),
    catchup=False,
) as dag:

    train_task = PythonOperator(
        task_id="train_and_save_model",
        python_callable=train_and_save_model,
    )

    predict_task = PythonOperator(
        task_id="predict_new_order",
        python_callable=predict_new_order,
    )

    train_task >> predict_task
