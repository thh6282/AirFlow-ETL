from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Định nghĩa hàm xử lý
def my_function():
    print("Hello from Airflow!")

# Thiết lập default_args cho DAG
default_args = {
    'owner': 'airflow',  # Người sở hữu DAG
    'retries': 1,        # Số lần thử lại nếu thất bại
    'retry_delay': timedelta(minutes=5),  # Khoảng thời gian giữa các lần thử lại
    'start_date': datetime(2023, 1, 1)  # Ngày bắt đầu
}

# Tạo DAG
with DAG(
    'my_dag',  # Tên DAG
    default_args=default_args,
    description='A simple Airflow DAG',
    schedule_interval='@daily',  # Chu kỳ chạy (hàng ngày)
    catchup=False  # Không bắt kịp các lần chạy đã bỏ lỡ
) as dag:

    # Tạo task sử dụng PythonOperator
    task1 = PythonOperator(
        task_id='print_hello',  # Tên task
        python_callable=my_function  # Hàm sẽ được thực thi
    )

    # Bạn có thể thêm các task khác ở đây và sắp xếp thứ tự
    # Ví dụ: task1 >> task2 (task2 sẽ chạy sau khi task1 hoàn thành)
    
# Lưu file này vào thư mục `dags`

