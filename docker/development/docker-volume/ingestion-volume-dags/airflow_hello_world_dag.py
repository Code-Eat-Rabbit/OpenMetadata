import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 定义默认参数，这是Airflow DAG的标准做法
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello_message():
    """一个简单的Python函数，用于在日志中打印信息。"""
    print("Hello from the PythonOperator!")
    print(f"This task ran at {pendulum.now().to_iso8601_string()}")

# 使用 "with" 语句定义DAG，这是现代Airflow推荐的风格
with DAG(
    dag_id='simple_hello_world_dag',
    default_args=default_args,
    description='一个不依赖外部包的简单示例DAG',
    schedule=timedelta(seconds=1),  # 设置DAG每天运行一次
    start_date=pendulum.datetime(2025, 9, 15, tz="UTC"),
    catchup=False, # catchup=False确保DAG不会回填从start_date到现在的历史任务
    tags=['example', 'simple'],
) as dag:

    # 任务1: 使用BashOperator执行一个简单的shell命令
    # 'ds' 是一个Airflow宏，代表执行日期 (execution_date)，格式为 YYYY-MM-DD
    task_print_date = BashOperator(
        task_id='print_current_date',
        bash_command='echo "Today is {{ ds }}"',
    )

    # 任务2: 使用PythonOperator调用一个Python函数
    task_print_hello = PythonOperator(
        task_id='print_hello_message',
        python_callable=print_hello_message,
    )

    # 定义任务的执行顺序：task_print_date 执行成功后，再执行 task_print_hello
    task_print_date >> task_print_hello