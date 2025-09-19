# dags/process_employees_mysql.py

import datetime
import pendulum
import os
import requests
import csv

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id="process_employees_mysql",
    schedule=None,  # 设置为None，方便手动触发
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mysql_demo"],
)
def ProcessEmployeesMySQL():

    # 这个ID必须与您在Airflow UI中创建的Connection ID完全一致
    MYSQL_CONN_ID = "mysql_default"

    create_employees_table = SQLExecuteQueryOperator(
        task_id="create_employees_table",
        conn_id=MYSQL_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                serial_number INT PRIMARY KEY,
                company_name TEXT,
                employee_markme TEXT,
                description TEXT,
                `leave` INT
            );
        """,
    )

    create_employees_staging_table = SQLExecuteQueryOperator(
        task_id="create_employees_staging_table",
        conn_id=MYSQL_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS employees_staging (
                serial_number INT PRIMARY KEY,
                company_name TEXT,
                employee_markme TEXT,
                description TEXT,
                `leave` INT
            );
            TRUNCATE TABLE employees_staging;
        """,
    )

    @task
    def get_and_load_data():
        """下载CSV数据并使用MySqlHook将其加载到临时表中"""
        data_path = "/opt/airflow/files/employees.csv"
        # --- START: 添加这一行 ---
        # 在写入文件前，确保其所在的目录一定存在
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        # --- END: 添加这一行 ---

        url = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv"

        response = requests.get(url)
        with open(data_path, "w") as file:
            file.write(response.text)

        # ... 函数的其余部分保持不变 ...
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

        with open(data_path, "r") as file:
            reader = csv.reader(file)
            next(reader)  # 跳过CSV文件的表头
            rows = [tuple(row) for row in reader]

        if rows:
            target_table = "employees_staging"
            target_fields = ["serial_number", "company_name", "employee_markme", "description", "`leave`"]
            mysql_hook.insert_rows(table=target_table, rows=rows, target_fields=target_fields)

    @task
    def merge_data():
        sql_query = """
            INSERT INTO employees (serial_number, company_name, employee_markme, description, `leave`)
            SELECT serial_number, company_name, employee_markme, description, `leave`
            FROM employees_staging  -- <-- 修改这里
            ON DUPLICATE KEY UPDATE
                company_name = VALUES(company_name),
                employee_markme = VALUES(employee_markme),
                description = VALUES(description),
                `leave` = VALUES(`leave`);
        """
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        mysql_hook.run(sql_query)

    # 更新任务依赖关系
    [create_employees_table, create_employees_staging_table] >> get_and_load_data() >> merge_data()

process_employees_dag = ProcessEmployeesMySQL()