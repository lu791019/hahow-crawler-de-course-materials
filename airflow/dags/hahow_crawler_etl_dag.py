"""
Hahow 爬蟲 DAG
爬取 Hahow 平台的課程和文章數據，並上傳至 MySQL 資料庫
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# 導入爬蟲任務
from data_ingestion.hahow_crawler_course_optimized_sales import crawler_hahow_course
from data_ingestion.hahow_crawler_article_optimized import crawler_hahow_article

# 導入 View 和 Table 相關函數
from data_ingestion.mysql import create_view, create_table_from_view

# 定義要爬取的分類
CATEGORIES = [
    "programming", "marketing", "language", "design", 
    "lifestyle", "music", "art", "photography", 'humanities',
    "finance-and-investment", "career-skills", "cooking",
]

# 預設參數
default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,  # 失敗時重試 2 次
    'retry_delay': timedelta(minutes=1),  # 重試間隔 5 分鐘
    'execution_timeout': timedelta(hours=1),  # 執行超時時間 1 小時
}


def create_course_sales_daily_view():
    """
    建立課程銷售日統計 View
    """
    view_sql = """
    CREATE OR REPLACE VIEW vw_course_sales_daily AS
    SELECT
      t.course_id,
      DATE(t.captured_at) AS captured_date,
      t.price,
      t.sold_num,
      t.price * t.sold_num AS revenue
    FROM (
      SELECT
        s.*,
        ROW_NUMBER() OVER (
          PARTITION BY s.course_id, DATE(s.captured_at)
          ORDER BY s.sold_num DESC, s.captured_at DESC, s.id DESC
        ) AS rn
      FROM hahow_course_sales s
      WHERE
        s.price < 999999
    ) AS t
    WHERE t.rn = 1;
    """
    
    create_view(view_name="vw_course_sales_daily", view_sql=view_sql)
    print("✅ 課程銷售日統計 View 建立完成")


def replace_course_sales_daily_table():
    """
    從 vw_course_sales_daily View 建立實體 Table
    """
    create_table_from_view(view_name="vw_course_sales_daily", table_name="hahow_course_sales_daily")
    print("✅ 課程銷售日統計 Table 建立完成")



# 建立 DAG
with DAG(
    dag_id='hahow_crawler_etl_dag',
    default_args=default_args,
    description='Hahow 平台數據爬取 DAG - 爬取課程和文章數據',
    schedule_interval='0 11,23 * * *',  # 每天 11 點和 23 點執行
    catchup=False,  # 不執行歷史任務
    max_active_runs=1,  # 同時只允許一個 DAG 實例運行
    tags=['hahow', 'crawler', 'etl'],
) as dag:

    # 開始任務
    start_task = BashOperator(
        task_id='start_crawler',
        bash_command='echo "開始執行 Hahow 爬蟲任務..."',
    )

    # 課程分流 dummy task
    course_branch = DummyOperator(
        task_id='course_branch',
    )

    # 文章分流 dummy task  
    article_branch = DummyOperator(
        task_id='article_branch',
    )

    # 課程爬取任務 - 為每個分類創建單獨的任務
    course_tasks = []
    for category in CATEGORIES:
        task = PythonOperator(
            task_id=f'crawl_course_{category}',
            python_callable=crawler_hahow_course,
            op_args=[category],
        )
        course_tasks.append(task)

    # 文章爬取任務 - 為每個分類創建單獨的任務
    article_tasks = []
    for category in CATEGORIES:
        task = PythonOperator(
            task_id=f'crawl_article_{category}',
            python_callable=crawler_hahow_article,
            op_args=[category],
        )
        article_tasks.append(task)

    # 建立 View 任務
    create_view_task = PythonOperator(
        task_id='create_course_sales_daily_view',
        python_callable=create_course_sales_daily_view,
    )

    # 建立 Table 任務 (依賴於 View 任務)
    create_table_task = PythonOperator(
        task_id='replace_course_sales_daily_table',
        python_callable=replace_course_sales_daily_table,
    )

    # ETL 任務
    etl_task = DummyOperator(
        task_id='etl_task',
    )
    
    # 結束任務
    end_task = BashOperator(
        task_id='end_crawler',
        bash_command='echo "Hahow 爬蟲任務執行完成！"',
        trigger_rule='all_success',  # 只有當所有前置任務成功時才執行
    )

    # 設定任務依賴關係
    # 開始 -> 兩個分流 -> 各自的爬取任務 -> 建立 View -> 建立 Table -> 結束
    start_task >> course_branch >> course_tasks >> etl_task
    start_task >> article_branch >> article_tasks >> etl_task
    etl_task >> create_view_task >> create_table_task >> end_task